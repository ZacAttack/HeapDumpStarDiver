use std::collections::HashMap;
use std::sync::Arc;
use arrow_array::{Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, StructArray, UInt16Array, UInt32Array, UInt64Array};
use arrow_array::builder::{ListBuilder, BooleanBuilder, Int8Builder, UInt16Builder, Int16Builder, Int32Builder, Int64Builder, Float32Builder, Float64Builder, UInt64Builder};
use arrow_schema::{DataType, Field, Schema};
use dashmap::DashMap;
use jvm_hprof::{Hprof, Id, Record};
use jvm_hprof::heap_dump::{FieldType, FieldValue, PrimitiveArrayType, SubRecord};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use crate::hprof_index::HprofIndex;
use crate::util::generate_schema_from_descriptors;

// ---------------------------------------------------------------------------
// ExtendedFieldValue & helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum ExtendedFieldValue {
    FieldValue(FieldValue),
    /// Any object reference (instance, array, class, or null). Type resolved at build time.
    Reference(Id),
}

// Ensure ExtendedFieldValue can be sent across threads for parallel column building
unsafe impl Send for ExtendedFieldValue {}
unsafe impl Sync for ExtendedFieldValue {}

/// Appends one instance's field values to the positional column vecs.
/// Reference classification (instance vs prim array) is deferred to build_column
/// to avoid redundant DashMap lookups during the hot parse loop.
fn add_instance_values(
    hprof: &Hprof,
    field_columns: &mut Vec<Vec<ExtendedFieldValue>>,
    mut field_val_input: &[u8],
    field_descriptors: &[jvm_hprof::heap_dump::FieldDescriptor],
) {
    for (i, fd) in field_descriptors.iter().enumerate() {
        let (input, field_val) = fd
            .field_type()
            .parse_value(field_val_input, hprof.header().id_size())
            .unwrap();
        field_val_input = input;
        match field_val {
            FieldValue::ObjectId(Some(field_ref_id)) => {
                field_columns[i].push(ExtendedFieldValue::Reference(field_ref_id));
            }
            FieldValue::ObjectId(None) => {
                field_columns[i].push(ExtendedFieldValue::Reference(Id::from(0)));
            }
            _ => {
                field_columns[i].push(ExtendedFieldValue::FieldValue(field_val));
            }
        }
    }
}

/// Build an Arrow column from buffered field values, using the schema's declared
/// DataType to determine the output type.
fn build_column(field_val_vec: &[ExtendedFieldValue], index: &HprofIndex, expected_type: &DataType) -> Arc<dyn Array> {
    match expected_type {
        DataType::Struct(_) => {
            let id_vec = field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::Reference(val) => val.id(),
                _ => 0,
            }).collect::<Vec<u64>>();
            let type_vec: Vec<std::borrow::Cow<str>> = field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::Reference(val) => {
                    resolve_ref_type_str(*val, index)
                },
                _ => std::borrow::Cow::Borrowed("null"),
            }).collect();
            let id_array: Arc<dyn Array> = Arc::new(UInt64Array::from(id_vec));
            let type_strs: Vec<&str> = type_vec.iter().map(|c| c.as_ref()).collect();
            let type_array: Arc<dyn Array> = Arc::new(StringArray::from(type_strs));
            let struct_array = StructArray::from(vec![
                (Arc::new(Field::new("id", DataType::UInt64, false)), id_array),
                (Arc::new(Field::new("type", DataType::Utf8, false)), type_array),
            ]);
            Arc::new(struct_array)
        }
        DataType::Int32 => {
            Arc::new(Int32Array::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::FieldValue(FieldValue::Int(val)) => *val,
                _ => 0,
            }).collect::<Vec<i32>>()))
        }
        DataType::Int64 => {
            Arc::new(Int64Array::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::FieldValue(FieldValue::Long(val)) => *val,
                _ => 0,
            }).collect::<Vec<i64>>()))
        }
        DataType::Boolean => {
            Arc::new(BooleanArray::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::FieldValue(FieldValue::Boolean(val)) => *val,
                _ => false,
            }).collect::<Vec<bool>>()))
        }
        DataType::UInt16 => {
            Arc::new(UInt16Array::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::FieldValue(FieldValue::Char(val)) => *val as u16,
                _ => 0,
            }).collect::<Vec<u16>>()))
        }
        DataType::Float32 => {
            Arc::new(Float32Array::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::FieldValue(FieldValue::Float(val)) => *val,
                _ => 0.0,
            }).collect::<Vec<f32>>()))
        }
        DataType::Float64 => {
            Arc::new(Float64Array::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::FieldValue(FieldValue::Double(val)) => *val,
                _ => 0.0,
            }).collect::<Vec<f64>>()))
        }
        DataType::Int8 => {
            Arc::new(Int8Array::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::FieldValue(FieldValue::Byte(val)) => *val,
                _ => 0,
            }).collect::<Vec<i8>>()))
        }
        DataType::Int16 => {
            Arc::new(Int16Array::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::FieldValue(FieldValue::Short(val)) => *val,
                _ => 0,
            }).collect::<Vec<i16>>()))
        }
        DataType::UInt64 => {
            Arc::new(UInt64Array::from(field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::Reference(val) => val.id(),
                ExtendedFieldValue::FieldValue(FieldValue::ObjectId(val)) => val.map(|v| v.id()).unwrap_or(0),
                _ => 0,
            }).collect::<Vec<u64>>()))
        }
        _ => panic!("Unsupported schema data type: {:?}", expected_type),
    }
}

/// Resolve the type name for an object reference.
/// Returns a &str where possible to avoid allocation for the common cases
/// (instance/object array references), falling back to String for rare cases
/// (primitive array refs, class refs, unresolved).
fn resolve_ref_type_str<'a>(id: Id, index: &'a HprofIndex) -> std::borrow::Cow<'a, str> {
    use std::borrow::Cow;
    if id.id() == 0 {
        return Cow::Borrowed("null");
    }
    // Most common: instance or object array → class name is a &str from the index
    if let Some(class_obj_id) = index.obj_id_to_class_obj_id.get(&id) {
        if let Some(c) = index.classes.get(&*class_obj_id) {
            return Cow::Borrowed(c.name);
        }
    }
    // Primitive array ref (rarer — only when a field points to a prim array)
    if let Some(pt) = index.prim_array_obj_id_to_type.get(&id) {
        return Cow::Owned(format!("{}[]", pt.java_type_name()));
    }
    // Class reference
    if let Some(c) = index.classes.get(&id) {
        return Cow::Owned(format!("class {}", c.name));
    }
    Cow::Borrowed("(unresolved)")
}

fn resolve_ref_type(id: Id, index: &HprofIndex) -> String {
    resolve_ref_type_str(id, index).into_owned()
}

fn format_field_value(fv: &FieldValue) -> (String, String, u64, String) {
    match fv {
        FieldValue::ObjectId(Some(id)) => ("object".into(), String::new(), id.id(), String::new()),
        FieldValue::ObjectId(None) => ("object".into(), "null".into(), 0, "null".into()),
        FieldValue::Boolean(v) => ("boolean".into(), v.to_string(), 0, String::new()),
        FieldValue::Char(v) => ("char".into(), v.to_string(), 0, String::new()),
        FieldValue::Float(v) => ("float".into(), v.to_string(), 0, String::new()),
        FieldValue::Double(v) => ("double".into(), v.to_string(), 0, String::new()),
        FieldValue::Byte(v) => ("byte".into(), v.to_string(), 0, String::new()),
        FieldValue::Short(v) => ("short".into(), v.to_string(), 0, String::new()),
        FieldValue::Int(v) => ("int".into(), v.to_string(), 0, String::new()),
        FieldValue::Long(v) => ("long".into(), v.to_string(), 0, String::new()),
    }
}

// ---------------------------------------------------------------------------
// Fully parallel segment processing: parse + build RecordBatches in rayon
// ---------------------------------------------------------------------------

/// A ready-to-write batch: file key, schema, and the RecordBatch.
struct WritableBatch {
    file_key: String,
    schema: Arc<Schema>,
    batch: RecordBatch,
}

// WritableBatch contains RecordBatch (which is Send+Sync) and Strings/Arc — all Send.
unsafe impl Send for WritableBatch {}

/// Process a single segment: parse sub-records, build Arrow arrays, and return
/// ready-to-write RecordBatches. ALL CPU work happens here inside rayon.
fn process_segment_to_batches<'a>(
    record: &Record<'a>,
    hprof: &Hprof,
    index: &HprofIndex,
    schemas: &HashMap<Id, Schema>,
    robo_mode: bool,
) -> Vec<WritableBatch> {
    let mut batches = Vec::new();

    // Temporary per-class accumulators for this segment
    let mut instances: HashMap<Id, (Vec<u64>, Vec<Vec<ExtendedFieldValue>>)> = HashMap::new();

    // Primitive array accumulators
    let mut bool_arrays: Vec<(u64, Vec<bool>)> = Vec::new();
    let mut byte_arrays: Vec<(u64, Vec<i8>)> = Vec::new();
    let mut char_arrays: Vec<(u64, Vec<u16>)> = Vec::new();
    let mut short_arrays: Vec<(u64, Vec<i16>)> = Vec::new();
    let mut int_arrays: Vec<(u64, Vec<i32>)> = Vec::new();
    let mut long_arrays: Vec<(u64, Vec<i64>)> = Vec::new();
    let mut float_arrays: Vec<(u64, Vec<f32>)> = Vec::new();
    let mut double_arrays: Vec<(u64, Vec<f64>)> = Vec::new();

    // Object array accumulators
    let mut oa_obj_ids: Vec<u64> = Vec::new();
    let mut oa_class_names: Vec<String> = Vec::new();
    let mut oa_elements: Vec<Vec<u64>> = Vec::new();

    // GC root accumulators
    let mut gc_roots: Vec<(String, u64, Option<u32>, Option<u32>)> = Vec::new();

    // Object index accumulators (robo mode only)
    let mut idx_obj_ids: Vec<u64> = Vec::new();
    let mut idx_type_names: Vec<String> = Vec::new();

    // --- Parse sub-records ---
    let segment = record.as_heap_dump_segment().unwrap().unwrap();
    for p in segment.sub_records() {
        let s = p.unwrap();
        match s {
            SubRecord::Instance(instance) => {
                if robo_mode {
                    idx_obj_ids.push(instance.obj_id().id());
                    idx_type_names.push(
                        index.classes.get(&instance.class_obj_id())
                            .map(|c| c.name.to_string())
                            .unwrap_or_else(|| "(unresolved)".to_string())
                    );
                }

                let field_descriptors = match index.class_instance_field_descriptors
                    .get(&instance.class_obj_id())
                {
                    Some(fd) => fd,
                    None => continue,
                };
                if !schemas.contains_key(&instance.class_obj_id()) {
                    continue;
                }

                let entry = instances
                    .entry(instance.class_obj_id())
                    .or_insert_with(|| {
                        let columns: Vec<Vec<ExtendedFieldValue>> =
                            (0..field_descriptors.len()).map(|_| Vec::new()).collect();
                        (Vec::new(), columns)
                    });

                entry.0.push(instance.obj_id().id());
                add_instance_values(
                    hprof, &mut entry.1, instance.fields(), field_descriptors,
                );
            }
            SubRecord::PrimitiveArray(pa) => {
                let obj_id = pa.obj_id().id();
                if robo_mode {
                    idx_obj_ids.push(obj_id);
                    idx_type_names.push(format!("{}[]", pa.primitive_type().java_type_name()));
                }
                match pa.primitive_type() {
                    PrimitiveArrayType::Boolean => {
                        bool_arrays.push((obj_id, pa.booleans().unwrap().map(|v| v.unwrap()).collect()));
                    }
                    PrimitiveArrayType::Byte => {
                        byte_arrays.push((obj_id, pa.bytes().unwrap().map(|v| v.unwrap()).collect()));
                    }
                    PrimitiveArrayType::Char => {
                        char_arrays.push((obj_id, pa.chars().unwrap().map(|v| v.unwrap() as u16).collect()));
                    }
                    PrimitiveArrayType::Short => {
                        short_arrays.push((obj_id, pa.shorts().unwrap().map(|v| v.unwrap()).collect()));
                    }
                    PrimitiveArrayType::Int => {
                        int_arrays.push((obj_id, pa.ints().unwrap().map(|v| v.unwrap()).collect()));
                    }
                    PrimitiveArrayType::Long => {
                        long_arrays.push((obj_id, pa.longs().unwrap().map(|v| v.unwrap()).collect()));
                    }
                    PrimitiveArrayType::Float => {
                        float_arrays.push((obj_id, pa.floats().unwrap().map(|v| v.unwrap()).collect()));
                    }
                    PrimitiveArrayType::Double => {
                        double_arrays.push((obj_id, pa.doubles().unwrap().map(|v| v.unwrap()).collect()));
                    }
                }
            }
            SubRecord::ObjectArray(oa) => {
                if robo_mode {
                    idx_obj_ids.push(oa.obj_id().id());
                    idx_type_names.push(
                        index.classes.get(&oa.array_class_obj_id())
                            .map(|c| format!("{}[]", c.name))
                            .unwrap_or_else(|| "(unresolved)[]".to_string())
                    );
                }
                oa_obj_ids.push(oa.obj_id().id());
                oa_class_names.push(
                    index.classes.get(&oa.array_class_obj_id())
                        .map(|c| c.name.to_string())
                        .unwrap_or_else(|| "(unresolved)".to_string())
                );
                oa_elements.push(
                    oa.elements(hprof.header().id_size())
                        .map(|elem| match elem.unwrap() {
                            Some(id) => id.id(),
                            None => 0,
                        })
                        .collect()
                );
            }
            SubRecord::GcRootUnknown(r) => {
                gc_roots.push(("Unknown".into(), r.obj_id().id(), None, None));
            }
            SubRecord::GcRootThreadObj(r) => {
                gc_roots.push(("ThreadObj".into(), r.thread_obj_id().map(|id| id.id()).unwrap_or(0), Some(r.thread_serial().num()), None));
            }
            SubRecord::GcRootJniGlobal(r) => {
                gc_roots.push(("JniGlobal".into(), r.obj_id().id(), None, None));
            }
            SubRecord::GcRootJniLocalRef(r) => {
                gc_roots.push(("JniLocal".into(), r.obj_id().id(), Some(r.thread_serial().num()), r.frame_index()));
            }
            SubRecord::GcRootJavaStackFrame(r) => {
                gc_roots.push(("JavaStackFrame".into(), r.obj_id().id(), Some(r.thread_serial().num()), r.frame_index()));
            }
            SubRecord::GcRootNativeStack(r) => {
                gc_roots.push(("NativeStack".into(), r.obj_id().id(), Some(r.thread_serial().num()), None));
            }
            SubRecord::GcRootSystemClass(r) => {
                gc_roots.push(("SystemClass".into(), r.obj_id().id(), None, None));
            }
            SubRecord::GcRootThreadBlock(r) => {
                gc_roots.push(("ThreadBlock".into(), r.obj_id().id(), Some(r.thread_serial().num()), None));
            }
            SubRecord::GcRootBusyMonitor(r) => {
                gc_roots.push(("BusyMonitor".into(), r.obj_id().id(), None, None));
            }
            SubRecord::Class(c) if robo_mode => {
                idx_obj_ids.push(c.obj_id().id());
                idx_type_names.push(
                    index.classes.get(&c.obj_id())
                        .map(|ec| format!("class {}", ec.name))
                        .unwrap_or_else(|| "class (unresolved)".to_string())
                );
            }
            _ => {}
        }
    }

    // --- Build RecordBatches (all CPU work, still inside rayon task) ---

    // Instance batches per class
    for (class_id, (obj_ids, field_columns)) in instances {
        let schema = match schemas.get(&class_id) {
            Some(s) => s,
            None => continue,
        };

        let mut fields = vec![Field::new("obj_id", DataType::UInt64, false)];
        fields.extend(schema.fields().iter().map(|f| f.as_ref().clone()));
        let full_schema = Arc::new(Schema::new(fields));

        let data_columns: Vec<Arc<dyn Array>> = field_columns.iter()
            .zip(schema.fields().iter())
            .map(|(col, field)| build_column(col, index, field.data_type()))
            .collect();

        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(data_columns.len() + 1);
        columns.push(Arc::new(UInt64Array::from(obj_ids)));
        columns.extend(data_columns);

        let batch = RecordBatch::try_new(full_schema.clone(), columns)
            .unwrap_or_else(|e| {
                let class_name = index.classes.get(&class_id).map(|c| c.name).unwrap_or("unknown");
                panic!("RecordBatch creation failed for class '{}': {}", class_name, e);
            });

        let class_name = index.classes.get(&class_id).unwrap().name;
        let file_key = format!("{}_{}", class_name, class_id);
        batches.push(WritableBatch { file_key, schema: full_schema, batch });
    }

    // Object array batch
    if !oa_obj_ids.is_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("obj_id", DataType::UInt64, false),
            Field::new("class_name", DataType::Utf8, false),
            Field::new("elements", DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))), false),
        ]));
        // ListBuilder created and consumed within this task — never sent across threads
        let mut list_builder = ListBuilder::new(UInt64Builder::new());
        for elems in &oa_elements {
            for e in elems { list_builder.values().append_value(*e); }
            list_builder.append(true);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(oa_obj_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(oa_class_names)) as Arc<dyn Array>,
                Arc::new(list_builder.finish()) as Arc<dyn Array>,
            ],
        ).unwrap();
        batches.push(WritableBatch { file_key: "_object_arrays".into(), schema, batch });
    }

    // Primitive array batches
    macro_rules! build_prim_batch {
        ($arrays:expr, $name:expr, $inner_type:expr, $builder_type:ident) => {
            if !$arrays.is_empty() {
                let mut obj_ids = Vec::with_capacity($arrays.len());
                let mut list_builder = ListBuilder::new($builder_type::new());
                for (oid, vals) in &$arrays {
                    obj_ids.push(*oid);
                    for v in vals { list_builder.values().append_value(*v); }
                    list_builder.append(true);
                }
                let schema = Arc::new(Schema::new(vec![
                    Field::new("obj_id", DataType::UInt64, false),
                    Field::new("values", DataType::List(Arc::new(Field::new("item", $inner_type, true))), false),
                ]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(obj_ids)) as Arc<dyn Array>,
                        Arc::new(list_builder.finish()) as Arc<dyn Array>,
                    ],
                ).unwrap();
                batches.push(WritableBatch { file_key: $name.into(), schema, batch });
            }
        };
    }

    build_prim_batch!(bool_arrays, "_primitive_arrays_boolean", DataType::Boolean, BooleanBuilder);
    build_prim_batch!(byte_arrays, "_primitive_arrays_byte", DataType::Int8, Int8Builder);
    build_prim_batch!(char_arrays, "_primitive_arrays_char", DataType::UInt16, UInt16Builder);
    build_prim_batch!(short_arrays, "_primitive_arrays_short", DataType::Int16, Int16Builder);
    build_prim_batch!(int_arrays, "_primitive_arrays_int", DataType::Int32, Int32Builder);
    build_prim_batch!(long_arrays, "_primitive_arrays_long", DataType::Int64, Int64Builder);
    build_prim_batch!(float_arrays, "_primitive_arrays_float", DataType::Float32, Float32Builder);
    build_prim_batch!(double_arrays, "_primitive_arrays_double", DataType::Float64, Float64Builder);

    // GC root batch
    if !gc_roots.is_empty() {
        let mut types = Vec::with_capacity(gc_roots.len());
        let mut obj_ids = Vec::with_capacity(gc_roots.len());
        let mut thread_serials = Vec::with_capacity(gc_roots.len());
        let mut frame_indexes = Vec::with_capacity(gc_roots.len());
        for (rt, oid, ts, fi) in gc_roots {
            types.push(rt);
            obj_ids.push(oid);
            thread_serials.push(ts);
            frame_indexes.push(fi);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("root_type", DataType::Utf8, false),
            Field::new("obj_id", DataType::UInt64, false),
            Field::new("thread_serial", DataType::UInt32, true),
            Field::new("frame_index", DataType::UInt32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(types)) as Arc<dyn Array>,
                Arc::new(UInt64Array::from(obj_ids)) as Arc<dyn Array>,
                Arc::new(UInt32Array::from(thread_serials)) as Arc<dyn Array>,
                Arc::new(UInt32Array::from(frame_indexes)) as Arc<dyn Array>,
            ],
        ).unwrap();
        batches.push(WritableBatch { file_key: "_gc_roots".into(), schema, batch });
    }

    // Object index batch (robo mode)
    if !idx_obj_ids.is_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("obj_id", DataType::UInt64, false),
            Field::new("type_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(idx_obj_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(idx_type_names)) as Arc<dyn Array>,
            ],
        ).unwrap();
        batches.push(WritableBatch { file_key: "_object_index".into(), schema, batch });
    }

    batches
}

// ---------------------------------------------------------------------------
// Schema generation
// ---------------------------------------------------------------------------

fn generate_all_schemas(index: &HprofIndex, robo_mode: bool) -> HashMap<Id, Schema> {
    index.class_instance_field_descriptors.iter()
        .map(|(class_id, field_descriptors)| {
            let schema = generate_schema_from_descriptors(
                field_descriptors,
                &index.utf8,
                index.class_field_declaring_classes.get(class_id),
                robo_mode,
            );
            (*class_id, schema)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Static fields writer
// ---------------------------------------------------------------------------

fn build_static_fields_batch(index: &HprofIndex, robo_mode: bool) -> Option<WritableBatch> {
    let mut class_obj_ids: Vec<u64> = Vec::new();
    let mut class_names: Vec<String> = Vec::new();
    let mut field_names: Vec<String> = Vec::new();
    let mut field_types: Vec<String> = Vec::new();
    let mut primitive_values: Vec<String> = Vec::new();
    let mut ref_ids: Vec<u64> = Vec::new();
    let mut ref_types: Vec<String> = Vec::new();

    for (_, ez_class) in index.classes.iter() {
        for sf in &ez_class.static_fields {
            let field_name = index.utf8.get(&sf.name_id())
                .unwrap_or(&"(missing utf8)");

            let (ft, pv, rid, rt) = format_field_value(&sf.value());

            class_obj_ids.push(ez_class.obj_id.id());
            class_names.push(ez_class.name.to_string());
            field_names.push(field_name.to_string());
            field_types.push(ft);

            if matches!(sf.field_type(), FieldType::ObjectId) {
                ref_ids.push(rid);
                if !robo_mode {
                    if rt.is_empty() && rid != 0 {
                        ref_types.push(resolve_ref_type(Id::from(rid), index));
                    } else {
                        ref_types.push(rt);
                    }
                }
                primitive_values.push(String::new());
            } else {
                ref_ids.push(0);
                if !robo_mode {
                    ref_types.push(String::new());
                }
                primitive_values.push(pv);
            }
        }
    }

    if class_obj_ids.is_empty() {
        return None;
    }

    if robo_mode {
        let schema = Arc::new(Schema::new(vec![
            Field::new("class_obj_id", DataType::UInt64, false),
            Field::new("class_name", DataType::Utf8, false),
            Field::new("field_name", DataType::Utf8, false),
            Field::new("field_type", DataType::Utf8, false),
            Field::new("primitive_value", DataType::Utf8, false),
            Field::new("ref_id", DataType::UInt64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(class_obj_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(class_names)) as Arc<dyn Array>,
                Arc::new(StringArray::from(field_names)) as Arc<dyn Array>,
                Arc::new(StringArray::from(field_types)) as Arc<dyn Array>,
                Arc::new(StringArray::from(primitive_values)) as Arc<dyn Array>,
                Arc::new(UInt64Array::from(ref_ids)) as Arc<dyn Array>,
            ],
        ).unwrap();

        Some(WritableBatch { file_key: "_static_fields".into(), schema, batch })
    } else {
        let schema = Arc::new(Schema::new(vec![
            Field::new("class_obj_id", DataType::UInt64, false),
            Field::new("class_name", DataType::Utf8, false),
            Field::new("field_name", DataType::Utf8, false),
            Field::new("field_type", DataType::Utf8, false),
            Field::new("primitive_value", DataType::Utf8, false),
            Field::new("ref_id", DataType::UInt64, false),
            Field::new("ref_type", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(class_obj_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(class_names)) as Arc<dyn Array>,
                Arc::new(StringArray::from(field_names)) as Arc<dyn Array>,
                Arc::new(StringArray::from(field_types)) as Arc<dyn Array>,
                Arc::new(StringArray::from(primitive_values)) as Arc<dyn Array>,
                Arc::new(UInt64Array::from(ref_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(ref_types)) as Arc<dyn Array>,
            ],
        ).unwrap();

        Some(WritableBatch { file_key: "_static_fields".into(), schema, batch })
    }
}

// ---------------------------------------------------------------------------
// SharedWriterPool — thread-safe pool with per-writer Mutex locking
// ---------------------------------------------------------------------------
// Outer Mutex protects the HashMap for lazy writer creation.
// Inner Mutex protects individual ArrowWriters so multiple threads can write
// to different files concurrently — they only block when two threads need
// the same file.

// ---------------------------------------------------------------------------
// ShardedWriterPool — lock-free sharded writer pool
// ---------------------------------------------------------------------------
// Each shard is a dedicated thread that owns a set of ArrowWriters exclusively.
// Batches are routed to shards by hashing the file_key. No Mutexes needed —
// each thread is the sole owner of its writers.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

struct ShardedWriterPool {
    senders: Vec<crossbeam_channel::Sender<WritableBatch>>,
    handles: Vec<std::thread::JoinHandle<()>>,
    robo_mode: bool,
}

impl ShardedWriterPool {
    fn new(num_shards: usize, compression: Compression, robo_mode: bool) -> Self {
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        let mut senders = Vec::with_capacity(if robo_mode { 1 } else { num_shards });
        let mut handles = Vec::with_capacity(num_shards);

        if robo_mode {
            // MPMC: one shared channel, all workers pull from the same queue
            let (tx, rx) = crossbeam_channel::unbounded::<WritableBatch>();
            for worker_id in 0..num_shards {
                let rx = rx.clone();
                let props = props.clone();
                let handle = std::thread::spawn(move || {
                    let mut writers: HashMap<String, ArrowWriter<std::fs::File>> = HashMap::new();

                    for wb in rx {
                        let writer = writers.entry(wb.file_key.clone()).or_insert_with(|| {
                            let safe_name = wb.file_key.replace("/", ".");
                            let file = std::fs::File::create(
                                format!("parquet/{}_chunk{}.parquet", safe_name, worker_id)
                            ).unwrap();
                            ArrowWriter::try_new(file, wb.schema.clone(), Some(props.clone())).unwrap()
                        });
                        writer.write(&wb.batch).unwrap();
                    }

                    for (_name, writer) in writers {
                        writer.close().unwrap();
                    }
                });
                handles.push(handle);
            }
            senders.push(tx);
        } else {
            // Per-shard channels with hash routing (existing behavior)
            for _ in 0..num_shards {
                let (tx, rx) = crossbeam_channel::unbounded::<WritableBatch>();
                let props = props.clone();
                let handle = std::thread::spawn(move || {
                    let mut writers: HashMap<String, ArrowWriter<std::fs::File>> = HashMap::new();

                    for wb in rx {
                        let writer = writers.entry(wb.file_key.clone()).or_insert_with(|| {
                            let safe_name = wb.file_key.replace("/", ".");
                            let file = std::fs::File::create(format!("parquet/{}.parquet", safe_name)).unwrap();
                            ArrowWriter::try_new(file, wb.schema.clone(), Some(props.clone())).unwrap()
                        });
                        writer.write(&wb.batch).unwrap();
                    }

                    for (_name, writer) in writers {
                        writer.close().unwrap();
                    }
                });
                senders.push(tx);
                handles.push(handle);
            }
        }

        ShardedWriterPool { senders, handles, robo_mode }
    }

    fn write_batch(&self, wb: WritableBatch) {
        if self.robo_mode {
            // MPMC: send to the single shared channel; any idle worker picks it up
            self.senders[0].send(wb).unwrap();
        } else {
            // Hash-route to a specific shard
            let mut hasher = DefaultHasher::new();
            wb.file_key.hash(&mut hasher);
            let shard = (hasher.finish() as usize) % self.senders.len();
            self.senders[shard].send(wb).unwrap();
        }
    }

    fn close_all(self) {
        // Drop all senders to signal threads to finish
        drop(self.senders);
        // Wait for all shard threads to close their writers
        for handle in self.handles {
            handle.join().unwrap();
        }
    }
}

// ---------------------------------------------------------------------------
// Robo-mode metadata writers
// ---------------------------------------------------------------------------

/// Write `_class_hierarchy.parquet`: class_obj_id, class_name, super_class_obj_id, super_class_name.
fn write_class_hierarchy(index: &HprofIndex) {
    let mut class_obj_ids: Vec<u64> = Vec::new();
    let mut class_names: Vec<String> = Vec::new();
    let mut super_class_obj_ids: Vec<Option<u64>> = Vec::new();
    let mut super_class_names: Vec<Option<String>> = Vec::new();

    for (class_id, ez_class) in index.classes.iter() {
        class_obj_ids.push(class_id.id());
        class_names.push(ez_class.name.to_string());

        match ez_class.super_class_obj_id {
            Some(super_id) => {
                super_class_obj_ids.push(Some(super_id.id()));
                super_class_names.push(
                    index.classes.get(&super_id)
                        .map(|c| c.name.to_string())
                );
            }
            None => {
                super_class_obj_ids.push(None);
                super_class_names.push(None);
            }
        }
    }

    if class_obj_ids.is_empty() {
        return;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("class_obj_id", DataType::UInt64, false),
        Field::new("class_name", DataType::Utf8, false),
        Field::new("super_class_obj_id", DataType::UInt64, true),
        Field::new("super_class_name", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(class_obj_ids)) as Arc<dyn Array>,
            Arc::new(StringArray::from(class_names)) as Arc<dyn Array>,
            Arc::new(UInt64Array::from(super_class_obj_ids)) as Arc<dyn Array>,
            Arc::new(StringArray::from(super_class_names)) as Arc<dyn Array>,
        ],
    ).unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let file = std::fs::File::create("parquet/_class_hierarchy.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Build `_stack_frames` WritableBatch: frame_id, class_name, method_name, method_signature, source_file, line_num.
fn build_stack_frames_batch(index: &HprofIndex) -> Option<WritableBatch> {
    if index.stack_frames.is_empty() {
        return None;
    }

    let len = index.stack_frames.len();
    let mut frame_ids: Vec<u64> = Vec::with_capacity(len);
    let mut class_names: Vec<&str> = Vec::with_capacity(len);
    let mut method_names: Vec<&str> = Vec::with_capacity(len);
    let mut method_sigs: Vec<&str> = Vec::with_capacity(len);
    let mut source_files: Vec<&str> = Vec::with_capacity(len);
    let mut line_nums: Vec<i32> = Vec::with_capacity(len);

    for sf in &index.stack_frames {
        frame_ids.push(sf.frame_id);
        class_names.push(&sf.class_name);
        method_names.push(&sf.method_name);
        method_sigs.push(&sf.method_signature);
        source_files.push(&sf.source_file);
        line_nums.push(sf.line_num);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("frame_id", DataType::UInt64, false),
        Field::new("class_name", DataType::Utf8, false),
        Field::new("method_name", DataType::Utf8, false),
        Field::new("method_signature", DataType::Utf8, false),
        Field::new("source_file", DataType::Utf8, false),
        Field::new("line_num", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(frame_ids)) as Arc<dyn Array>,
            Arc::new(StringArray::from(class_names)) as Arc<dyn Array>,
            Arc::new(StringArray::from(method_names)) as Arc<dyn Array>,
            Arc::new(StringArray::from(method_sigs)) as Arc<dyn Array>,
            Arc::new(StringArray::from(source_files)) as Arc<dyn Array>,
            Arc::new(Int32Array::from(line_nums)) as Arc<dyn Array>,
        ],
    ).unwrap();

    println!("  Built {} stack frames for _stack_frames.parquet", len);
    Some(WritableBatch { file_key: "_stack_frames".into(), schema, batch })
}

/// Build `_stack_traces` WritableBatch: stack_trace_serial, thread_serial, frame_ids (list of u64).
fn build_stack_traces_batch(index: &HprofIndex) -> Option<WritableBatch> {
    if index.stack_traces.is_empty() {
        return None;
    }

    let len = index.stack_traces.len();
    let mut trace_serials: Vec<u32> = Vec::with_capacity(len);
    let mut thread_serials: Vec<u32> = Vec::with_capacity(len);
    let total_frame_refs: usize = index.stack_traces.iter().map(|st| st.frame_ids.len()).sum();
    let mut frame_id_lists = ListBuilder::new(UInt64Builder::with_capacity(total_frame_refs));

    for st in &index.stack_traces {
        trace_serials.push(st.stack_trace_serial);
        thread_serials.push(st.thread_serial);
        let values = frame_id_lists.values();
        for &fid in &st.frame_ids {
            values.append_value(fid);
        }
        frame_id_lists.append(true);
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("stack_trace_serial", DataType::UInt32, false),
        Field::new("thread_serial", DataType::UInt32, false),
        Field::new("frame_ids", DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))), false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(trace_serials)) as Arc<dyn Array>,
            Arc::new(UInt32Array::from(thread_serials)) as Arc<dyn Array>,
            Arc::new(frame_id_lists.finish()) as Arc<dyn Array>,
        ],
    ).unwrap();

    println!("  Built {} stack traces for _stack_traces.parquet", len);
    Some(WritableBatch { file_key: "_stack_traces".into(), schema, batch })
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub fn dump_objects_to_parquet(hprof: &Hprof, _flush_row_threshold: usize, robo_mode: bool) {
    use std::time::Instant;

    // Clean output directory so stale files from previous runs don't persist
    let _ = std::fs::remove_dir_all("parquet");
    std::fs::create_dir_all("parquet").unwrap();

    // -----------------------------------------------------------------------
    // Pass 1: Parallel index build + collect segment handles
    // -----------------------------------------------------------------------
    let t0 = Instant::now();
    let (index, segments) = HprofIndex::build_with_segments(hprof);
    let pass1_dur = t0.elapsed();

    println!("Pass 1 complete in {:.1}s: {} classes, {} obj mappings, {} segments",
        pass1_dur.as_secs_f64(),
        index.classes.len(), index.obj_id_to_class_obj_id.len(), segments.len());

    if robo_mode {
        println!("Robo mode enabled: bare IDs for references, separate type index files");
    }

    // Generate schemas from field descriptors (no file scan needed)
    let schemas = generate_all_schemas(&index, robo_mode);
    println!("{} schemas generated", schemas.len());

    // -----------------------------------------------------------------------
    // Pass 2: Parallel compute + sharded lock-free write
    // -----------------------------------------------------------------------
    // Rayon threads process segments and send WritableBatches to shard threads.
    // Each shard thread owns a set of files exclusively (hashed by file_key),
    // so there is zero Mutex contention on writers.
    let t1 = Instant::now();
    let num_shards = 16;
    let pool = ShardedWriterPool::new(num_shards, Compression::SNAPPY, robo_mode);

    // Use a smaller rayon pool for compute so shard threads get more CPU.
    // Compute only needs ~10s of wall time — 8 threads is plenty.
    let compute_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(5)
        .build()
        .unwrap();

    // Write class hierarchy metadata (tiny — 1,781 rows)
    if robo_mode {
        write_class_hierarchy(&index);
    }

    compute_pool.install(|| {
        segments.par_iter().for_each(|record| {
            let batches = process_segment_to_batches(record, hprof, &index, &schemas, robo_mode);
            for wb in batches {
                pool.write_batch(wb);
            }
        });
    });

    // Write static fields, stack frames, and stack traces through the pool
    if let Some(sb) = build_static_fields_batch(&index, robo_mode) {
        pool.write_batch(sb);
    }
    if let Some(sf) = build_stack_frames_batch(&index) {
        pool.write_batch(sf);
    }
    if let Some(st) = build_stack_traces_batch(&index) {
        pool.write_batch(st);
    }

    let pass2_dur = t1.elapsed();
    println!("Pass 2 in {:.1}s", pass2_dur.as_secs_f64());

    // Close all shard threads and their writers
    let t2 = Instant::now();
    pool.close_all();
    println!("Writers closed in {:.1}s", t2.elapsed().as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hprof_index::{HprofIndex, ResolvedStackFrame, ResolvedStackTrace};
    use arrow_array::{cast::AsArray, types::UInt64Type};

    /// Create a minimal HprofIndex with only stack_frames and stack_traces populated.
    fn make_test_index<'a>(
        frames: Vec<ResolvedStackFrame<'a>>,
        traces: Vec<ResolvedStackTrace>,
    ) -> HprofIndex<'a> {
        HprofIndex {
            utf8: HashMap::new(),
            load_classes: HashMap::new(),
            classes: HashMap::new(),
            obj_id_to_class_obj_id: DashMap::new(),
            prim_array_obj_id_to_type: DashMap::new(),
            class_instance_field_descriptors: HashMap::new(),
            class_field_declaring_classes: HashMap::new(),
            stack_frames: frames,
            stack_traces: traces,
        }
    }

    // -----------------------------------------------------------------------
    // build_stack_frames_batch tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_stack_frames_batch_empty_returns_none() {
        let index = make_test_index(vec![], vec![]);
        assert!(build_stack_frames_batch(&index).is_none());
    }

    #[test]
    fn test_build_stack_frames_batch_single_frame() {
        let frames = vec![ResolvedStackFrame {
            frame_id: 42,
            method_name: "run",
            method_signature: "()V",
            source_file: "Main.java",
            class_name: "com.example.Main",
            line_num: 100,
        }];
        let index = make_test_index(frames, vec![]);
        let wb = build_stack_frames_batch(&index).expect("should produce a batch");

        assert_eq!(wb.file_key, "_stack_frames");
        assert_eq!(wb.batch.num_rows(), 1);
        assert_eq!(wb.batch.num_columns(), 6);

        // Verify column values
        let frame_ids = wb.batch.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(frame_ids.value(0), 42);

        let class_names = wb.batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(class_names.value(0), "com.example.Main");

        let method_names = wb.batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(method_names.value(0), "run");

        let method_sigs = wb.batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(method_sigs.value(0), "()V");

        let source_files = wb.batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(source_files.value(0), "Main.java");

        let line_nums = wb.batch.column(5).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(line_nums.value(0), 100);
    }

    #[test]
    fn test_build_stack_frames_batch_multiple_frames() {
        let frames = vec![
            ResolvedStackFrame {
                frame_id: 1,
                method_name: "methodA",
                method_signature: "(I)V",
                source_file: "A.java",
                class_name: "com.example.A",
                line_num: 10,
            },
            ResolvedStackFrame {
                frame_id: 2,
                method_name: "methodB",
                method_signature: "(Ljava/lang/String;)I",
                source_file: "B.java",
                class_name: "com.example.B",
                line_num: -1, // unknown
            },
            ResolvedStackFrame {
                frame_id: 3,
                method_name: "nativeCall",
                method_signature: "()J",
                source_file: "(unknown)",
                class_name: "sun.misc.Unsafe",
                line_num: -3, // native method
            },
        ];
        let index = make_test_index(frames, vec![]);
        let wb = build_stack_frames_batch(&index).unwrap();

        assert_eq!(wb.batch.num_rows(), 3);

        let frame_ids = wb.batch.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(frame_ids.value(0), 1);
        assert_eq!(frame_ids.value(1), 2);
        assert_eq!(frame_ids.value(2), 3);

        let line_nums = wb.batch.column(5).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(line_nums.value(0), 10);
        assert_eq!(line_nums.value(1), -1); // unknown
        assert_eq!(line_nums.value(2), -3); // native
    }

    #[test]
    fn test_build_stack_frames_batch_all_line_num_variants() {
        let frames = vec![
            ResolvedStackFrame {
                frame_id: 1, method_name: "a", method_signature: "",
                source_file: "", class_name: "", line_num: 42, // Normal
            },
            ResolvedStackFrame {
                frame_id: 2, method_name: "b", method_signature: "",
                source_file: "", class_name: "", line_num: -1, // Unknown
            },
            ResolvedStackFrame {
                frame_id: 3, method_name: "c", method_signature: "",
                source_file: "", class_name: "", line_num: -2, // Compiled
            },
            ResolvedStackFrame {
                frame_id: 4, method_name: "d", method_signature: "",
                source_file: "", class_name: "", line_num: -3, // Native
            },
        ];
        let index = make_test_index(frames, vec![]);
        let wb = build_stack_frames_batch(&index).unwrap();

        let line_nums = wb.batch.column(5).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(line_nums.value(0), 42);
        assert_eq!(line_nums.value(1), -1);
        assert_eq!(line_nums.value(2), -2);
        assert_eq!(line_nums.value(3), -3);
    }

    #[test]
    fn test_build_stack_frames_batch_schema() {
        let frames = vec![ResolvedStackFrame {
            frame_id: 1, method_name: "m", method_signature: "()V",
            source_file: "X.java", class_name: "X", line_num: 1,
        }];
        let index = make_test_index(frames, vec![]);
        let wb = build_stack_frames_batch(&index).unwrap();

        let fields: Vec<(&str, &DataType)> = wb.schema.fields().iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect();
        assert_eq!(fields, vec![
            ("frame_id", &DataType::UInt64),
            ("class_name", &DataType::Utf8),
            ("method_name", &DataType::Utf8),
            ("method_signature", &DataType::Utf8),
            ("source_file", &DataType::Utf8),
            ("line_num", &DataType::Int32),
        ]);
    }

    // -----------------------------------------------------------------------
    // build_stack_traces_batch tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_build_stack_traces_batch_empty_returns_none() {
        let index = make_test_index(vec![], vec![]);
        assert!(build_stack_traces_batch(&index).is_none());
    }

    #[test]
    fn test_build_stack_traces_batch_single_trace() {
        let traces = vec![ResolvedStackTrace {
            stack_trace_serial: 1,
            thread_serial: 10,
            frame_ids: vec![100, 200, 300],
        }];
        let index = make_test_index(vec![], traces);
        let wb = build_stack_traces_batch(&index).expect("should produce a batch");

        assert_eq!(wb.file_key, "_stack_traces");
        assert_eq!(wb.batch.num_rows(), 1);
        assert_eq!(wb.batch.num_columns(), 3);

        let trace_serials = wb.batch.column(0).as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(trace_serials.value(0), 1);

        let thread_serials = wb.batch.column(1).as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(thread_serials.value(0), 10);

        // Verify list column
        let frame_ids_col = wb.batch.column(2).as_list::<i32>();
        let row0 = frame_ids_col.value(0);
        let values = row0.as_primitive::<UInt64Type>();
        assert_eq!(values.values(), &[100, 200, 300]);
    }

    #[test]
    fn test_build_stack_traces_batch_multiple_traces_varying_depth() {
        let traces = vec![
            ResolvedStackTrace {
                stack_trace_serial: 1,
                thread_serial: 10,
                frame_ids: vec![100],
            },
            ResolvedStackTrace {
                stack_trace_serial: 2,
                thread_serial: 20,
                frame_ids: vec![200, 300, 400, 500],
            },
            ResolvedStackTrace {
                stack_trace_serial: 3,
                thread_serial: 30,
                frame_ids: vec![], // empty stack trace (e.g., system thread)
            },
        ];
        let index = make_test_index(vec![], traces);
        let wb = build_stack_traces_batch(&index).unwrap();

        assert_eq!(wb.batch.num_rows(), 3);

        let trace_serials = wb.batch.column(0).as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(trace_serials.value(0), 1);
        assert_eq!(trace_serials.value(1), 2);
        assert_eq!(trace_serials.value(2), 3);

        let frame_ids_col = wb.batch.column(2).as_list::<i32>();

        // Trace 1: single frame
        let row0 = frame_ids_col.value(0);
        assert_eq!(row0.as_primitive::<UInt64Type>().values(), &[100]);

        // Trace 2: four frames
        let row1 = frame_ids_col.value(1);
        assert_eq!(row1.as_primitive::<UInt64Type>().values(), &[200, 300, 400, 500]);

        // Trace 3: empty
        let row2 = frame_ids_col.value(2);
        assert_eq!(row2.as_primitive::<UInt64Type>().len(), 0);
    }

    #[test]
    fn test_build_stack_traces_batch_schema() {
        let traces = vec![ResolvedStackTrace {
            stack_trace_serial: 1, thread_serial: 1, frame_ids: vec![1],
        }];
        let index = make_test_index(vec![], traces);
        let wb = build_stack_traces_batch(&index).unwrap();

        let fields: Vec<(&str, &DataType)> = wb.schema.fields().iter()
            .map(|f| (f.name().as_str(), f.data_type()))
            .collect();
        assert_eq!(fields[0], ("stack_trace_serial", &DataType::UInt32));
        assert_eq!(fields[1], ("thread_serial", &DataType::UInt32));
        assert_eq!(fields[2].0, "frame_ids");
        match fields[2].1 {
            DataType::List(_) => {} // expected
            other => panic!("Expected List type for frame_ids, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Integration: both batches built from same index
    // -----------------------------------------------------------------------

    #[test]
    fn test_both_batches_from_populated_index() {
        let frames = vec![
            ResolvedStackFrame {
                frame_id: 100, method_name: "run", method_signature: "()V",
                source_file: "Thread.java", class_name: "java.lang.Thread", line_num: 748,
            },
            ResolvedStackFrame {
                frame_id: 200, method_name: "main", method_signature: "([Ljava/lang/String;)V",
                source_file: "App.java", class_name: "com.example.App", line_num: 15,
            },
        ];
        let traces = vec![ResolvedStackTrace {
            stack_trace_serial: 1,
            thread_serial: 5,
            frame_ids: vec![100, 200],
        }];
        let index = make_test_index(frames, traces);

        let sf_batch = build_stack_frames_batch(&index).unwrap();
        let st_batch = build_stack_traces_batch(&index).unwrap();

        assert_eq!(sf_batch.batch.num_rows(), 2);
        assert_eq!(st_batch.batch.num_rows(), 1);

        // Verify frame_ids in the trace reference valid frame_id values from frames
        let trace_frame_ids = st_batch.batch.column(2).as_list::<i32>().value(0);
        let trace_fids = trace_frame_ids.as_primitive::<UInt64Type>();
        let frame_id_col = sf_batch.batch.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..trace_fids.len() {
            let fid = trace_fids.value(i);
            let found = (0..frame_id_col.len()).any(|j| frame_id_col.value(j) == fid);
            assert!(found, "frame_id {} from trace not found in stack_frames", fid);
        }
    }
}
