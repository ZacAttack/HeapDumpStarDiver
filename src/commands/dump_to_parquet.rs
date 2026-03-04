use std::collections::HashMap;
use std::sync::Arc;
use arrow_array::{Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, StructArray, UInt16Array, UInt32Array, UInt64Array};
use arrow_array::builder::{ListBuilder, BooleanBuilder, Int8Builder, UInt16Builder, Int16Builder, Int32Builder, Int64Builder, Float32Builder, Float64Builder, UInt64Builder};
use arrow_schema::{DataType, Field, Schema};
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
    ObjectReference(Id),
    PrimitiveArrayReference(Id),
}

// Ensure ExtendedFieldValue can be sent across threads for parallel column building
unsafe impl Send for ExtendedFieldValue {}
unsafe impl Sync for ExtendedFieldValue {}

/// Appends one instance's field values to the positional column vecs.
fn add_instance_values(
    hprof: &Hprof,
    field_columns: &mut Vec<Vec<ExtendedFieldValue>>,
    mut field_val_input: &[u8],
    field_descriptors: &[jvm_hprof::heap_dump::FieldDescriptor],
    obj_id_to_class_obj_id: &HashMap<Id, Id>,
    prim_array_obj_id_to_type: &HashMap<Id, PrimitiveArrayType>,
) {
    for (i, fd) in field_descriptors.iter().enumerate() {
        let (input, field_val) = fd
            .field_type()
            .parse_value(field_val_input, hprof.header().id_size())
            .unwrap();
        field_val_input = input;
        match field_val {
            FieldValue::ObjectId(Some(field_ref_id)) => {
                if obj_id_to_class_obj_id.contains_key(&field_ref_id) {
                    field_columns[i].push(ExtendedFieldValue::ObjectReference(field_ref_id));
                } else if prim_array_obj_id_to_type.contains_key(&field_ref_id) {
                    field_columns[i].push(ExtendedFieldValue::PrimitiveArrayReference(field_ref_id));
                } else {
                    field_columns[i].push(ExtendedFieldValue::ObjectReference(field_ref_id));
                }
            }
            FieldValue::ObjectId(None) => {
                field_columns[i].push(ExtendedFieldValue::ObjectReference(Id::from(0)));
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
                ExtendedFieldValue::ObjectReference(val)
                | ExtendedFieldValue::PrimitiveArrayReference(val) => val.id(),
                _ => 0,
            }).collect::<Vec<u64>>();
            let type_vec = field_val_vec.iter().map(|v| match v {
                ExtendedFieldValue::ObjectReference(val)
                | ExtendedFieldValue::PrimitiveArrayReference(val) => {
                    resolve_ref_type(*val, index)
                },
                _ => "null".to_string(),
            }).collect::<Vec<String>>();
            let id_array: Arc<dyn Array> = Arc::new(UInt64Array::from(id_vec));
            let type_array: Arc<dyn Array> = Arc::new(StringArray::from(type_vec));
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
                ExtendedFieldValue::FieldValue(FieldValue::ObjectId(val)) => val.unwrap().id(),
                _ => 0,
            }).collect::<Vec<u64>>()))
        }
        _ => panic!("Unsupported schema data type: {:?}", expected_type),
    }
}

fn resolve_ref_type(id: Id, index: &HprofIndex) -> String {
    if id.id() == 0 {
        return "null".to_string();
    }
    index.obj_id_to_class_obj_id
        .get(&id)
        .and_then(|class_obj_id| index.classes.get(class_obj_id))
        .map(|c| c.name.to_string())
        .or_else(|| index.prim_array_obj_id_to_type.get(&id).map(|pt| format!("{}[]", pt.java_type_name())))
        .or_else(|| index.classes.get(&id).map(|c| format!("class {}", c.name)))
        .unwrap_or_else(|| "(unresolved)".to_string())
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

    // --- Parse sub-records ---
    let segment = record.as_heap_dump_segment().unwrap().unwrap();
    for p in segment.sub_records() {
        let s = p.unwrap();
        match s {
            SubRecord::Instance(instance) => {
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
                    &index.obj_id_to_class_obj_id, &index.prim_array_obj_id_to_type,
                );
            }
            SubRecord::PrimitiveArray(pa) => {
                let obj_id = pa.obj_id().id();
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

    batches
}

// ---------------------------------------------------------------------------
// Schema generation
// ---------------------------------------------------------------------------

fn generate_all_schemas(index: &HprofIndex) -> HashMap<Id, Schema> {
    index.class_instance_field_descriptors.iter()
        .map(|(class_id, field_descriptors)| {
            let schema = generate_schema_from_descriptors(
                field_descriptors,
                &index.utf8,
                index.class_field_declaring_classes.get(class_id),
            );
            (*class_id, schema)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Static fields writer
// ---------------------------------------------------------------------------

fn build_static_fields_batch(index: &HprofIndex) -> Option<WritableBatch> {
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
                if rt.is_empty() && rid != 0 {
                    ref_types.push(resolve_ref_type(Id::from(rid), index));
                } else {
                    ref_types.push(rt);
                }
                primitive_values.push(String::new());
            } else {
                ref_ids.push(0);
                ref_types.push(String::new());
                primitive_values.push(pv);
            }
        }
    }

    if class_obj_ids.is_empty() {
        return None;
    }

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

// ---------------------------------------------------------------------------
// SharedWriterPool — thread-safe pool with per-writer Mutex locking
// ---------------------------------------------------------------------------
// Outer Mutex protects the HashMap for lazy writer creation.
// Inner Mutex protects individual ArrowWriters so multiple threads can write
// to different files concurrently — they only block when two threads need
// the same file.

use std::sync::{Arc as StdArc, Mutex};

struct SharedWriterPool {
    writers: Mutex<HashMap<String, StdArc<Mutex<ArrowWriter<std::fs::File>>>>>,
    props: WriterProperties,
}

impl SharedWriterPool {
    fn new() -> Self {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        SharedWriterPool {
            writers: Mutex::new(HashMap::new()),
            props,
        }
    }

    fn write_batch(&self, file_key: &str, schema: Arc<Schema>, batch: &RecordBatch) {
        // Hold outer lock briefly: get or create the writer, clone the Arc
        let writer_arc = {
            let mut map = self.writers.lock().unwrap();
            map.entry(file_key.to_string()).or_insert_with(|| {
                let safe_name = file_key.replace("/", ".");
                let file = std::fs::File::create(format!("parquet/{}.parquet", safe_name)).unwrap();
                StdArc::new(Mutex::new(
                    ArrowWriter::try_new(file, schema, Some(self.props.clone())).unwrap()
                ))
            }).clone()
        };
        // Per-writer lock: encoding + compression happens here, other files unblocked
        writer_arc.lock().unwrap().write(batch).unwrap();
    }

    fn close_all(self) {
        let map = self.writers.into_inner().unwrap();
        let writers: Vec<_> = map.into_iter().collect();
        writers.into_par_iter().for_each(|(_name, writer_arc)| {
            let writer = StdArc::try_unwrap(writer_arc)
                .expect("writer Arc still has multiple owners")
                .into_inner().unwrap();
            writer.close().unwrap();
        });
    }
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub fn dump_objects_to_parquet(hprof: &Hprof, _flush_row_threshold: usize) {
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

    // Generate schemas from field descriptors (no file scan needed)
    let schemas = generate_all_schemas(&index);
    println!("{} schemas generated", schemas.len());

    // -----------------------------------------------------------------------
    // Pass 2: Fully parallel compute + write (one file per class)
    // -----------------------------------------------------------------------
    // Each rayon thread processes a segment, builds RecordBatches, and writes
    // them directly to the shared writer pool. Different files are written
    // concurrently; same-file access is serialized by per-writer Mutexes.
    let t1 = Instant::now();
    let pool = SharedWriterPool::new();

    segments.par_iter().for_each(|record| {
        let batches = process_segment_to_batches(record, hprof, &index, &schemas);
        for wb in batches {
            pool.write_batch(&wb.file_key, wb.schema, &wb.batch);
        }
    });

    // Write static fields
    if let Some(sb) = build_static_fields_batch(&index) {
        pool.write_batch(&sb.file_key, sb.schema.clone(), &sb.batch);
    }

    let pass2_dur = t1.elapsed();
    println!("Pass 2 in {:.1}s", pass2_dur.as_secs_f64());

    // Close all writers in parallel (writes parquet footers)
    let t2 = Instant::now();
    pool.close_all();
    println!("Writers closed in {:.1}s", t2.elapsed().as_secs_f64());
}
