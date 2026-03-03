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
// ParquetWriterPool — keeps one ArrowWriter<File> open per output file
// ---------------------------------------------------------------------------

struct ParquetWriterPool {
    writers: HashMap<String, ArrowWriter<std::fs::File>>,
    props: WriterProperties,
}

impl ParquetWriterPool {
    fn new() -> Self {
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        ParquetWriterPool {
            writers: HashMap::new(),
            props,
        }
    }

    fn write_batch(&mut self, filename_prefix: &str, schema: Arc<Schema>, batch: &RecordBatch) {
        let writer = self.writers.entry(filename_prefix.to_string()).or_insert_with(|| {
            let safe_name = filename_prefix.replace("/", ".");
            let file = std::fs::File::create(format!("parquet/{}.parquet", safe_name)).unwrap();
            ArrowWriter::try_new(file, schema.clone(), Some(self.props.clone())).unwrap()
        });
        writer.write(batch).unwrap();
    }

    fn close_all(self) {
        // Close writers in parallel — each close writes the Parquet footer
        let writers: Vec<(String, ArrowWriter<std::fs::File>)> = self.writers.into_iter().collect();
        writers.into_par_iter().for_each(|(_name, writer)| {
            writer.close().unwrap();
        });
    }
}

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
/// `field_columns[i]` collects values for the i-th field descriptor.
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
                    // Class reference or unresolvable — still record the id
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
/// DataType to determine the output type. This prevents type mismatches between
/// flush batches when a field's first element variant differs across flushes.
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
    // Returns (field_type, primitive_value, ref_id, ref_type_placeholder)
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
// SegmentDataResult — thread-local extraction buffer (all Send-safe types)
// ---------------------------------------------------------------------------

/// Per-segment extraction result. Uses Vec-based buffers instead of Arrow's
/// ListBuilder (which is !Send) so it can be produced inside rayon threads.
struct SegmentDataResult {
    /// Instances: class_obj_id → (obj_ids, field_columns)
    instances: HashMap<Id, (Vec<u64>, Vec<Vec<ExtendedFieldValue>>)>,
    /// Object arrays
    oa_obj_ids: Vec<u64>,
    oa_class_names: Vec<String>,
    oa_elements: Vec<Vec<u64>>,
    /// Primitive arrays — Vec of (obj_id, values)
    bool_arrays: Vec<(u64, Vec<bool>)>,
    byte_arrays: Vec<(u64, Vec<i8>)>,
    char_arrays: Vec<(u64, Vec<u16>)>,
    short_arrays: Vec<(u64, Vec<i16>)>,
    int_arrays: Vec<(u64, Vec<i32>)>,
    long_arrays: Vec<(u64, Vec<i64>)>,
    float_arrays: Vec<(u64, Vec<f32>)>,
    double_arrays: Vec<(u64, Vec<f64>)>,
    /// GC roots: (type, obj_id, thread_serial, frame_index)
    gc_roots: Vec<(String, u64, Option<u32>, Option<u32>)>,
    /// Number of data rows collected (for flush threshold tracking)
    row_count: usize,
}

impl SegmentDataResult {
    fn new() -> Self {
        SegmentDataResult {
            instances: HashMap::new(),
            oa_obj_ids: Vec::new(),
            oa_class_names: Vec::new(),
            oa_elements: Vec::new(),
            bool_arrays: Vec::new(),
            byte_arrays: Vec::new(),
            char_arrays: Vec::new(),
            short_arrays: Vec::new(),
            int_arrays: Vec::new(),
            long_arrays: Vec::new(),
            float_arrays: Vec::new(),
            double_arrays: Vec::new(),
            gc_roots: Vec::new(),
            row_count: 0,
        }
    }
}

/// Process a single HeapDump segment Record, extracting all data into a SegmentDataResult.
fn process_segment_data<'a>(
    record: &Record<'a>,
    hprof: &Hprof,
    index: &HprofIndex,
) -> SegmentDataResult {
    let mut result = SegmentDataResult::new();
    let segment = record.as_heap_dump_segment().unwrap().unwrap();

    for p in segment.sub_records() {
        let s = p.unwrap();
        match s {
            SubRecord::Instance(instance) => {
                let field_descriptors = match index.class_instance_field_descriptors
                    .get(&instance.class_obj_id())
                {
                    Some(fd) => fd,
                    None => continue, // skip instances whose class wasn't indexed
                };

                let entry = result.instances
                    .entry(instance.class_obj_id())
                    .or_insert_with(|| {
                        let columns: Vec<Vec<ExtendedFieldValue>> =
                            (0..field_descriptors.len()).map(|_| Vec::new()).collect();
                        (Vec::new(), columns)
                    });

                entry.0.push(instance.obj_id().id());

                add_instance_values(
                    hprof,
                    &mut entry.1,
                    instance.fields(),
                    field_descriptors,
                    &index.obj_id_to_class_obj_id,
                    &index.prim_array_obj_id_to_type,
                );

                result.row_count += 1;
            }
            SubRecord::PrimitiveArray(pa) => {
                let obj_id = pa.obj_id().id();
                match pa.primitive_type() {
                    PrimitiveArrayType::Boolean => {
                        let vals: Vec<bool> = pa.booleans().unwrap().map(|v| v.unwrap()).collect();
                        result.bool_arrays.push((obj_id, vals));
                    }
                    PrimitiveArrayType::Byte => {
                        let vals: Vec<i8> = pa.bytes().unwrap().map(|v| v.unwrap()).collect();
                        result.byte_arrays.push((obj_id, vals));
                    }
                    PrimitiveArrayType::Char => {
                        let vals: Vec<u16> = pa.chars().unwrap().map(|v| v.unwrap() as u16).collect();
                        result.char_arrays.push((obj_id, vals));
                    }
                    PrimitiveArrayType::Short => {
                        let vals: Vec<i16> = pa.shorts().unwrap().map(|v| v.unwrap()).collect();
                        result.short_arrays.push((obj_id, vals));
                    }
                    PrimitiveArrayType::Int => {
                        let vals: Vec<i32> = pa.ints().unwrap().map(|v| v.unwrap()).collect();
                        result.int_arrays.push((obj_id, vals));
                    }
                    PrimitiveArrayType::Long => {
                        let vals: Vec<i64> = pa.longs().unwrap().map(|v| v.unwrap()).collect();
                        result.long_arrays.push((obj_id, vals));
                    }
                    PrimitiveArrayType::Float => {
                        let vals: Vec<f32> = pa.floats().unwrap().map(|v| v.unwrap()).collect();
                        result.float_arrays.push((obj_id, vals));
                    }
                    PrimitiveArrayType::Double => {
                        let vals: Vec<f64> = pa.doubles().unwrap().map(|v| v.unwrap()).collect();
                        result.double_arrays.push((obj_id, vals));
                    }
                }
                result.row_count += 1;
            }
            SubRecord::ObjectArray(oa) => {
                result.oa_obj_ids.push(oa.obj_id().id());

                let class_name = index.classes.get(&oa.array_class_obj_id())
                    .map(|c| c.name.to_string())
                    .unwrap_or_else(|| "(unresolved)".to_string());
                result.oa_class_names.push(class_name);

                let elems: Vec<u64> = oa.elements(hprof.header().id_size())
                    .map(|elem| match elem.unwrap() {
                        Some(id) => id.id(),
                        None => 0,
                    })
                    .collect();
                result.oa_elements.push(elems);

                result.row_count += 1;
            }
            SubRecord::GcRootUnknown(r) => {
                result.gc_roots.push(("Unknown".into(), r.obj_id().id(), None, None));
            }
            SubRecord::GcRootThreadObj(r) => {
                result.gc_roots.push(("ThreadObj".into(), r.thread_obj_id().map(|id| id.id()).unwrap_or(0), Some(r.thread_serial().num()), None));
            }
            SubRecord::GcRootJniGlobal(r) => {
                result.gc_roots.push(("JniGlobal".into(), r.obj_id().id(), None, None));
            }
            SubRecord::GcRootJniLocalRef(r) => {
                result.gc_roots.push(("JniLocal".into(), r.obj_id().id(), Some(r.thread_serial().num()), r.frame_index()));
            }
            SubRecord::GcRootJavaStackFrame(r) => {
                result.gc_roots.push(("JavaStackFrame".into(), r.obj_id().id(), Some(r.thread_serial().num()), r.frame_index()));
            }
            SubRecord::GcRootNativeStack(r) => {
                result.gc_roots.push(("NativeStack".into(), r.obj_id().id(), Some(r.thread_serial().num()), None));
            }
            SubRecord::GcRootSystemClass(r) => {
                result.gc_roots.push(("SystemClass".into(), r.obj_id().id(), None, None));
            }
            SubRecord::GcRootThreadBlock(r) => {
                result.gc_roots.push(("ThreadBlock".into(), r.obj_id().id(), Some(r.thread_serial().num()), None));
            }
            SubRecord::GcRootBusyMonitor(r) => {
                result.gc_roots.push(("BusyMonitor".into(), r.obj_id().id(), None, None));
            }
            _ => {}
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Merge helpers — fold SegmentDataResult into main accumulators
// ---------------------------------------------------------------------------

/// Flush a SegmentDataResult directly to the writer pool — no intermediate merge step.
/// Instance columns are built and written per-class. Primitive/object arrays use ListBuilder
/// (built on the main thread) and written immediately.
fn flush_segment_data(
    seg: SegmentDataResult,
    pool: &mut ParquetWriterPool,
    schemas: &HashMap<Id, Schema>,
    index: &HprofIndex,
) {
    // Flush instances per-class
    let work_items: Vec<(Id, Vec<u64>, Vec<Vec<ExtendedFieldValue>>)> = seg.instances.into_iter()
        .filter(|(cid, _)| schemas.contains_key(cid))
        .map(|(cid, (obj_ids, field_cols))| (cid, obj_ids, field_cols))
        .collect();

    if !work_items.is_empty() {
        let batches: Vec<(String, Arc<Schema>, RecordBatch)> = work_items.into_par_iter()
            .map(|(class_id, obj_ids, field_columns)| {
                let schema = schemas.get(&class_id).unwrap();
                let mut fields = vec![Field::new("obj_id", DataType::UInt64, false)];
                fields.extend(schema.fields().iter().map(|f| f.as_ref().clone()));
                let full_schema = Arc::new(Schema::new(fields));

                let data_columns: Vec<Arc<dyn Array>> = field_columns.par_iter()
                    .zip(schema.fields().into_par_iter())
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
                (file_key, full_schema, batch)
            })
            .collect();

        for (file_key, full_schema, batch) in batches {
            pool.write_batch(&file_key, full_schema, &batch);
        }
    }

    // Flush object arrays
    if !seg.oa_obj_ids.is_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("obj_id", DataType::UInt64, false),
            Field::new("class_name", DataType::Utf8, false),
            Field::new("elements", DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))), false),
        ]));
        let mut oa_elements = ListBuilder::new(UInt64Builder::new());
        for elems in &seg.oa_elements {
            for e in elems { oa_elements.values().append_value(*e); }
            oa_elements.append(true);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(seg.oa_obj_ids)) as Arc<dyn Array>,
                Arc::new(StringArray::from(seg.oa_class_names)) as Arc<dyn Array>,
                Arc::new(oa_elements.finish()) as Arc<dyn Array>,
            ],
        ).unwrap();
        pool.write_batch("_object_arrays", schema, &batch);
    }

    // Flush primitive arrays
    macro_rules! flush_seg_prim {
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
                pool.write_batch($name, schema, &batch);
            }
        };
    }

    flush_seg_prim!(seg.bool_arrays, "_primitive_arrays_boolean", DataType::Boolean, BooleanBuilder);
    flush_seg_prim!(seg.byte_arrays, "_primitive_arrays_byte", DataType::Int8, Int8Builder);
    flush_seg_prim!(seg.char_arrays, "_primitive_arrays_char", DataType::UInt16, UInt16Builder);
    flush_seg_prim!(seg.short_arrays, "_primitive_arrays_short", DataType::Int16, Int16Builder);
    flush_seg_prim!(seg.int_arrays, "_primitive_arrays_int", DataType::Int32, Int32Builder);
    flush_seg_prim!(seg.long_arrays, "_primitive_arrays_long", DataType::Int64, Int64Builder);
    flush_seg_prim!(seg.float_arrays, "_primitive_arrays_float", DataType::Float32, Float32Builder);
    flush_seg_prim!(seg.double_arrays, "_primitive_arrays_double", DataType::Float64, Float64Builder);

    // Flush GC roots
    if !seg.gc_roots.is_empty() {
        let mut types = Vec::with_capacity(seg.gc_roots.len());
        let mut obj_ids = Vec::with_capacity(seg.gc_roots.len());
        let mut thread_serials = Vec::with_capacity(seg.gc_roots.len());
        let mut frame_indexes = Vec::with_capacity(seg.gc_roots.len());
        for (rt, oid, ts, fi) in seg.gc_roots {
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
        pool.write_batch("_gc_roots", schema, &batch);
    }
}

/// Generate Arrow schemas for all classes from their field descriptors.
/// No instance data needed — the type mapping is deterministic from FieldType.
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
// Flush helpers
// ---------------------------------------------------------------------------

fn write_static_fields(pool: &mut ParquetWriterPool, index: &HprofIndex) {
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
        return;
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

    pool.write_batch("_static_fields", schema, &batch);
}

// ---------------------------------------------------------------------------
// Main entry point — Three-phase parallel pipeline
// ---------------------------------------------------------------------------

pub fn dump_objects_to_parquet(hprof: &Hprof, _flush_row_threshold: usize) {
    // Clean output directory so stale files from previous runs don't persist
    let _ = std::fs::remove_dir_all("parquet");
    std::fs::create_dir_all("parquet").unwrap();

    // -----------------------------------------------------------------------
    // Pass 1: Sequential index build + collect segment handles (single pass)
    // -----------------------------------------------------------------------
    let (index, segments) = HprofIndex::build_with_segments(hprof);

    println!("Pass 1 complete: {} classes, {} obj mappings, {} segments",
        index.classes.len(), index.obj_id_to_class_obj_id.len(), segments.len());

    // Generate schemas from field descriptors (no file scan needed)
    let schemas = generate_all_schemas(&index);

    println!("{} schemas generated", schemas.len());

    // -----------------------------------------------------------------------
    // Pass 2: Parallel data extraction → direct flush (no merge step)
    // -----------------------------------------------------------------------
    // Each batch of segments is processed in parallel, then each segment's
    // data is flushed directly to parquet. This eliminates the sequential
    // merge bottleneck — no shared accumulators.
    let mut pool = ParquetWriterPool::new();

    // Process segments in batches to control memory usage.
    let batch_size = segments.len().min(256).max(1);
    for batch in segments.chunks(batch_size) {
        // Process this batch of segments in parallel
        let segment_results: Vec<SegmentDataResult> = batch.par_iter()
            .map(|record| process_segment_data(record, hprof, &index))
            .collect();

        // Flush each segment's results directly (no merge into shared accumulators)
        for seg in segment_results {
            flush_segment_data(seg, &mut pool, &schemas, &index);
        }
    }

    println!("Pass 2 complete: extracted data from {} segments", segments.len());

    // Write static fields (small, one-shot)
    write_static_fields(&mut pool, &index);

    // Close all writers in parallel (writes parquet footers)
    pool.close_all();
}
