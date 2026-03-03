use std::collections::HashMap;
use std::sync::Arc;
use arrow_array::{Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, StructArray, UInt16Array, UInt32Array, UInt64Array};
use arrow_array::builder::{ListBuilder, BooleanBuilder, Int8Builder, UInt16Builder, Int16Builder, Int32Builder, Int64Builder, Float32Builder, Float64Builder, UInt64Builder};
use arrow_schema::{DataType, Field, Schema};
use jvm_hprof::{Hprof, Id, RecordTag};
use jvm_hprof::heap_dump::{FieldType, FieldValue, PrimitiveArrayType, SubRecord};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rayon::prelude::*;
use crate::hprof_index::HprofIndex;
use crate::util::generate_schema_from_type;

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
// Flush helpers — drain accumulated buffers into the writer pool
// ---------------------------------------------------------------------------

/// Build RecordBatches in parallel using rayon, then write them sequentially.
/// Column building (especially struct columns with type resolution) is the
/// CPU-intensive part, while Parquet writes are I/O-bound and sequential per writer.
fn flush_instance_buffers(
    pool: &mut ParquetWriterPool,
    schemas: &HashMap<Id, Schema>,
    class_field_columns: &mut HashMap<Id, Vec<Vec<ExtendedFieldValue>>>,
    class_obj_ids: &mut HashMap<Id, Vec<u64>>,
    index: &HprofIndex,
) {
    // Collect classes that have data to flush
    let class_ids_to_flush: Vec<Id> = schemas.keys()
        .filter(|class_id| {
            class_obj_ids.get(class_id).map_or(false, |v| !v.is_empty())
        })
        .cloned()
        .collect();

    if class_ids_to_flush.is_empty() {
        return;
    }

    // Take ownership of buffers for parallel processing
    let work_items: Vec<(Id, Vec<u64>, Vec<Vec<ExtendedFieldValue>>)> = class_ids_to_flush.iter()
        .map(|class_id| {
            let obj_ids = std::mem::take(class_obj_ids.get_mut(class_id).unwrap());
            let columns = class_field_columns.get_mut(class_id).unwrap();
            let taken_columns: Vec<Vec<ExtendedFieldValue>> = columns.iter_mut()
                .map(|col| std::mem::take(col))
                .collect();
            (*class_id, obj_ids, taken_columns)
        })
        .collect();

    // Build RecordBatches in parallel (CPU-intensive column building + type resolution)
    let batches: Vec<(String, Arc<Schema>, RecordBatch)> = work_items.into_par_iter()
        .map(|(class_id, obj_ids, field_columns)| {
            let schema = schemas.get(&class_id).unwrap();

            let mut fields = vec![Field::new("obj_id", DataType::UInt64, false)];
            fields.extend(schema.fields().iter().map(|f| f.as_ref().clone()));
            let full_schema = Arc::new(Schema::new(fields));

            // Build columns in parallel within each class
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
            // Use class_id in the file key to disambiguate classes loaded by
            // different classloaders that share the same fully-qualified name.
            let file_key = format!("{}_{}", class_name, class_id);
            (file_key, full_schema, batch)
        })
        .collect();

    // Write batches sequentially (writers are not thread-safe)
    for (file_key, full_schema, batch) in batches {
        pool.write_batch(&file_key, full_schema, &batch);
    }
}

fn flush_object_array_buffer(
    pool: &mut ParquetWriterPool,
    oa_obj_ids: &mut Vec<u64>,
    oa_class_names: &mut Vec<String>,
    oa_elements: &mut ListBuilder<UInt64Builder>,
) {
    if oa_obj_ids.is_empty() {
        return;
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("obj_id", DataType::UInt64, false),
        Field::new("class_name", DataType::Utf8, false),
        Field::new("elements", DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))), false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(std::mem::take(oa_obj_ids))) as Arc<dyn Array>,
            Arc::new(StringArray::from(std::mem::take(oa_class_names))) as Arc<dyn Array>,
            Arc::new(oa_elements.finish()) as Arc<dyn Array>,
        ],
    ).unwrap();

    pool.write_batch("_object_arrays", schema, &batch);
}

macro_rules! flush_prim_array_buffer {
    ($pool:expr, $name:expr, $obj_ids:expr, $list_builder:expr, $inner_type:expr) => {
        if !$obj_ids.is_empty() {
            let schema = Arc::new(Schema::new(vec![
                Field::new("obj_id", DataType::UInt64, false),
                Field::new("values", DataType::List(Arc::new(Field::new("item", $inner_type, true))), false),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(std::mem::take(&mut $obj_ids))) as Arc<dyn Array>,
                    Arc::new($list_builder.finish()) as Arc<dyn Array>,
                ],
            ).unwrap();
            $pool.write_batch($name, schema, &batch);
        }
    };
}

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
// Main entry point
// ---------------------------------------------------------------------------

pub fn dump_objects_to_parquet(hprof: &Hprof, flush_row_threshold: usize) {
    // Clean output directory so stale files from previous runs don't persist
    let _ = std::fs::remove_dir_all("parquet");
    std::fs::create_dir_all("parquet").unwrap();

    let index = HprofIndex::build(hprof);
    let mut pool = ParquetWriterPool::new();

    // Instance collection state
    let mut schemas: HashMap<Id, Schema> = HashMap::new();
    let mut class_field_columns: HashMap<Id, Vec<Vec<ExtendedFieldValue>>> = HashMap::new();
    let mut class_obj_ids: HashMap<Id, Vec<u64>> = HashMap::new();

    // Primitive array collection state
    let mut bool_obj_ids: Vec<u64> = Vec::new();
    let mut bool_values = ListBuilder::new(BooleanBuilder::new());
    let mut byte_obj_ids: Vec<u64> = Vec::new();
    let mut byte_values = ListBuilder::new(Int8Builder::new());
    let mut char_obj_ids: Vec<u64> = Vec::new();
    let mut char_values = ListBuilder::new(UInt16Builder::new());
    let mut short_obj_ids: Vec<u64> = Vec::new();
    let mut short_values = ListBuilder::new(Int16Builder::new());
    let mut int_obj_ids: Vec<u64> = Vec::new();
    let mut int_values = ListBuilder::new(Int32Builder::new());
    let mut long_obj_ids: Vec<u64> = Vec::new();
    let mut long_values = ListBuilder::new(Int64Builder::new());
    let mut float_obj_ids: Vec<u64> = Vec::new();
    let mut float_values = ListBuilder::new(Float32Builder::new());
    let mut double_obj_ids: Vec<u64> = Vec::new();
    let mut double_values = ListBuilder::new(Float64Builder::new());

    // Object array collection state
    let mut oa_obj_ids: Vec<u64> = Vec::new();
    let mut oa_class_names: Vec<String> = Vec::new();
    let mut oa_elements = ListBuilder::new(UInt64Builder::new());

    // GC root collection state
    let mut gc_root_types: Vec<String> = Vec::new();
    let mut gc_root_obj_ids: Vec<u64> = Vec::new();
    let mut gc_root_thread_serials: Vec<Option<u32>> = Vec::new();
    let mut gc_root_frame_indexes: Vec<Option<u32>> = Vec::new();

    let mut rows_since_flush: usize = 0;

    // Single pass: collect instances, primitive arrays, and object arrays
    for r in hprof.records_iter().map(|r| r.unwrap()) {
        match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    match s {
                        SubRecord::Instance(instance) => {
                            if index.classes.get(&instance.class_obj_id()).is_none() {
                                panic!(
                                    "Could not find class {} for instance {}",
                                    instance.class_obj_id(),
                                    instance.obj_id()
                                );
                            }

                            let field_descriptors = index.class_instance_field_descriptors
                                .get(&instance.class_obj_id())
                                .expect("Should have all classes available");

                            if !schemas.contains_key(&instance.class_obj_id()) {
                                schemas.insert(
                                    instance.class_obj_id(),
                                    generate_schema_from_type(
                                        &hprof,
                                        &field_descriptors,
                                        instance.fields(),
                                        &index.utf8,
                                        index.class_field_declaring_classes.get(&instance.class_obj_id()),
                                    ),
                                );
                            }

                            if !class_field_columns.contains_key(&instance.class_obj_id()) {
                                let columns: Vec<Vec<ExtendedFieldValue>> =
                                    (0..field_descriptors.len()).map(|_| Vec::new()).collect();
                                class_field_columns.insert(instance.class_obj_id(), columns);
                                class_obj_ids.insert(instance.class_obj_id(), Vec::new());
                            }

                            class_obj_ids.get_mut(&instance.class_obj_id()).unwrap()
                                .push(instance.obj_id().id());

                            let field_columns = class_field_columns.get_mut(&instance.class_obj_id()).unwrap();
                            add_instance_values(
                                &hprof,
                                field_columns,
                                instance.fields(),
                                &field_descriptors,
                                &index.obj_id_to_class_obj_id,
                                &index.prim_array_obj_id_to_type,
                            );

                            rows_since_flush += 1;
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            let obj_id = pa.obj_id().id();
                            match pa.primitive_type() {
                                PrimitiveArrayType::Boolean => {
                                    bool_obj_ids.push(obj_id);
                                    for v in pa.booleans().unwrap() {
                                        bool_values.values().append_value(v.unwrap());
                                    }
                                    bool_values.append(true);
                                }
                                PrimitiveArrayType::Byte => {
                                    byte_obj_ids.push(obj_id);
                                    for v in pa.bytes().unwrap() {
                                        byte_values.values().append_value(v.unwrap());
                                    }
                                    byte_values.append(true);
                                }
                                PrimitiveArrayType::Char => {
                                    char_obj_ids.push(obj_id);
                                    for v in pa.chars().unwrap() {
                                        char_values.values().append_value(v.unwrap() as u16);
                                    }
                                    char_values.append(true);
                                }
                                PrimitiveArrayType::Short => {
                                    short_obj_ids.push(obj_id);
                                    for v in pa.shorts().unwrap() {
                                        short_values.values().append_value(v.unwrap());
                                    }
                                    short_values.append(true);
                                }
                                PrimitiveArrayType::Int => {
                                    int_obj_ids.push(obj_id);
                                    for v in pa.ints().unwrap() {
                                        int_values.values().append_value(v.unwrap());
                                    }
                                    int_values.append(true);
                                }
                                PrimitiveArrayType::Long => {
                                    long_obj_ids.push(obj_id);
                                    for v in pa.longs().unwrap() {
                                        long_values.values().append_value(v.unwrap());
                                    }
                                    long_values.append(true);
                                }
                                PrimitiveArrayType::Float => {
                                    float_obj_ids.push(obj_id);
                                    for v in pa.floats().unwrap() {
                                        float_values.values().append_value(v.unwrap());
                                    }
                                    float_values.append(true);
                                }
                                PrimitiveArrayType::Double => {
                                    double_obj_ids.push(obj_id);
                                    for v in pa.doubles().unwrap() {
                                        double_values.values().append_value(v.unwrap());
                                    }
                                    double_values.append(true);
                                }
                            }

                            rows_since_flush += 1;
                        }
                        SubRecord::ObjectArray(oa) => {
                            oa_obj_ids.push(oa.obj_id().id());

                            let class_name = index.classes.get(&oa.array_class_obj_id())
                                .map(|c| c.name.to_string())
                                .unwrap_or_else(|| "(unresolved)".to_string());
                            oa_class_names.push(class_name);

                            for elem in oa.elements(hprof.header().id_size()) {
                                match elem.unwrap() {
                                    Some(id) => oa_elements.values().append_value(id.id()),
                                    None => oa_elements.values().append_value(0),
                                }
                            }
                            oa_elements.append(true);

                            rows_since_flush += 1;
                        }
                        SubRecord::GcRootUnknown(r) => {
                            gc_root_types.push("Unknown".into());
                            gc_root_obj_ids.push(r.obj_id().id());
                            gc_root_thread_serials.push(None);
                            gc_root_frame_indexes.push(None);
                        }
                        SubRecord::GcRootThreadObj(r) => {
                            gc_root_types.push("ThreadObj".into());
                            gc_root_obj_ids.push(r.thread_obj_id().map(|id| id.id()).unwrap_or(0));
                            gc_root_thread_serials.push(Some(r.thread_serial().num()));
                            gc_root_frame_indexes.push(None);
                        }
                        SubRecord::GcRootJniGlobal(r) => {
                            gc_root_types.push("JniGlobal".into());
                            gc_root_obj_ids.push(r.obj_id().id());
                            gc_root_thread_serials.push(None);
                            gc_root_frame_indexes.push(None);
                        }
                        SubRecord::GcRootJniLocalRef(r) => {
                            gc_root_types.push("JniLocal".into());
                            gc_root_obj_ids.push(r.obj_id().id());
                            gc_root_thread_serials.push(Some(r.thread_serial().num()));
                            gc_root_frame_indexes.push(r.frame_index());
                        }
                        SubRecord::GcRootJavaStackFrame(r) => {
                            gc_root_types.push("JavaStackFrame".into());
                            gc_root_obj_ids.push(r.obj_id().id());
                            gc_root_thread_serials.push(Some(r.thread_serial().num()));
                            gc_root_frame_indexes.push(r.frame_index());
                        }
                        SubRecord::GcRootNativeStack(r) => {
                            gc_root_types.push("NativeStack".into());
                            gc_root_obj_ids.push(r.obj_id().id());
                            gc_root_thread_serials.push(Some(r.thread_serial().num()));
                            gc_root_frame_indexes.push(None);
                        }
                        SubRecord::GcRootSystemClass(r) => {
                            gc_root_types.push("SystemClass".into());
                            gc_root_obj_ids.push(r.obj_id().id());
                            gc_root_thread_serials.push(None);
                            gc_root_frame_indexes.push(None);
                        }
                        SubRecord::GcRootThreadBlock(r) => {
                            gc_root_types.push("ThreadBlock".into());
                            gc_root_obj_ids.push(r.obj_id().id());
                            gc_root_thread_serials.push(Some(r.thread_serial().num()));
                            gc_root_frame_indexes.push(None);
                        }
                        SubRecord::GcRootBusyMonitor(r) => {
                            gc_root_types.push("BusyMonitor".into());
                            gc_root_obj_ids.push(r.obj_id().id());
                            gc_root_thread_serials.push(None);
                            gc_root_frame_indexes.push(None);
                        }
                        _ => {}
                    }

                    // Periodic flush to keep memory bounded
                    if rows_since_flush >= flush_row_threshold {
                        flush_instance_buffers(
                            &mut pool, &schemas, &mut class_field_columns,
                            &mut class_obj_ids, &index,
                        );
                        flush_prim_array_buffer!(pool, "_primitive_arrays_boolean", bool_obj_ids, bool_values, DataType::Boolean);
                        flush_prim_array_buffer!(pool, "_primitive_arrays_byte", byte_obj_ids, byte_values, DataType::Int8);
                        flush_prim_array_buffer!(pool, "_primitive_arrays_char", char_obj_ids, char_values, DataType::UInt16);
                        flush_prim_array_buffer!(pool, "_primitive_arrays_short", short_obj_ids, short_values, DataType::Int16);
                        flush_prim_array_buffer!(pool, "_primitive_arrays_int", int_obj_ids, int_values, DataType::Int32);
                        flush_prim_array_buffer!(pool, "_primitive_arrays_long", long_obj_ids, long_values, DataType::Int64);
                        flush_prim_array_buffer!(pool, "_primitive_arrays_float", float_obj_ids, float_values, DataType::Float32);
                        flush_prim_array_buffer!(pool, "_primitive_arrays_double", double_obj_ids, double_values, DataType::Float64);
                        flush_object_array_buffer(
                            &mut pool, &mut oa_obj_ids, &mut oa_class_names, &mut oa_elements,
                        );
                        rows_since_flush = 0;
                    }
                }
            }
            _ => {}
        }
    }

    // Flush remaining buffered data
    flush_instance_buffers(
        &mut pool, &schemas, &mut class_field_columns,
        &mut class_obj_ids, &index,
    );
    flush_prim_array_buffer!(pool, "_primitive_arrays_boolean", bool_obj_ids, bool_values, DataType::Boolean);
    flush_prim_array_buffer!(pool, "_primitive_arrays_byte", byte_obj_ids, byte_values, DataType::Int8);
    flush_prim_array_buffer!(pool, "_primitive_arrays_char", char_obj_ids, char_values, DataType::UInt16);
    flush_prim_array_buffer!(pool, "_primitive_arrays_short", short_obj_ids, short_values, DataType::Int16);
    flush_prim_array_buffer!(pool, "_primitive_arrays_int", int_obj_ids, int_values, DataType::Int32);
    flush_prim_array_buffer!(pool, "_primitive_arrays_long", long_obj_ids, long_values, DataType::Int64);
    flush_prim_array_buffer!(pool, "_primitive_arrays_float", float_obj_ids, float_values, DataType::Float32);
    flush_prim_array_buffer!(pool, "_primitive_arrays_double", double_obj_ids, double_values, DataType::Float64);
    flush_object_array_buffer(
        &mut pool, &mut oa_obj_ids, &mut oa_class_names, &mut oa_elements,
    );

    // Write static fields (small, one-shot)
    write_static_fields(&mut pool, &index);

    // Write GC roots (small, one-shot)
    if !gc_root_obj_ids.is_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("root_type", DataType::Utf8, false),
            Field::new("obj_id", DataType::UInt64, false),
            Field::new("thread_serial", DataType::UInt32, true),
            Field::new("frame_index", DataType::UInt32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(gc_root_types)) as Arc<dyn Array>,
                Arc::new(UInt64Array::from(gc_root_obj_ids)) as Arc<dyn Array>,
                Arc::new(UInt32Array::from(gc_root_thread_serials)) as Arc<dyn Array>,
                Arc::new(UInt32Array::from(gc_root_frame_indexes)) as Arc<dyn Array>,
            ],
        ).unwrap();

        pool.write_batch("_gc_roots", schema, &batch);
    }

    // Close all writers in parallel (writes parquet footers)
    pool.close_all();
}
