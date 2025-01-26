mod util;
use clap;
use collections::HashMap;
use std::{fs, collections};
use std::sync::Arc;
use arrow_array::builder::{BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, ListBuilder, UInt16Builder};
use arrow_array::{Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StructArray, UInt16Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use jvm_hprof::{*};
use jvm_hprof::heap_dump::{FieldDescriptor, FieldType, FieldValue, PrimitiveArrayType, SubRecord};
use crate::util::{generate_schema_from_type, write_to_parquet};

fn main() {
    let app = clap::Command::new("Analyze Hprof")
        .arg(
            clap::Arg::new("file")
                .short('f')
                .long("file")
                .required(true)
                .value_name("FILE")
                .help("Heap dump file to read"),
        )
        .subcommand(clap::Command::new("dump-objects")
            .about("Display Object (and other associated) heap dump subrecords to stdout"))
        .subcommand(clap::Command::new("count-records")
            .about("Display the number of each of the top level hprof record types"))
        .subcommand(clap::Command::new("dump-objects-to-parquet")
            .about("Parses and dumps objects in the heap dump to parquet files")
        );
    let matches = app.get_matches();

    let file_path = matches.get_one::<String>("file").expect("file must be specified");

    let file = fs::File::open(file_path).unwrap_or_else(|_| panic!("Could not open file at path: {}", file_path));

    let memmap = unsafe { memmap::MmapOptions::new().map(&file) }.unwrap();

    let hprof: Hprof = parse_hprof(&memmap[..]).unwrap();

    matches.subcommand().map(|(subcommand, _)| match subcommand {
        "dump-objects" => dump_objects(&hprof),
        "count-records" => count_records(&hprof),
        "dump-objects-to-parquet" => dump_objects_to_parquet(&hprof),
        _ => panic!("Unknown subcommand"),
    });
}

macro_rules! process_primitive_array {
    ($pa:expr, $getter:ident, $ids:expr, $vals:expr) => {
        {
            let mut contains_val = false;
            $pa.$getter()
                .unwrap()
                .map(|r| r.unwrap())
                .for_each(|e| {
                    $vals.values().append_value(e);
                    contains_val = true;
                });
            if contains_val {
                $vals.append(true);
                $ids.push($pa.obj_id().id() as u64);
            }
        }
    };
}

fn count_records(hprof: &Hprof) {
    // start with zero counts for all types
    let mut counts = RecordTag::iter()
        .map(|r| (r, 0_u64))
        .collect::<HashMap<RecordTag, u64>>();

    // overwrite zeros with real counts for each record that exists in the hprof
    hprof
        .records_iter()
        .map(|r| r. unwrap().tag())
        .for_each(|tag| {
            counts.entry(tag).and_modify(|c| *c += 1).or_insert(1);
        });

    let mut counts: Vec<(RecordTag, u64)> = counts
        .into_iter()
        .collect::<Vec<(jvm_hprof::RecordTag, u64)>>();

    // highest count on top
    counts.sort_unstable_by_key(|&(_, count)| count);
    counts.reverse();

    for (tag, count) in counts {
        println!("{:?}: {}", tag, count);
    }
}

const MISSING_UTF8: &str = "(missing utf8)";

#[derive(Debug)]
enum ExtendedFieldValue {
    FieldValue(FieldValue),
    ObjectReference(Id),
    PrimitiveArrayReference(Id),
}

fn add_instance_values(
    hprof: &Hprof,
    field_val_map: &mut collections::HashMap<String, Vec<ExtendedFieldValue>>,
    field_descriptors: &Vec<FieldDescriptor>,
    mut field_val_input: &[u8],
    utf8: &collections::HashMap<Id, &str>,
    obj_id_to_class_obj_id: &collections::HashMap<Id, Id>,
    classes: &collections::HashMap<Id, EzClass>,
    prim_array_obj_id_to_type: &collections::HashMap<Id, PrimitiveArrayType>,
)
{
    for fd in field_descriptors.iter() {
        let (input, field_val) = fd
            .field_type()
            .parse_value(field_val_input, hprof.header().id_size())
            .unwrap();
        field_val_input = input;
        let field_name = utf8.get(&fd.name_id()).unwrap_or_else(|| &MISSING_UTF8).to_string();
        if !field_val_map.contains_key(&field_name) {
            field_val_map.insert(field_name.clone(), vec![]);
        }
        let field_val_vec = field_val_map.get_mut(&field_name).unwrap();
        // println!("field_name: {}", field_name);
        match field_val {
            FieldValue::ObjectId(Some(field_ref_id)) => {
                // println!("field_name: {}, contains: {}", field_name, obj_id_to_class_obj_id.contains_key(&field_ref_id));
                obj_id_to_class_obj_id
                    .get(&field_ref_id)
                    .map(|class_obj_id: &Id| {
                        field_val_vec.push(ExtendedFieldValue::ObjectReference(field_ref_id));
                        // case where the field_ref_id is in the obj_id_to_class_object
                        // (essentially this is a reference to a single instance)

                        // if !id_map.contains_key(&fd.name_id()) {
                        //     id_map.insert(fd.name_id(), vec![]);
                        // }
                        // id_map.get_mut(&fd.name_id()).unwrap().push(field_ref_id);
                        // println!("{:?}", input);
                        // println!("ObjectReference {} {}: field_ref_id: {}, field_ref_type: {}", field_name, &fd.name_id(), field_ref_id, classes.get(obj_id_to_class_obj_id.get(&field_ref_id).unwrap()).unwrap().name);
                        // println!("ObjectReference class_obj_id: {}, class_obj_type: {}", class_obj_id, classes.get(class_obj_id).unwrap().name);
                        // field_val_map.push(Field::new(&fd.name_id(), DataType::Struct(
                        //     Fields::from(vec![
                        //         Field::new("id", DataType::UInt64, false), 
                        //         Field::new("type", DataType::Utf8, false)])
                        // ), false));
                    })
                    .or_else(|| {
                        // TODO:
                        // Case where this is a primitive type array
                        prim_array_obj_id_to_type
                            .get(&field_ref_id)
                            .map(|prim_type| {
                                field_val_vec.push(ExtendedFieldValue::PrimitiveArrayReference(field_ref_id));
                            });
                        None
                    })
                    .or_else(|| {

                        classes.get(&field_ref_id).map(|dest_class| {
                            // This is a class reference case, we can probably ignore this, though clazz references can be legit, let's drop for MVP
                        })
                    })
                    .unwrap_or_else(|| {
                        // not found, which.... we should log, but we'll avoid it for now
                    });
            }
            FieldValue::ObjectId(None) => {
                field_val_vec.push(ExtendedFieldValue::ObjectReference(Id::from(0)));
            }
            FieldValue::Boolean(v) => {
                field_val_vec.push(ExtendedFieldValue::FieldValue(field_val));
            }
            FieldValue::Char(v) => {
                field_val_vec.push(ExtendedFieldValue::FieldValue(field_val));
            }
            FieldValue::Float(v) => {
                field_val_vec.push(ExtendedFieldValue::FieldValue(field_val));
            }
            FieldValue::Double(v) => {
                field_val_vec.push(ExtendedFieldValue::FieldValue(field_val));
            }
            FieldValue::Byte(v) => {
                field_val_vec.push(ExtendedFieldValue::FieldValue(field_val));
            }
            FieldValue::Short(v) => {
                field_val_vec.push(ExtendedFieldValue::FieldValue(field_val));
            }
            FieldValue::Int(v) => {
                field_val_vec.push(ExtendedFieldValue::FieldValue(field_val));
            }
            FieldValue::Long(v) => {
                field_val_vec.push(ExtendedFieldValue::FieldValue(field_val));
            }
        }
    }
}

pub fn dump_objects_to_parquet(hprof: &Hprof) {
    // class obj id -> LoadClass
    let mut load_classes = collections::HashMap::new();
    // name id -> String
    let mut utf8 = collections::HashMap::new();
    let mut utf_8 = collections::HashMap::new();

    let mut classes: collections::HashMap<Id, EzClass> = collections::HashMap::new();
    let mut schemas: collections::HashMap<Id, Schema> = collections::HashMap::new();
    // instance obj id to class obj id
    // TODO if this gets big, could use lmdb or similar to get it off-heap
    let mut obj_id_to_class_obj_id: collections::HashMap<Id, Id> = collections::HashMap::new();
    let mut prim_array_obj_id_to_type = collections::HashMap::new();

    // class_obj_id (SIT) -> &fd.name_id() (BatchProcessor) -> vec_values (instance1, instance2)
    let mut class_field_val_map: collections::HashMap<Id, collections::HashMap<String, Vec<ExtendedFieldValue>>> = collections::HashMap::new();
    // class_obj_id -> &fd.name_id() -> other_class_obj_id
    let mut class_id_map: collections::HashMap<Id, collections::HashMap<Id, Vec<Id>>> = collections::HashMap::new();

    // build obj -> class and class id -> class metadata maps
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .for_each(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();
                    match s {
                        SubRecord::Class(c) => {
                            classes
                                .insert(c.obj_id(), EzClass::from_class(&c, &load_classes, &utf8));
                        }
                        SubRecord::Instance(instance) => {
                            obj_id_to_class_obj_id
                                .insert(instance.obj_id(), instance.class_obj_id());
                        }
                        SubRecord::ObjectArray(obj_array) => {
                            obj_id_to_class_obj_id
                                .insert(obj_array.obj_id(), obj_array.array_class_obj_id());
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            prim_array_obj_id_to_type.insert(pa.obj_id(), pa.primitive_type());
                        }
                        _ => {}
                    };
                }
            }
            RecordTag::Utf8 => {
                let u = r.as_utf_8().unwrap().unwrap();
                let s = u.text_as_str().unwrap_or("(invalid UTF-8)");
                utf8.insert(u.name_id(), s);
                utf_8.insert(s, u.name_id());
            }
            RecordTag::LoadClass => {
                let lc = r.as_load_class().unwrap().unwrap();
                load_classes.insert(lc.class_obj_id(), lc);
            }
            _ => {}
        });

    let class_instance_field_descriptors = build_type_hierarchy_field_descriptors(&classes);

    let mut bool_ids = vec![];
    let mut bool_vals = ListBuilder::new(BooleanBuilder::new());
    let mut byte_ids = vec![];
    let mut byte_vals = ListBuilder::new(Int8Builder::new());
    let mut short_ids = vec![];
    let mut short_vals = ListBuilder::new(Int16Builder::new());
    let mut char_ids = vec![];
    let mut char_vals = ListBuilder::new(UInt16Builder::new());
    let mut int_ids = vec![];
    let mut int_vals = ListBuilder::new(Int32Builder::new());
    let mut long_ids = vec![];
    let mut long_vals = ListBuilder::new(Int64Builder::new());
    let mut float_ids = vec![];
    let mut float_vals = ListBuilder::new(Float32Builder::new());
    let mut double_ids = vec![];
    let mut double_vals = ListBuilder::new(Float64Builder::new());
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .for_each(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    match s {
                        SubRecord::Class(class) => {
                            // let mc = match classes.get(&class.obj_id()) {
                            //     None => panic!("Could not find class {}", class.obj_id()),
                            //     Some(c) => c,
                            // };
                        }
                        SubRecord::Instance(instance) => {
                            let mc = match classes.get(&instance.class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for instance {}",
                                    instance.class_obj_id(),
                                    instance.obj_id()
                                ),
                                Some(c) => c,
                            };

                            let field_descriptors = class_instance_field_descriptors
                                .get(&instance.class_obj_id())
                                .expect("Should have all classes available");

                            if !schemas.contains_key(&instance.class_obj_id()) {
                                schemas.insert(
                                    instance.class_obj_id(),
                                    generate_schema_from_type(
                                        &hprof,
                                        &field_descriptors,
                                        instance.fields(),
                                        &utf8,
                                        &obj_id_to_class_obj_id,
                                        &classes,
                                        &prim_array_obj_id_to_type,
                                    ),
                                );
                            }

                            if !class_field_val_map.contains_key(&instance.class_obj_id()) {
                                class_field_val_map.insert(instance.class_obj_id(), collections::HashMap::new());
                            }

                            let mut field_val_map = class_field_val_map.get_mut(&instance.class_obj_id()).unwrap();
                            add_instance_values(
                                &hprof,
                                field_val_map,
                                &field_descriptors,
                                instance.fields(),
                                &utf8,
                                &obj_id_to_class_obj_id,
                                &classes,
                                &prim_array_obj_id_to_type);
                        }
                        SubRecord::ObjectArray(oa) => {
                            // let mc = match classes.get(&oa.array_class_obj_id()) {
                            //     None => panic!(
                            //         "Could not find class {} for instance {}",
                            //         oa.array_class_obj_id(),
                            //         oa.obj_id()
                            //     ),
                            //     Some(c) => c,
                            // };

                            // println!("\nid {}: {} = [", oa.obj_id(), mc.name);

                            // for pr in oa.elements(hprof.header().id_size()) {
                            //     match pr.unwrap() {
                            //         Some(id) => {
                            //             let element_class_name = obj_id_to_class_obj_id
                            //                 .get(&id)
                            //                 .and_then(|class_id| classes.get(class_id))
                            //                 .map(|c| c.name)
                            //                 .unwrap_or_else(|| "(could not resolve class)");

                            //             println!("  - id {}: {}", id, element_class_name);
                            //         }
                            //         None => {
                            //             println!("  - null");
                            //         }
                            //     }
                            // }

                            // println!("]");
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            match pa.primitive_type() {
                                PrimitiveArrayType::Boolean => process_primitive_array!(pa, booleans, bool_ids, bool_vals),
                                PrimitiveArrayType::Char => process_primitive_array!(pa, chars, char_ids, char_vals),
                                PrimitiveArrayType::Float => process_primitive_array!(pa, floats, float_ids, float_vals),
                                PrimitiveArrayType::Double => process_primitive_array!(pa, doubles, double_ids, double_vals),
                                PrimitiveArrayType::Byte => process_primitive_array!(pa, bytes, byte_ids, byte_vals),
                                PrimitiveArrayType::Short => process_primitive_array!(pa, shorts, short_ids, short_vals),
                                PrimitiveArrayType::Int => process_primitive_array!(pa, ints, int_ids, int_vals),
                                PrimitiveArrayType::Long => process_primitive_array!(pa, longs, long_ids, long_vals),
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        });

    for (class_id, schema) in schemas.iter() {
        let field_val_map = class_field_val_map.get(class_id).unwrap();
        // let schema = schemas.get(class_id).unwrap();
        let mut columns = vec![];
        schema.fields().iter().for_each(|f| {
            let field_name = f.name();
            let field_id = utf_8.get(field_name.as_str()).unwrap();
            // println!("Field: {} FieldID: {}", field_name, field_id);

            if field_val_map.contains_key(field_name) {
                let field_val_vec = field_val_map.get(field_name).unwrap();
                match field_val_vec[0] {
                    ExtendedFieldValue::ObjectReference(_) => {
                        let id_vec = field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::ObjectReference(val) => val.id(),
                            _ => 0, // handle other types accordingly
                        }).collect::<Vec<u64>>();
                        let type_vec = field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::ObjectReference(val) => {
                                if val.id() == 0 {
                                    return "null".to_string();
                                }
                                classes.get(obj_id_to_class_obj_id.get(val).unwrap()).unwrap().name.to_string()
                            },
                            _ => "null".to_string(), // handle other types accordingly
                        }).collect::<Vec<String>>();
                        // println!("{} {} id_vec: {:?}", id_vec.len(), field_val_vec.len(), id_vec);
                        // println!("{} {} type_vec: {:?}", type_vec.len(), field_name, type_vec);
                        let id_array: Arc<dyn Array> = Arc::new(UInt64Array::from(id_vec));
                        let type_array: Arc<dyn Array> = Arc::new(arrow_array::StringArray::from(type_vec));
                        let struct_array = StructArray::from(vec![
                            (Arc::new(Field::new("id", DataType::UInt64, false)), id_array),
                            (Arc::new(Field::new("type", DataType::Utf8, false)), type_array),
                        ]);
                        let array: Arc<dyn Array> = Arc::new(struct_array);
                        columns.push(array);
                    }
                    ExtendedFieldValue::PrimitiveArrayReference(_) => {
                        let id_vec = field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::PrimitiveArrayReference(val) => val.id(),
                            _ => 0, // handle other types accordingly
                        }).collect::<Vec<u64>>();
                        let type_vec = field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::PrimitiveArrayReference(val) => match prim_array_obj_id_to_type.get(&val) {
                                Some(PrimitiveArrayType::Boolean) => "boolean".to_string(),
                                Some(PrimitiveArrayType::Char) => "char".to_string(),
                                Some(PrimitiveArrayType::Float) => "float".to_string(),
                                Some(PrimitiveArrayType::Double) => "double".to_string(),
                                Some(PrimitiveArrayType::Byte) => "byte".to_string(),
                                Some(PrimitiveArrayType::Short) => "short".to_string(),
                                Some(PrimitiveArrayType::Int) => "int".to_string(),
                                Some(PrimitiveArrayType::Long) => "long".to_string(),
                                _ => "null".to_string(),
                            },
                            _ => "null".to_string(), // handle other types accordingly
                        }).collect::<Vec<String>>();
                        // println!("{} prim id_vec: {:?}", id_vec.len(), id_vec);
                        // println!("{} prim type_vec: {:?}", type_vec.len(), type_vec);
                        let id_array: Arc<dyn Array> = Arc::new(UInt64Array::from(id_vec));
                        let type_array: Arc<dyn Array> = Arc::new(arrow_array::StringArray::from(type_vec));
                        let struct_array = StructArray::from(vec![
                            (Arc::new(Field::new("id", DataType::UInt64, false)), id_array),
                            (Arc::new(Field::new("type", DataType::Utf8, false)), type_array),
                        ]);
                        let array: Arc<dyn Array> = Arc::new(struct_array);
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::ObjectId(_)) => {
                        // println!("Field: {} FieldID: {}", field_name, field_id);
                        let array: Arc<dyn Array> = Arc::new(UInt64Array::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::ObjectId(val)) => val.unwrap().id(),
                            _ => 0, // handle other types accordingly
                        }).collect::<Vec<u64>>()));
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::Int(_)) => {
                        let array: Arc<dyn Array> = Arc::new(Int32Array::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::Int(val)) => *val,
                            _ => 0, // handle other types accordingly
                        }).collect::<Vec<i32>>()));
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::Long(_)) => {
                        let array: Arc<dyn Array> = Arc::new(Int64Array::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::Long(val)) => *val,
                            _ => 0, // handle other types accordingly
                        }).collect::<Vec<i64>>()));
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::Boolean(_)) => {
                        let array: Arc<dyn Array> = Arc::new(BooleanArray::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::Boolean(val)) => *val,
                            _ => false, // handle other types accordingly
                        }).collect::<Vec<bool>>()));
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::Char(_)) => {
                        let array: Arc<dyn Array> = Arc::new(UInt16Array::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::Char(val)) => *val as u16,
                            _ => 0, // handle other types accordingly
                        }).collect::<Vec<u16>>()));
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::Float(_)) => {
                        let array: Arc<dyn Array> = Arc::new(Float32Array::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::Float(val)) => *val,
                            _ => 0.0, // handle other types accordingly
                        }).collect::<Vec<f32>>()));
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::Double(_)) => {
                        let array: Arc<dyn Array> = Arc::new(Float64Array::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::Double(val)) => *val,
                            _ => 0.0, // handle other types accordingly
                        }).collect::<Vec<f64>>()));
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::Byte(_)) => {
                        let array: Arc<dyn Array> = Arc::new(Int8Array::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::Byte(val)) => *val,
                            _ => 0, // handle other types accordingly
                        }).collect::<Vec<i8>>()));
                        columns.push(array);
                    }
                    ExtendedFieldValue::FieldValue(FieldValue::Short(_)) => {
                        let array: Arc<dyn Array> = Arc::new(Int16Array::from(field_val_vec.iter().map(|v| match v {
                            ExtendedFieldValue::FieldValue(FieldValue::Short(val)) => *val,
                            _ => 0, // handle other types accordingly
                        }).collect::<Vec<i16>>()));
                        columns.push(array);
                    }
                }
                // let array: Arc<dyn Array> = match f.data_type() {
                //     DataType::Int32 => Arc::new(Int32Array::from(field_val_vec.iter().map(|v| match v {
                //         FieldValue::Int(val) => *val,
                //         _ => 0, // handle other types accordingly
                //     }).collect::<Vec<i32>>())),
                //     DataType::Int64 => Arc::new(Int64Array::from(field_val_vec.iter().map(|v| match v {
                //         FieldValue::Long(val) => *val,
                //         _ => 0, // handle other types accordingly
                //     }).collect::<Vec<i64>>())),
                //     DataType::Boolean => Arc::new(BooleanArray::from(field_val_vec.iter().map(|v| match v {
                //         FieldValue::Boolean(val) => *val,
                //         _ => false, // handle other types accordingly
                //     }).collect::<Vec<bool>>())),
                //     DataType::UInt16 => Arc::new(UInt16Array::from(field_val_vec.iter().map(|v| match v {
                //         FieldValue::Char(val) => *val as u16,
                //         _ => 0, // handle other types accordingly
                //     }).collect::<Vec<u16>>())),
                //     DataType::Float32 => Arc::new(Float32Array::from(field_val_vec.iter().map(|v| match v {
                //         FieldValue::Float(val) => *val,
                //         _ => 0.0, // handle other types accordingly
                //     }).collect::<Vec<f32>>())),
                //     DataType::Float64 => Arc::new(Float64Array::from(field_val_vec.iter().map(|v| match v {
                //         FieldValue::Double(val) => *val,
                //         _ => 0.0, // handle other types accordingly
                //     }).collect::<Vec<f64>>())),
                //     DataType::Int8 => Arc::new(Int8Array::from(field_val_vec.iter().map(|v| match v {
                //         FieldValue::Byte(val) => *val,
                //         _ => 0, // handle other types accordingly
                //     }).collect::<Vec<i8>>())),
                //     DataType::Int16 => Arc::new(Int16Array::from(field_val_vec.iter().map(|v| match v {
                //         FieldValue::Short(val) => *val,
                //         _ => 0, // handle other types accordingly
                //     }).collect::<Vec<i16>>())),
                //     _ => Arc::new(NullArray::new(field_val_vec.len())), // handle other types accordingly
                // };
                // columns.push(array);
            }
        });

        if columns.len() == 0 {
            continue;
        }

        // println!("columns: {:?}", columns);
        // println!("printing columns for class: {}", classes.get(class_id).unwrap().name);
        // columns.iter().for_each(|col| {
        //     println!("Column length: {}", col.len());
        // });
        if columns.iter().any(|col| col.len() != columns[0].len()) {
            continue; // TODO: yeah let's just leave it as a TODO LOL
        }

        let batch: RecordBatch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            columns
        ).unwrap();

        write_to_parquet(classes.get(class_id).unwrap().name, batch);
    }

    // write_to_parquet("bools", generate_batch(bool_ids, bool_vals, DataType::Boolean));
    // write_to_parquet("bytes", generate_batch(byte_ids, byte_vals, DataType::Int8));
    // write_to_parquet("shorts", generate_batch(short_ids, short_vals, DataType::Int16));
    // write_to_parquet("chars", generate_batch(char_ids, char_vals, DataType::UInt16));
    // write_to_parquet("ints", generate_batch(int_ids, int_vals, DataType::Int32));
    // write_to_parquet("longs", generate_batch(long_ids, long_vals, DataType::Int64));
    // write_to_parquet("floats", generate_batch(float_ids, float_vals, DataType::Float32));
    // write_to_parquet("doubles", generate_batch(double_ids, double_vals, DataType::Float64));
}

pub fn dump_objects(hprof: &Hprof) {
    // class obj id -> LoadClass
    let mut load_classes = HashMap::new();
    // name id -> String
    let mut utf8 = HashMap::new();

    let mut classes: HashMap<Id, EzClass> = HashMap::new();
    // instance obj id to class obj id
    // TODO if this gets big, could use lmdb or similar to get it off-heap
    let mut obj_id_to_class_obj_id: HashMap<Id, Id> = HashMap::new();
    let mut prim_array_obj_id_to_type = HashMap::new();

    let missing_utf8 = "(missing utf8)";

    // build obj -> class and class id -> class metadata maps
    // TODO use index
    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .for_each(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();
                    match s {
                        SubRecord::Class(c) => {
                            classes
                                .insert(c.obj_id(), EzClass::from_class(&c, &load_classes, &utf8));
                        }
                        SubRecord::Instance(instance) => {
                            obj_id_to_class_obj_id
                                .insert(instance.obj_id(), instance.class_obj_id());
                        }
                        SubRecord::ObjectArray(obj_array) => {
                            obj_id_to_class_obj_id
                                .insert(obj_array.obj_id(), obj_array.array_class_obj_id());
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            prim_array_obj_id_to_type.insert(pa.obj_id(), pa.primitive_type());
                        }
                        _ => {}
                    };
                }
            }
            RecordTag::Utf8 => {
                let u = r.as_utf_8().unwrap().unwrap();
                utf8.insert(u.name_id(), u.text_as_str().unwrap_or("(invalid UTF-8)"));
            }
            RecordTag::LoadClass => {
                let lc = r.as_load_class().unwrap().unwrap();
                load_classes.insert(lc.class_obj_id(), lc);
            }
            _ => {}
        });

    let class_instance_field_descriptors = build_type_hierarchy_field_descriptors(&classes);

    hprof
        .records_iter()
        .map(|r| r.unwrap())
        .for_each(|r| match r.tag() {
            RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                let segment = r.as_heap_dump_segment().unwrap().unwrap();
                for p in segment.sub_records() {
                    let s = p.unwrap();

                    match s {
                        SubRecord::Class(class) => {
                            let mc = match classes.get(&class.obj_id()) {
                                None => panic!("Could not find class {}", class.obj_id()),
                                Some(c) => c,
                            };

                            println!("\nid {}: class {}", class.obj_id(), mc.name);
                            for sf in &mc.static_fields {
                                let field_name =
                                    utf8.get(&sf.name_id()).unwrap_or_else(|| &missing_utf8);

                                print_field_val(
                                    &sf.value(),
                                    field_name,
                                    sf.field_type(),
                                    &obj_id_to_class_obj_id,
                                    &classes,
                                    &prim_array_obj_id_to_type,
                                );
                            }
                        }
                        SubRecord::Instance(instance) => {
                            let mc = match classes.get(&instance.class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for instance {}",
                                    instance.class_obj_id(),
                                    instance.obj_id()
                                ),
                                Some(c) => c,
                            };

                            println!("\nid {}: {}", instance.obj_id(), mc.name);

                            let field_descriptors = class_instance_field_descriptors
                                .get(&instance.class_obj_id())
                                .expect("Should have all classes available");

                            let mut field_val_input: &[u8] = instance.fields();
                            for fd in field_descriptors.iter() {
                                let (input, field_val) = fd
                                    .field_type()
                                    .parse_value(field_val_input, hprof.header().id_size())
                                    .unwrap();
                                field_val_input = input;

                                let field_name =
                                    utf8.get(&fd.name_id()).unwrap_or_else(|| &missing_utf8);

                                print_field_val(
                                    &field_val,
                                    field_name,
                                    fd.field_type(),
                                    &obj_id_to_class_obj_id,
                                    &classes,
                                    &prim_array_obj_id_to_type,
                                );
                            }
                        }
                        SubRecord::ObjectArray(oa) => {
                            let mc = match classes.get(&oa.array_class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for instance {}",
                                    oa.array_class_obj_id(),
                                    oa.obj_id()
                                ),
                                Some(c) => c,
                            };

                            println!("\nid {}: {} = [", oa.obj_id(), mc.name);

                            for pr in oa.elements(hprof.header().id_size()) {
                                match pr.unwrap() {
                                    Some(id) => {
                                        let element_class_name = obj_id_to_class_obj_id
                                            .get(&id)
                                            .and_then(|class_id| classes.get(class_id))
                                            .map(|c| c.name)
                                            .unwrap_or_else(|| "(could not resolve class)");

                                        println!("  - id {}: {}", id, element_class_name);
                                    }
                                    None => {
                                        println!("  - null");
                                    }
                                }
                            }

                            println!("]");
                        }
                        SubRecord::PrimitiveArray(pa) => {
                            print!(
                                "\n{}: {}[] = [",
                                pa.obj_id(),
                                pa.primitive_type().java_type_name()
                            );

                            match pa.primitive_type() {
                                PrimitiveArrayType::Boolean => {
                                    pa.booleans()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Char => {
                                    pa.chars()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Float => {
                                    pa.floats()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Double => {
                                    pa.doubles()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Byte => {
                                    pa.bytes()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{:#X}, ", e));
                                }
                                PrimitiveArrayType::Short => {
                                    pa.shorts()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Int => {
                                    pa.ints()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                                PrimitiveArrayType::Long => {
                                    pa.longs()
                                        .unwrap()
                                        .map(|r| r.unwrap())
                                        .for_each(|e| print!("{}, ", e));
                                }
                            }

                            println!("]");
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        });
}

fn print_field_val(
    field_val: &FieldValue,
    field_name: &str,
    field_type: FieldType,
    obj_id_to_class_obj_id: &HashMap<Id, Id>,
    classes: &HashMap<Id, EzClass>,
    prim_array_obj_id_to_type: &HashMap<Id, PrimitiveArrayType>,
) {
    match field_val {
        FieldValue::ObjectId(Some(field_ref_id)) => {
            obj_id_to_class_obj_id
                .get(&field_ref_id)
                .map(|class_obj_id| {
                    println!(
                        "  - {} = id {} ({})",
                        field_name,
                        field_ref_id,
                        classes
                            .get(class_obj_id)
                            .map(|c| c.name)
                            .unwrap_or("(class not found)"),
                    );
                })
                .or_else(|| {
                    prim_array_obj_id_to_type
                        .get(&field_ref_id)
                        .map(|prim_type| {
                            println!(
                                "  - {} = id {} ({}[])",
                                field_name,
                                field_ref_id,
                                prim_type.java_type_name()
                            );
                        })
                })
                .or_else(|| {
                    classes.get(&field_ref_id).map(|dest_class| {
                        println!(
                            "  - {} = id {} (class {})",
                            field_name, field_ref_id, dest_class.name
                        );
                    })
                })
                .unwrap_or_else(|| {
                    println!(
                        "  - {} = id {} (type for obj id not found)",
                        field_name, field_ref_id
                    );
                });
        }
        FieldValue::ObjectId(None) => {
            println!("  - {} = null", field_name,);
        }
        FieldValue::Boolean(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Char(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Float(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Double(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Byte(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Short(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Int(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
        FieldValue::Long(v) => {
            println!(
                "  - {}: {} = {}",
                field_name,
                field_type.java_type_name(),
                v
            );
        }
    }
}