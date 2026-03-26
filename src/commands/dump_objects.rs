// SPDX-License-Identifier: MIT
// Copyright (c) 2026 Zac Policzer

use std::collections::HashMap;
use dashmap::DashMap;
use jvm_hprof::{EzClass, Hprof, Id, RecordTag};
use jvm_hprof::heap_dump::{FieldType, FieldValue, PrimitiveArrayType, SubRecord};
use crate::hprof_index::HprofIndex;

pub fn dump_objects(hprof: &Hprof) {
    let index = HprofIndex::build(hprof);

    let missing_utf8 = "(missing utf8)";

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
                            let mc = match index.classes.get(&class.obj_id()) {
                                None => panic!("Could not find class {}", class.obj_id()),
                                Some(c) => c,
                            };

                            println!("\nid {}: class {}", class.obj_id(), mc.name);
                            for sf in &mc.static_fields {
                                let field_name =
                                    index.utf8.get(&sf.name_id()).unwrap_or_else(|| &missing_utf8);

                                print_field_val(
                                    &sf.value(),
                                    field_name,
                                    sf.field_type(),
                                    &index.obj_id_to_class_obj_id,
                                    &index.classes,
                                    &index.prim_array_obj_id_to_type,
                                );
                            }
                        }
                        SubRecord::Instance(instance) => {
                            let mc = match index.classes.get(&instance.class_obj_id()) {
                                None => panic!(
                                    "Could not find class {} for instance {}",
                                    instance.class_obj_id(),
                                    instance.obj_id()
                                ),
                                Some(c) => c,
                            };

                            println!("\nid {}: {}", instance.obj_id(), mc.name);

                            let field_descriptors = index.class_instance_field_descriptors
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
                                    index.utf8.get(&fd.name_id()).unwrap_or_else(|| &missing_utf8);

                                print_field_val(
                                    &field_val,
                                    field_name,
                                    fd.field_type(),
                                    &index.obj_id_to_class_obj_id,
                                    &index.classes,
                                    &index.prim_array_obj_id_to_type,
                                );
                            }
                        }
                        SubRecord::ObjectArray(oa) => {
                            let mc = match index.classes.get(&oa.array_class_obj_id()) {
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
                                        let element_class_name = index.obj_id_to_class_obj_id
                                            .get(&id)
                                            .and_then(|class_id| index.classes.get(&*class_id))
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
    obj_id_to_class_obj_id: &DashMap<Id, Id>,
    classes: &HashMap<Id, EzClass>,
    prim_array_obj_id_to_type: &DashMap<Id, PrimitiveArrayType>,
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
                            .get(&*class_obj_id)
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
