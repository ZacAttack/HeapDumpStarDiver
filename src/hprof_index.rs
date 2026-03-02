use std::collections::HashMap;
use jvm_hprof::{Hprof, Id, LoadClass, RecordTag, EzClass, build_type_hierarchy_field_descriptors};
use jvm_hprof::heap_dump::{FieldDescriptor, PrimitiveArrayType, SubRecord};

pub(crate) struct HprofIndex<'a> {
    pub utf8: HashMap<Id, &'a str>,
    pub load_classes: HashMap<Id, LoadClass>,
    pub classes: HashMap<Id, EzClass<'a>>,
    pub obj_id_to_class_obj_id: HashMap<Id, Id>,
    pub prim_array_obj_id_to_type: HashMap<Id, PrimitiveArrayType>,
    pub class_instance_field_descriptors: HashMap<Id, Vec<FieldDescriptor>>,
    /// For each class, the declaring class name for each field descriptor (parallel to class_instance_field_descriptors)
    pub class_field_declaring_classes: HashMap<Id, Vec<&'a str>>,
}

impl<'a> HprofIndex<'a> {
    pub fn build(hprof: &'a Hprof<'a>) -> Self {
        let mut utf8 = HashMap::new();
        let mut load_classes = HashMap::new();
        let mut classes: HashMap<Id, EzClass> = HashMap::new();
        let mut obj_id_to_class_obj_id: HashMap<Id, Id> = HashMap::new();
        let mut prim_array_obj_id_to_type = HashMap::new();

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
                                classes.insert(
                                    c.obj_id(),
                                    EzClass::from_class(&c, &load_classes, &utf8),
                                );
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
                                prim_array_obj_id_to_type
                                    .insert(pa.obj_id(), pa.primitive_type());
                            }
                            _ => {}
                        };
                    }
                }
                RecordTag::Utf8 => {
                    let u = r.as_utf_8().unwrap().unwrap();
                    let s = u.text_as_str().unwrap_or("(invalid UTF-8)");
                    utf8.insert(u.name_id(), s);
                }
                RecordTag::LoadClass => {
                    let lc = r.as_load_class().unwrap().unwrap();
                    load_classes.insert(lc.class_obj_id(), lc);
                }
                _ => {}
            });

        let class_instance_field_descriptors = build_type_hierarchy_field_descriptors(&classes);

        // Build parallel map of declaring class names for each field descriptor
        let mut class_field_declaring_classes: HashMap<Id, Vec<&str>> = HashMap::new();
        for (id, mc) in &classes {
            let mut declaring_classes = Vec::new();
            // Child fields come first (same order as build_type_hierarchy_field_descriptors)
            for _ in &mc.instance_field_descriptors {
                declaring_classes.push(mc.name);
            }
            let mut opt_scid = mc.super_class_obj_id;
            while let Some(scid) = opt_scid {
                let sc = classes.get(&scid).expect("Could not find superclass");
                for _ in &sc.instance_field_descriptors {
                    declaring_classes.push(sc.name);
                }
                opt_scid = sc.super_class_obj_id;
            }
            class_field_declaring_classes.insert(*id, declaring_classes);
        }

        HprofIndex {
            utf8,
            load_classes,
            classes,
            obj_id_to_class_obj_id,
            prim_array_obj_id_to_type,
            class_instance_field_descriptors,
            class_field_declaring_classes,
        }
    }
}
