use std::collections::HashMap;
use dashmap::DashMap;
use jvm_hprof::{Hprof, Id, LoadClass, Record, RecordTag, EzClass, build_type_hierarchy_field_descriptors};
use jvm_hprof::heap_dump::{FieldDescriptor, PrimitiveArrayType, SubRecord};
use rayon::prelude::*;

pub(crate) struct HprofIndex<'a> {
    pub utf8: HashMap<Id, &'a str>,
    pub load_classes: HashMap<Id, LoadClass>,
    pub classes: HashMap<Id, EzClass<'a>>,
    pub obj_id_to_class_obj_id: DashMap<Id, Id>,
    pub prim_array_obj_id_to_type: DashMap<Id, PrimitiveArrayType>,
    pub class_instance_field_descriptors: HashMap<Id, Vec<FieldDescriptor>>,
    /// For each class, the declaring class name for each field descriptor (parallel to class_instance_field_descriptors)
    pub class_field_declaring_classes: HashMap<Id, Vec<&'a str>>,
}

impl<'a> HprofIndex<'a> {
    pub fn build(hprof: &'a Hprof<'a>) -> Self {
        let (index, _) = Self::build_with_segments(hprof);
        index
    }

    /// Build the index with parallel segment processing, also returning segment
    /// Record handles for later use in Pass 2.
    ///
    /// Phase 1a: Quick sequential scan — collect UTF8, LoadClass, and segment
    ///           Record handles. No sub-record parsing (fast).
    /// Phase 1b: Parallel sub-record processing via rayon — inserts directly
    ///           into shared DashMaps (no merge step needed).
    pub fn build_with_segments(hprof: &'a Hprof<'a>) -> (Self, Vec<Record<'a>>) {
        use std::time::Instant;

        // Phase 1a: Quick sequential scan of top-level records.
        let t0 = Instant::now();
        let mut utf8 = HashMap::new();
        let mut load_classes = HashMap::new();
        let mut segments: Vec<Record<'a>> = Vec::new();

        for r in hprof.records_iter().map(|r| r.unwrap()) {
            match r.tag() {
                RecordTag::Utf8 => {
                    let u = r.as_utf_8().unwrap().unwrap();
                    let s = u.text_as_str().unwrap_or("(invalid UTF-8)");
                    utf8.insert(u.name_id(), s);
                }
                RecordTag::LoadClass => {
                    let lc = r.as_load_class().unwrap().unwrap();
                    load_classes.insert(lc.class_obj_id(), lc);
                }
                RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                    segments.push(r);
                }
                _ => {}
            }
        }
        let phase1a_dur = t0.elapsed();
        println!("  Phase 1a (sequential scan): {:.1}s — {} utf8, {} load_classes, {} segments",
            phase1a_dur.as_secs_f64(), utf8.len(), load_classes.len(), segments.len());

        // Phase 1b: Parallel sub-record processing.
        // Shared concurrent maps — rayon threads insert directly, no merge needed.
        // Pre-size to reduce rehashing. Typical heap: ~200M instances, ~85M prim arrays.
        let t1 = Instant::now();
        let obj_id_to_class_obj_id: DashMap<Id, Id> = DashMap::with_capacity(200_000_000);
        let prim_array_obj_id_to_type: DashMap<Id, PrimitiveArrayType> = DashMap::with_capacity(100_000_000);
        // Classes are small (thousands, not millions), so thread-local + merge is fine.
        let classes_partial: std::sync::Mutex<HashMap<Id, EzClass<'a>>> = std::sync::Mutex::new(HashMap::new());

        segments.par_iter().for_each(|r| {
            let mut local_classes = HashMap::new();

            let segment = r.as_heap_dump_segment().unwrap().unwrap();
            for p in segment.sub_records() {
                let s = p.unwrap();
                match s {
                    SubRecord::Class(c) => {
                        local_classes.insert(
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
                }
            }

            if !local_classes.is_empty() {
                classes_partial.lock().unwrap().extend(local_classes);
            }
        });

        let classes = classes_partial.into_inner().unwrap();
        let phase1b_dur = t1.elapsed();
        println!("  Phase 1b (parallel index + DashMap): {:.1}s — {} classes, {} obj mappings, {} prim mappings",
            phase1b_dur.as_secs_f64(), classes.len(), obj_id_to_class_obj_id.len(), prim_array_obj_id_to_type.len());

        // Finalize: build field descriptors and declaring class maps
        let t2 = Instant::now();
        let class_instance_field_descriptors = build_type_hierarchy_field_descriptors(&classes);

        let mut class_field_declaring_classes: HashMap<Id, Vec<&str>> = HashMap::new();
        for (id, mc) in &classes {
            let mut declaring_classes = Vec::new();
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
        let finalize_dur = t2.elapsed();
        println!("  Phase 1c (finalize): {:.1}s", finalize_dur.as_secs_f64());

        let index = HprofIndex {
            utf8,
            load_classes,
            classes,
            obj_id_to_class_obj_id,
            prim_array_obj_id_to_type,
            class_instance_field_descriptors,
            class_field_declaring_classes,
        };
        (index, segments)
    }
}
