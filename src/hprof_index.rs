use std::collections::HashMap;
use dashmap::DashMap;
use jvm_hprof::{Hprof, Id, LineNum, LoadClass, Record, RecordTag, EzClass, build_type_hierarchy_field_descriptors};
use jvm_hprof::heap_dump::{FieldDescriptor, PrimitiveArrayType, SubRecord};
use rayon::prelude::*;

/// Resolved stack frame with string names (not raw IDs).
/// Borrows from the HPROF's UTF8 string table to avoid allocations.
pub(crate) struct ResolvedStackFrame<'a> {
    pub frame_id: u64,
    pub method_name: &'a str,
    pub method_signature: &'a str,
    pub source_file: &'a str,
    pub class_name: &'a str,
    pub line_num: i32, // >0 = normal, -1 = unknown, -2 = compiled, -3 = native
}

/// Resolved stack trace with frame IDs expanded.
pub(crate) struct ResolvedStackTrace {
    pub stack_trace_serial: u32,
    pub thread_serial: u32,
    pub frame_ids: Vec<u64>,
}

pub(crate) struct HprofIndex<'a> {
    pub utf8: HashMap<Id, &'a str>,
    pub load_classes: HashMap<Id, LoadClass>,
    pub classes: HashMap<Id, EzClass<'a>>,
    pub obj_id_to_class_obj_id: DashMap<Id, Id>,
    pub prim_array_obj_id_to_type: DashMap<Id, PrimitiveArrayType>,
    pub class_instance_field_descriptors: HashMap<Id, Vec<FieldDescriptor>>,
    /// For each class, the declaring class name for each field descriptor (parallel to class_instance_field_descriptors)
    pub class_field_declaring_classes: HashMap<Id, Vec<&'a str>>,
    pub stack_frames: Vec<ResolvedStackFrame<'a>>,
    pub stack_traces: Vec<ResolvedStackTrace>,
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
        // Collect raw stack frames/traces — resolve names after utf8 + load_classes are complete
        let mut raw_stack_frames = Vec::new();
        let mut raw_stack_traces = Vec::new();
        // serial → class_obj_id mapping for resolving StackFrame.class_serial → class name
        let mut class_serial_to_obj_id: HashMap<u32, Id> = HashMap::new();

        for r in hprof.records_iter().map(|r| r.unwrap()) {
            match r.tag() {
                RecordTag::Utf8 => {
                    let u = r.as_utf_8().unwrap().unwrap();
                    let s = u.text_as_str().unwrap_or("(invalid UTF-8)");
                    utf8.insert(u.name_id(), s);
                }
                RecordTag::LoadClass => {
                    let lc = r.as_load_class().unwrap().unwrap();
                    class_serial_to_obj_id.insert(lc.class_serial().num(), lc.class_obj_id());
                    load_classes.insert(lc.class_obj_id(), lc);
                }
                RecordTag::StackFrame => {
                    let sf = r.as_stack_frame().unwrap().unwrap();
                    raw_stack_frames.push(sf);
                }
                RecordTag::StackTrace => {
                    let st = r.as_stack_trace().unwrap().unwrap();
                    raw_stack_traces.push(st);
                }
                RecordTag::HeapDump | RecordTag::HeapDumpSegment => {
                    segments.push(r);
                }
                _ => {}
            }
        }

        // Resolve stack frame names from utf8 + load_classes (parallel — can be 30K+ frames)
        let stack_frames: Vec<ResolvedStackFrame> = raw_stack_frames.par_iter().map(|sf| {
            let method_name = *utf8.get(&sf.method_name_id()).unwrap_or(&"(unknown)");
            let method_signature = *utf8.get(&sf.method_signature_id()).unwrap_or(&"(unknown)");
            let source_file = *utf8.get(&sf.source_file_name_id()).unwrap_or(&"(unknown)");
            let class_name = *class_serial_to_obj_id.get(&sf.class_serial().num())
                .and_then(|obj_id| load_classes.get(obj_id))
                .and_then(|lc| utf8.get(&lc.class_name_id()))
                .unwrap_or(&"(unknown)");
            let line_num = match sf.line_num() {
                LineNum::Normal(n) => n as i32,
                LineNum::Unknown => -1,
                LineNum::CompiledMethod => -2,
                LineNum::NativeMethod => -3,
            };
            ResolvedStackFrame {
                frame_id: sf.id().id(),
                method_name,
                method_signature,
                source_file,
                class_name,
                line_num,
            }
        }).collect();

        // Stack traces are typically few (hundreds) with trivial per-item work — sequential is faster
        let stack_traces: Vec<ResolvedStackTrace> = raw_stack_traces.iter().map(|st| {
            ResolvedStackTrace {
                stack_trace_serial: st.stack_trace_serial().num(),
                thread_serial: st.thread_serial().num(),
                frame_ids: st.frame_ids().map(|id| id.unwrap().id()).collect(),
            }
        }).collect();

        let phase1a_dur = t0.elapsed();
        println!("  Phase 1a (sequential scan): {:.1}s — {} utf8, {} load_classes, {} segments, {} stack_frames, {} stack_traces",
            phase1a_dur.as_secs_f64(), utf8.len(), load_classes.len(), segments.len(),
            stack_frames.len(), stack_traces.len());

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
            stack_frames,
            stack_traces,
        };
        (index, segments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_stack_frame_fields() {
        let frame = ResolvedStackFrame {
            frame_id: 12345,
            method_name: "processRecord",
            method_signature: "(Lcom/linkedin/venice/Record;)V",
            source_file: "Ingestion.java",
            class_name: "com.linkedin.venice.Ingestion",
            line_num: 42,
        };
        assert_eq!(frame.frame_id, 12345);
        assert_eq!(frame.method_name, "processRecord");
        assert_eq!(frame.method_signature, "(Lcom/linkedin/venice/Record;)V");
        assert_eq!(frame.source_file, "Ingestion.java");
        assert_eq!(frame.class_name, "com.linkedin.venice.Ingestion");
        assert_eq!(frame.line_num, 42);
    }

    #[test]
    fn test_resolved_stack_frame_special_line_numbers() {
        // Verify the line number encoding contract
        let unknown = ResolvedStackFrame {
            frame_id: 1, method_name: "", method_signature: "",
            source_file: "", class_name: "", line_num: -1,
        };
        let compiled = ResolvedStackFrame {
            frame_id: 2, method_name: "", method_signature: "",
            source_file: "", class_name: "", line_num: -2,
        };
        let native = ResolvedStackFrame {
            frame_id: 3, method_name: "", method_signature: "",
            source_file: "", class_name: "", line_num: -3,
        };
        assert_eq!(unknown.line_num, -1);
        assert_eq!(compiled.line_num, -2);
        assert_eq!(native.line_num, -3);
    }

    #[test]
    fn test_resolved_stack_trace_fields() {
        let trace = ResolvedStackTrace {
            stack_trace_serial: 100,
            thread_serial: 5,
            frame_ids: vec![1000, 2000, 3000],
        };
        assert_eq!(trace.stack_trace_serial, 100);
        assert_eq!(trace.thread_serial, 5);
        assert_eq!(trace.frame_ids, vec![1000, 2000, 3000]);
    }

    #[test]
    fn test_resolved_stack_trace_empty_frames() {
        let trace = ResolvedStackTrace {
            stack_trace_serial: 1,
            thread_serial: 1,
            frame_ids: vec![],
        };
        assert!(trace.frame_ids.is_empty());
    }
}
