use std::collections;
use std::sync::Arc;
use arrow_array::{ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, NullArray, RecordBatch, StringArray, StructArray, UInt16Array, UInt64Array};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use jvm_hprof::{EzClass, Hprof, Id};
use jvm_hprof::heap_dump::{FieldDescriptor, FieldDescriptors, FieldValue, PrimitiveArrayType};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

// TODO: This opens and flushes a file with every call to this function.  This is not efficient.
// the writer itself has a notion of how much memory it's taking up.  What we could do is keep
// an array of open writers, and when the cumulative size of the memory getting used by these writers
// reaches some threshhold, we could them flush them all and then start buffering again.
// For MVP this 'seems' to be fast enough, but it's an easy opportunity to speed things up in exchange
// for using more memory.
pub fn write_to_parquet(filename_prefix: &str, batch: RecordBatch) {
    let filename_prefix = filename_prefix.replace("/", ".");
    
    // We need to open the file if it exists, or create it if it doesn't
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(format!("parquet/{}.parquet", filename_prefix))
        .unwrap();
    
    // let file = std::fs::File::create(format!("parquet/{}.parquet", filename_prefix)).unwrap();
    // let file = std::fs::File::create(format!("{}.parquet", filename_prefix)).unwrap();

    // WriterProperties can be used to set Parquet file options
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)// TODO: experiment with Gzip
        .build();
    
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).unwrap();

    // writer must be closed to write footer
    writer.close().unwrap();
    if filename_prefix == "sun.nio.fs.UnixPath" {
        println!("Writing to file: {}", filename_prefix);
    }
}

const MISSING_UTF8: &str = "(missing utf8)";

// This function takes a type and generates a RecordBatch from it which includes a schema.
// There might be a speed advantage to be had by generating all the schemas for the different
// object types before hand.  It's not very clear how much memory that could consume.
pub fn generate_schema_from_type(
    hprof: &Hprof,
    field_descriptors: &Vec<FieldDescriptor>,
    mut field_val_input: &[u8],
    utf8: &collections::HashMap<Id, &str>,
    obj_id_to_class_obj_id: &collections::HashMap<Id, Id>,
    classes: &collections::HashMap<Id, EzClass>,
    prim_array_obj_id_to_type: &collections::HashMap<Id, PrimitiveArrayType>,
) -> Schema
{
    let mut field_vec: Vec<Field> = vec![];
    for fd in field_descriptors.iter() {
        let (input, field_val) = fd
            .field_type()
            .parse_value(field_val_input, hprof.header().id_size())
            .unwrap();
        field_val_input = input;
        let field_name: &str = utf8.get(&fd.name_id()).unwrap_or_else(|| &MISSING_UTF8);
        match field_val {
            FieldValue::ObjectId(Some(field_ref_id)) => {
                obj_id_to_class_obj_id
                    .get(&field_ref_id)
                    .map(|class_obj_id| {
                        // case where the field_ref_id is in the obj_id_to_class_object
                        // (essentially this is a reference to a single instance)
                        field_vec.push(Field::new(field_name, DataType::Struct(
                            Fields::from(vec![
                                Field::new("id", DataType::UInt64, false),
                                Field::new("type", DataType::Utf8, false)])
                        ), false));
                        // println!("{:?}", input);
                        // println!("{} {}: field_ref_id: {}, field_ref_type: {}", field_name, &fd.name_id(), field_ref_id, classes.get(obj_id_to_class_obj_id.get(&field_ref_id).unwrap()).unwrap().name);
                        // println!("class_obj_id: {}, class_obj_type: {}", class_obj_id, classes.get(class_obj_id).unwrap().name);
                    })
                    .or_else(|| {
                        // Case where this is a primitive type array
                        prim_array_obj_id_to_type
                            .get(&field_ref_id)
                            .map(|prim_type| {
                                // field_vec.push(Field::new(field_name, DataType::List(Arc::new(Field::new("id", DataType::UInt64, false))), false));
                                field_vec.push(Field::new(field_name, DataType::Struct(
                                    Fields::from(vec![
                                        Field::new("id", DataType::UInt64, false),
                                        Field::new("type", DataType::Utf8, false)])
                                ), false));
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
                field_vec.push(Field::new(field_name, DataType::Struct(
                    Fields::from(vec![
                        Field::new("id", DataType::UInt64, false),
                        Field::new("type", DataType::Utf8, false)])
                ), false));
                // field_vec.push(Field::new(field_name, DataType::Null, true));
            }
            FieldValue::Boolean(v) => {
                field_vec.push(Field::new(field_name, DataType::Boolean, false));
            }
            FieldValue::Char(v) => {
                field_vec.push(Field::new(field_name, DataType::UInt16, false));
            }
            FieldValue::Float(v) => {
                field_vec.push(Field::new(field_name, DataType::Float32, false));
            }
            FieldValue::Double(v) => {
                field_vec.push(Field::new(field_name, DataType::Float64, false));
            }
            FieldValue::Byte(v) => {
                field_vec.push(Field::new(field_name, DataType::Int8, false));
            }
            FieldValue::Short(v) => {
                field_vec.push(Field::new(field_name, DataType::Int16, false));
            }
            FieldValue::Int(v) => {
                field_vec.push(Field::new(field_name, DataType::Int32, false));
            }
            FieldValue::Long(v) => {
                field_vec.push(Field::new(field_name, DataType::Int64, false));
            }
        }
    }

    Schema::new(field_vec)
}