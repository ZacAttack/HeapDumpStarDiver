use std::collections;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema};
use jvm_hprof::{Hprof, Id};
use jvm_hprof::heap_dump::{FieldDescriptor, FieldType, FieldValue};
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
    declaring_classes: Option<&Vec<&str>>,
) -> Schema
{
    let mut field_vec: Vec<Field> = vec![];
    let mut name_counts: collections::HashMap<&str, usize> = collections::HashMap::new();
    for (i, fd) in field_descriptors.iter().enumerate() {
        let (input, field_val) = fd
            .field_type()
            .parse_value(field_val_input, hprof.header().id_size())
            .unwrap();
        field_val_input = input;
        let base_name: &str = utf8.get(&fd.name_id()).unwrap_or_else(|| &MISSING_UTF8);
        let count = name_counts.entry(base_name).or_insert(0);
        let field_name = if *count == 0 {
            base_name.to_string()
        } else {
            // Prefix with declaring class short name for disambiguation
            let class_prefix = declaring_classes
                .and_then(|dc| dc.get(i))
                .map(|c| c.rsplit('/').next().unwrap_or(c))
                .unwrap_or("unknown");
            format!("{}@{}", class_prefix, base_name)
        };
        *count += 1;
        match field_val {
            FieldValue::ObjectId(Some(_)) => {
                // All reference types (instance, primitive array, class, unresolvable)
                // use the same schema: Struct{id, type}
                field_vec.push(Field::new(field_name, DataType::Struct(
                    Fields::from(vec![
                        Field::new("id", DataType::UInt64, false),
                        Field::new("type", DataType::Utf8, false)])
                ), false));
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

/// Generate an Arrow schema from field descriptors and their types alone.
/// No instance data required — the type mapping is deterministic from FieldType.
pub fn generate_schema_from_descriptors(
    field_descriptors: &[FieldDescriptor],
    utf8: &collections::HashMap<Id, &str>,
    declaring_classes: Option<&Vec<&str>>,
    robo_mode: bool,
) -> Schema {
    let ref_struct_type = DataType::Struct(Fields::from(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("type", DataType::Utf8, false),
    ]));

    let mut field_vec: Vec<Field> = vec![];
    let mut name_counts: collections::HashMap<&str, usize> = collections::HashMap::new();
    for (i, fd) in field_descriptors.iter().enumerate() {
        let base_name: &str = utf8.get(&fd.name_id()).unwrap_or_else(|| &MISSING_UTF8);
        let count = name_counts.entry(base_name).or_insert(0);
        let field_name = if *count == 0 {
            base_name.to_string()
        } else {
            let class_prefix = declaring_classes
                .and_then(|dc| dc.get(i))
                .map(|c| c.rsplit('/').next().unwrap_or(c))
                .unwrap_or("unknown");
            format!("{}@{}", class_prefix, base_name)
        };
        *count += 1;

        let data_type = match fd.field_type() {
            FieldType::ObjectId => if robo_mode { DataType::UInt64 } else { ref_struct_type.clone() },
            FieldType::Boolean => DataType::Boolean,
            FieldType::Char => DataType::UInt16,
            FieldType::Float => DataType::Float32,
            FieldType::Double => DataType::Float64,
            FieldType::Byte => DataType::Int8,
            FieldType::Short => DataType::Int16,
            FieldType::Int => DataType::Int32,
            FieldType::Long => DataType::Int64,
        };
        field_vec.push(Field::new(field_name, data_type, false));
    }

    Schema::new(field_vec)
}