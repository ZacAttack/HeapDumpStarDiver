use std::collections;
use std::sync::Arc;
use arrow_array::{ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, NullArray, RecordBatch, StringArray, StructArray, UInt16Array, UInt64Array};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use jvm_hprof::{EzClass, Id};
use jvm_hprof::heap_dump::{FieldValue, PrimitiveArrayType};
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
    let file = std::fs::File::create(format!("parquet/{}.parquet", filename_prefix)).unwrap();
    // let file = std::fs::File::create(format!("{}.parquet", filename_prefix)).unwrap();

    // WriterProperties can be used to set Parquet file options
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)// TODO: experiment with Gzip
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();

    writer.write(&batch).unwrap();

    // writer must be closed to write footer
    writer.close().unwrap();
}

// This function takes a type and generates a RecordBatch from it which includes a schema.
// There might be a speed advantage to be had by generating all the schemas for the different
// object types before hand.  It's not very clear how much memory that could consume.
fn generate_schema_from_type(
    field_val: &FieldValue,
    field_name: &str,
    obj_id_to_class_obj_id: &collections::HashMap<Id, Id>,
    classes: &collections::HashMap<Id, EzClass>,
    prim_array_obj_id_to_type: &collections::HashMap<Id, PrimitiveArrayType>,
    objectId: u64
) -> RecordBatch
{
    let mut schema_field_vec: Vec<Field> = vec![];
    schema_field_vec.push(Field::new("instance_id", DataType::UInt64, false));
    let mut data_vec:Vec<ArrayRef> = vec![];


    data_vec.push(Arc::new(UInt64Array::from(vec![objectId])));
    match field_val {
        FieldValue::ObjectId(Some(field_ref_id)) => {
            obj_id_to_class_obj_id
                .get(&field_ref_id)
                .map(|class_obj_id| {
                    // case where the field_ref_id is in the obj_id_to_class_object
                    // (essentially this is a reference to a single instance)

                    let class_type = classes
                        .get(class_obj_id)
                        .map(|c| c.name)
                        .unwrap_or("(class not found)");

                    let resolved_class_type;
                    if (field_ref_id.id() == 0) {
                        resolved_class_type = "null";
                    } else {
                        resolved_class_type = class_type;
                    }
                    let class_type_vector = Arc::new(StringArray::from(vec![resolved_class_type]));
                    let field_id = Arc::new(UInt64Array::from(vec![field_ref_id.id()]));
                    // TODO: Storing the type as a string in this struct is pretty inefficient.
                    // we're just going to end up making many many copies of what is the same string
                    // all over the place.  If we found disk size of the generated parquet's to be
                    // enormous, this would be a good place to start looking for optimizations.
                    let struct_array = StructArray::from(vec![
                        (
                            Arc::new(Field::new("id", DataType::UInt64, false)),
                            field_id.clone() as ArrayRef,
                        ),
                        (
                            Arc::new(Field::new("type", DataType::Utf8, false)),
                            class_type_vector.clone() as ArrayRef,
                        )
                    ]);

                    schema_field_vec.push(Field::new(field_name, DataType::Struct(
                        Fields::from(vec![
                            Field::new("id", DataType::UInt64, false),
                            Field::new("type", DataType::Utf8, false)])
                    ), false));
                    data_vec.push(Arc::new(struct_array));
                })
                .or_else(|| {
                    // Case where this is a primitive type array
                    prim_array_obj_id_to_type
                        .get(&field_ref_id)
                        .map(|prim_type| {


                            let primitive_array_name:String;
                            if (field_ref_id.id() == 0) {
                                primitive_array_name = "null".parse().unwrap();
                            } else {
                                primitive_array_name = format!("{}[]", prim_type.java_type_name());
                            }

                            let class_type_vector = Arc::new(StringArray::from(vec![primitive_array_name]));
                            let field_id = Arc::new(UInt64Array::from(vec![field_ref_id.id()]));

                            let struct_array = StructArray::from(vec![
                                (
                                    Arc::new(Field::new("id", DataType::UInt64, false)),
                                    field_id.clone() as ArrayRef,
                                ),
                                (
                                    Arc::new(Field::new("type", DataType::Utf8, false)),
                                    class_type_vector.clone() as ArrayRef,
                                )
                            ]);
                            schema_field_vec.push(Field::new(field_name, DataType::Struct(
                                Fields::from(vec![
                                    Field::new("id", DataType::UInt64, false),
                                    Field::new("type", DataType::Utf8, false)])
                            ), false));
                            data_vec.push(Arc::new(struct_array));

                        })
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
            // Push a vector with x
            schema_field_vec.push(Field::new(field_name, DataType::Null, true));
            data_vec.push(Arc::new(NullArray::new(1)));
        }
        FieldValue::Boolean(v) => {
            schema_field_vec.push(Field::new(field_name, DataType::Boolean, false));
            //     data_vec.push(Arc::new(UInt64Array::from(vec![objectId])));
            data_vec.push(Arc::new(BooleanArray::from(vec![*v])));
        }
        FieldValue::Char(v) => {
            // push a vector with x
            schema_field_vec.push(Field::new(field_name, DataType::UInt16, false));
            data_vec.push(Arc::new(UInt16Array::from(vec![*v])));
        }
        FieldValue::Float(v) => {
            schema_field_vec.push(Field::new(field_name, DataType::Float32, false));
            data_vec.push(Arc::new(Float32Array::from(vec![*v])));
        }
        FieldValue::Double(v) => {
            schema_field_vec.push(Field::new(field_name, DataType::Float64, false));
            data_vec.push(Arc::new(Float64Array::from(vec![*v])));
        }
        FieldValue::Byte(v) => {
            schema_field_vec.push(Field::new(field_name, DataType::Int8, false));
            data_vec.push(Arc::new(Int8Array::from(vec![*v])));
        }
        FieldValue::Short(v) => {
            schema_field_vec.push(Field::new(field_name, DataType::Int16, false));
            data_vec.push(Arc::new(Int16Array::from(vec![*v])));
        }
        FieldValue::Int(v) => {
            schema_field_vec.push(Field::new(field_name, DataType::Int32, false));
            data_vec.push(Arc::new(Int32Array::from(vec![*v])));
        }
        FieldValue::Long(v) => {
            schema_field_vec.push(Field::new(field_name, DataType::Int64, false));
            data_vec.push(Arc::new(Int64Array::from(vec![*v])));
        }
    }

    RecordBatch::try_new(
        Arc::new(Schema::new(schema_field_vec)),
        data_vec
    ).unwrap()
}