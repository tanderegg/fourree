use std::io::prelude::*;
use std::fs::File;

use serde_json::{Value, Map, from_str};

use schema::{Schema, Field, FieldGenerator};

/// Takes a filename as input, then parses it according to the Fourree format.
/// Any parsing errors cause the process to abort.
///
/// # Examples
/// ```
/// let result = load_schema_from_file("myfile.json");
/// ```
pub fn load_schema_from_file<'input>(file_name: &'input str) -> Result<Schema, String> {
    // Open the file, extract contents as a string, and load the schema
    let mut raw_json = String::new();

    File::open(file_name)
        .map_err(|err| err.to_string())
        .and_then(|mut file| {
            file.read_to_string(&mut raw_json)
                .map_err(|err| err.to_string())
        })
        .and_then(|_| {
            parse_json(raw_json)
        })
}

/// Takes a string as input, then parses is according to the Fourree format.
/// Any parsing errors cause the process to abort return the error.
///
/// # Examples
/// ```
/// let result = load_schema("{\"table_name\": \"my_table\", \"fields\": []}");
/// ```
pub fn parse_json(raw_json: String) -> Result<Schema, String> {
    let json_parsed: Value = from_str(&raw_json).expect("Invalid JSON string!");

    json_parsed.as_object()
        .ok_or("Root JSON value must be an object.".to_string())
        .and_then(|j| {
             parse_schema(j.clone())
        })
}

/// Parses a given JSON Map formatted schema
///
/// # Examples
/// ```
/// let result = parse_schema(json_map)
/// ```
fn parse_schema(json: Map<String, Value>) -> Result<Schema, String> {

    let table_name =
        json.get("table_name")
            .ok_or("Table name must be specified!")
            .and_then(|tn| {
                tn.as_str()
                  .ok_or("Table name must be a string!")
            })?;

    let delimiter =
        match json.get("delimiter") {
            Some(d) => d.as_str().ok_or("Delimiter must be a string!")?,
            None => {
                "\t"
            }
        };

    // Now process all the fields in the schema
    // fields must be an array containing objects
    json.get("fields")
        .ok_or("Fields must be provided!".to_string())
        .and_then(|fields| {
            fields.as_array()
                  .ok_or("Fields must be an array.".to_string())
        })
        .and_then(|fields| {
            parse_fields(fields.clone(), table_name, delimiter)
        })
}

/// Loops through all the fields provided by the schema, and validates them.
///
/// # Examples
/// ```
/// let result = parse_fields(fields, schema);
/// ```
fn parse_fields(fields: Vec<Value>, table_name: &str, delimiter: &str) -> Result<Schema, String> {
    let mut schema = Schema {
        table_name: table_name.to_string(),
        delimiter: delimiter.to_string(),
        fields: Vec::new()
    };

    for field in fields.iter() {
        let obj = field.as_object().ok_or("Each field must be an object")?;
        let field = parse_field(obj)?;
        if delimiter == "fixed" && field.length.is_none() {
            return Err("All fields must have a length if delimeter is 'fixed'.".to_string())
        }
        schema.add_field(field);
    }
    Ok(schema)
}

/// Takes a Map of the metadata for a field, validates it, and returns a Field object.  the
/// proper generator is selected at this time.
///
/// # Examples
/// ```
/// let field_data = json!("
/// {
///   "name": "myfield",
///   "data_type": "integer",
///   "generator": "gauss",
///   "mean": 1000,
///   "std_dev": 100
/// }
/// ")
/// let result = parse_field(field_data.as_object().unwrap())
/// ```
fn parse_field<'a>(obj: &'a Map<String, Value>) -> Result<Field, String> {
    let field_name = obj.get("name")
        .ok_or("Field name is required.".to_string())
        .and_then(|name| {
            name.as_str()
                .ok_or("Field name must be a string!".to_string())
        })?;

    let data_type = obj.get("data_type")
        .ok_or("Data type is required.".to_string())
        .and_then(|data_type| {
            data_type.as_str()
                .ok_or("Data type must be a string!".to_string())
        })?;

    let length = match obj.get("length") {
        Some(l) => Some(l.as_u64().ok_or("Length must be a positive integer!")? as usize),
        None => None
    };

    let padding = match obj.get("padding") {
        Some(p) => Some(p.as_str().ok_or("Padding must be a string!")?.to_string()),
        None => None
    };

    let generator_type = obj.get("generator")
        .ok_or("Generator is required.".to_string())
        .and_then(|data_type| {
            data_type.as_str()
                .ok_or("Generator must be a string!".to_string())
        })?;

    let generator = match generator_type {
        "integer" => parse_integer(obj)?,
        "gauss" => parse_gauss(obj)?,
        "string" => parse_string(obj)?,
        "date" => parse_date()?,
        "choice" => parse_choice(obj)?,
        _ => FieldGenerator::NoGen
    };

    Ok(Field{
        name: field_name.to_string(),
        data_type: data_type.to_string(),
        padding: padding,
        length: length,
        generator: generator
    })
}

/// Parses an integer field and creates the generator for it, which chooses a random value
/// between min and max.
/// # Examples
/// ```
/// let field_data = json!("
/// {
///   "name": "myfield",
///   "data_type": "integer",
///   "generator": "integer",
///   "min": 1,
///   "max": 100
/// }
/// ")
/// let integer_generator = parse_integer(field_data.as_object().unwrap()).unwrap()
/// ```
fn parse_integer<'a>(obj: &'a Map<String, Value>) -> Result<FieldGenerator, String> {
    let min = obj.get("min")
        .ok_or("Min is required for an integer field.".to_string())
        .and_then(|min| {
            min.as_i64()
                .ok_or("Min must be an integer!".to_string())
        })?;

    let max = obj.get("max")
        .ok_or("Max is required for an integer field.".to_string())
        .and_then(|max| {
            max.as_i64()
                .ok_or("Max must be an integer!".to_string())
        })?;

    Ok(FieldGenerator::Integer{ min: min, max: max })
}

/// Takes the JSON representation of a Field and produces a Gaussian Generator.
///
/// # Examples
/// ```
/// let field_data = json!("
/// {
///   "name": "myfield",
///   "data_type": "integer",
///   "generator": "gauss",
///   "mean": 1000,
///   "std_dev": 100
/// }
/// ")
/// let gauss_generator = parse_field(field_data.as_object().unwrap()).unwrap()
/// ```
fn parse_gauss<'a>(obj: &'a Map<String, Value>) -> Result<FieldGenerator, String> {
    let mean = obj.get("mean")
        .ok_or("Mean is required for a gauss distribution field.".to_string())
        .and_then(|std_dev| {
            std_dev.as_i64()
                .ok_or("Mean must be a number!".to_string())
        })?;
    let std_dev = obj.get("mean")
        .ok_or("Std deviation is required for a gauss distribution field.".to_string())
        .and_then(|std_dev| {
            std_dev.as_i64()
                .ok_or("Std deviation must be a number!".to_string())
        })?;

    Ok(FieldGenerator::Gauss{ mean: mean as i32, std_dev: std_dev as i32 })
}

/// Takes a JSON represntation of a string field and returns a String Generator.
///
/// # Examples
/// ```
/// let field_data = json!("
/// {
///   "name": "myfield",
///   "data_type": "varchar(6)",
///   "generator": "string",
///   "length": 6
/// }
/// ")
/// let string_generator = parse_field(field_data.as_object().unwrap()).unwrap()
/// ```
fn parse_string<'a>(obj: &'a Map<String, Value>) -> Result<FieldGenerator, String> {
    let length = obj.get("length")
        .ok_or("Length is required for a string field.".to_string())
        .and_then(|length| {
            length.as_u64()
                .ok_or("Length must be a positive integer!".to_string())
        })?;

    Ok(FieldGenerator::String{ length: length as usize })
}

/// Returns a new data generator, which has no configuration options.
fn parse_date() -> Result<FieldGenerator, String> {
    Ok(FieldGenerator::Date)
}

/// Takes a JSON representation of a choice field and returns a Choice generator,
/// which is used for generating strings from a list of options.
/// # Examples
/// ```
/// let field_data = json!("
/// {
///   "name": "myfield",
///   "data_type": "varchar(3)",
///   "generator": "choice",
///   "choices": ["1", "2", "3"]
/// }
/// ")
/// let choice_generator = parse_field(field_data.as_object().unwrap()).unwrap()
/// ```
fn parse_choice<'a>(obj: &'a Map<String, Value>) -> Result<FieldGenerator, String> {
    let length = match obj.get("length") {
        Some(length) => {
            let l = length.as_u64().ok_or("Length must be a positive integer!".to_string()).ok().unwrap();
            l as usize
        }
        _ => 1 as usize
    };

    obj.get("choices")
       .ok_or("A Choice field must have choices!".to_string())
       .and_then(|a| {
            a.as_array()
             .ok_or("Choices field must be an array!".to_string())
       })
       .and_then(|array| {
            let mut choices = Vec::new();

            let mut choice_length = 0;

            for choice in array.iter() {
                let c = choice.as_str().ok_or("All choices must be strings.".to_string())?;
                if c.len() > choice_length {
                    choice_length = c.len()
                }

                choices.push(c.to_string());
            }
            Ok(FieldGenerator::Choice{
                choices: choices,
                choice_length: choice_length,
                length: length
            })
       })
}
