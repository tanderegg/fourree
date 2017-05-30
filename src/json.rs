use serde_json::{Value, Map, from_str};

use std::io::prelude::*;
use std::fs::File;

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
        json.get("table_name").expect("Table name must be specified!")
            .as_str().expect("Table name must be a string!");

    let mut schema = Schema {
        table_name: table_name.to_string(),
        fields: Vec::new()
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
            match parse_fields(fields.clone(), &mut schema) {
                Ok(_) => {
                    Ok(schema)
                }
                Err(err) => Err(err)
            }
        })
}

/// Loops through all the fields provided by the schema, and validates them.
///
/// # Examples
/// ```
/// let result = parse_fields(fields, schema);
/// ```
fn parse_fields(fields: Vec<Value>, schema: &mut Schema) -> Result<String, String> {
    for field in fields.iter() {
        match field.as_object() {
            Some(obj) => {
                match parse_field(obj) {
                    // TODO: Rather than take a mutable schema object, create it and return it
                    Ok(f) => schema.add_field(f),
                    Err(err) => return Err(err)
                }
            }
            None => {
                return Err("Each field must be an object".to_string())
            }
        }
    }
    Ok("Success".to_string())
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
    let field_name =
        match obj.get("name")
                 .ok_or("Field name is required.".to_string())
                 .and_then(|name| {
                     name.as_str()
                         .ok_or("Field name must be a string!".to_string())
                 })
        {
            Ok(name) => name,
            Err(err) => return Err(err)
        };

    let data_type =
        match obj.get("data_type")
                 .ok_or("Data type is required.".to_string())
                 .and_then(|data_type| {
                     data_type.as_str()
                              .ok_or("Data type must be a string!".to_string())
                 })
        {
            Ok(dt)   => dt,
            Err(err) => return Err(err)
        };

    let generator_type =
        match obj.get("generator")
                 .ok_or("Generator is required.".to_string())
                 .and_then(|data_type| {
                     data_type.as_str()
                              .ok_or("Generator must be a string!".to_string())
                 })
        {
            Ok(g)   => g,
            Err(err) => return Err(err)
        };

    let generator = match generator_type {
        "integer" => {
            match parse_integer(obj) {
                Ok(g)    => g,
                Err(err) => return Err(err)
            }
        }
        "gauss" => {
            match parse_gauss(obj) {
                Ok(g)    => g,
                Err(err) => return Err(err)
            }
        }
        "string" => {
            match parse_string(obj) {
                Ok(g)    => g,
                Err(err) => return Err(err)
            }
        }
        "date" => {
            match parse_date() {
                Ok(g)    => g,
                Err(err) => return Err(err)
            }
        }
        "choice" => {
            match parse_choice(obj) {
                Ok(g)    => g,
                Err(err) => return Err(err)
            }
        }
        _ => {
            FieldGenerator::NoGen
        }
    };

    Ok(Field{
        name: field_name.to_string(),
        data_type: data_type.to_string(),
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
    let min =
        match obj.get("min")
                 .ok_or("Min is required for an integer field.".to_string())
                 .and_then(|min| {
                    min.as_i64()
                       .ok_or("Min must be an integer!".to_string())
                 })
        {
            Ok(m)    => m,
            Err(err) => return Err(err)
        };

    let max =
        match obj.get("max")
                 .ok_or("Max is required for an integer field.".to_string())
                 .and_then(|max| {
                    max.as_i64()
                       .ok_or("Max must be an integer!".to_string())
                 })
        {
            Ok(m)    => m,
            Err(err) => return Err(err)
        };

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
    let mean =
        match obj.get("mean")
                 .ok_or("Mean is required for a gauss distribution field.".to_string())
                 .and_then(|std_dev| {
                    std_dev.as_i64()
                           .ok_or("Mean must be a number!".to_string())
                 })
        {
            Ok(m)    => m,
            Err(err) => return Err(err)
        };
    let std_dev =
        match obj.get("mean")
                 .ok_or("Std deviation is required for a gauss distribution field.".to_string())
                 .and_then(|std_dev| {
                    std_dev.as_i64()
                           .ok_or("Std deviation must be a number!".to_string())
                 })
        {
            Ok(m)    => m,
            Err(err) => return Err(err)
        };

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
    let length =
        match obj.get("length")
                 .ok_or("Length is required for a string field.".to_string())
                 .and_then(|length| {
                    length.as_u64()
                          .ok_or("Length must be a positive integer!".to_string())
                 })
        {
            Ok(l)    => l as usize,
            Err(err) => return Err(err)
        };

    Ok(FieldGenerator::String{ length: length })
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
            for choice in array.iter() {
                match choice.as_str() {
                    Some(c) => {
                        choices.push(c.to_string());
                    }
                    None => return Err("All choices must be strings.".to_string())
                }
            }
            Ok(FieldGenerator::Choice{ choices: choices, length: length })
       })
}
