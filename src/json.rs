use serde::json::{self, Value};

use std::io::prelude::*;
use std::fs::File;
use std::collections::BTreeMap;

use schema::{Schema, Field, FieldGenerator};

/// load_schema_from_file takes a filename as input, then
/// parses is according to the Fourree format.
/// Any parsing errors cause the process to abort
/// return the error.
///
/// # Examples
///
/// let result = load_schema_from_file("myfile.json");
///
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

/// load_schema takes a string as input, then
/// parses is according to the Fourree format.
/// Any parsing errors cause the process to abort
/// return the error.
///
/// # Examples
///
/// let result = load_schema("{\"table_name\": \"my_table\", \"fields\": []}");
///
pub fn parse_json(raw_json: String) -> Result<Schema, String> {
    // Parse the string with serde as JSON
    /*let json: Value =
        match json::from_str(&raw_json) {
            Ok(j)       => j,
            Err(err)    => return Err(err.to_string())
        };*/
    let json_parsed: Value = json::from_str(&raw_json).unwrap();

    json_parsed.as_object()
        .ok_or("Root JSON value must be an object.".to_string())
        .and_then(|j| {
             parse_schema(j.clone())
        })

    /*match json_deser {
        Ok(json) => parse_schema(json),
        Err(err) => Err(err)
    }*/
}

fn parse_schema(json: BTreeMap<String, Value>) -> Result<Schema, String> {

    let table_name: Result<&str, String> =
        json.get("table_name")
            .ok_or("Table name must be specified!".to_string())
            .and_then(|name| {
                name.as_string()
                    .ok_or("Table name must be a string!".to_string())
            });

    match table_name {
        Ok(tn) => {
            let mut schema = Schema {
                table_name: tn.to_string(),
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
        Err(err) => Err(err)
    }

    /*let json_object =
        match json.as_object() {
            Some(o) => o,
            None => return Err()
        };*/

    // We must have a "table_name" field, or else
    // return with the error.


    // This was when I was doing the match as a separate step
    // Perhaps slightly more readible, but needs an
    // extraneous temp variable
    /*let table_name = match table_name_result {
        Ok(name) => name,
        Err(err) => return Err(err.to_string())
    };*/

    // Here it is using early returns only
    // Maybe more readable, but fairly longwinded
    // and less clear at a glance what the purpose is
    /*match json_object.get("table_name") {
        Some(name) => {
            match *name {
                Value::String(ref string) => table_name = string.clone(),
                _ => return Err("Table name must be a string!".to_string())
            }
        }
        None => {
            return Err("Table name must be specified!".to_string())
        }
    }*/
}

fn parse_fields(fields: Vec<Value>, schema: &mut Schema) -> Result<String, String> {
    for field in fields.iter() {
        match field.as_object() {
            Some(obj) => {
                match parse_field(obj) {
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

fn parse_field<'a>(obj: &'a BTreeMap<String, Value>) -> Result<Field, String> {
    let field_name =
        match obj.get("name")
                 .ok_or("Field name is required.".to_string())
                 .and_then(|name| {
                     name.as_string()
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
                     data_type.as_string()
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
                     data_type.as_string()
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

fn parse_integer<'a>(obj: &'a BTreeMap<String, Value>) -> Result<FieldGenerator, String> {
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

    Ok(FieldGenerator::Integer(min, max))
}

fn parse_gauss<'a>(obj: &'a BTreeMap<String, Value>) -> Result<FieldGenerator, String> {
    let mean =
        match obj.get("mean")
                 .ok_or("Mean is required for a gauss distribution field.".to_string())
                 .and_then(|std_dev| {
                    std_dev.as_u64()
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
                    std_dev.as_u64()
                           .ok_or("Std deviation must be a number!".to_string())
                 })
        {
            Ok(m)    => m,
            Err(err) => return Err(err)
        };

    Ok(FieldGenerator::Gauss(mean, std_dev))
}

fn parse_string<'a>(obj: &'a BTreeMap<String, Value>) -> Result<FieldGenerator, String> {
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

    Ok(FieldGenerator::String(length))
}

fn parse_date() -> Result<FieldGenerator, String> {
    Ok(FieldGenerator::Date)
}

fn parse_choice<'a>(obj: &'a BTreeMap<String, Value>) -> Result<FieldGenerator, String> {
    obj.get("choices")
       .ok_or("A Choice field must have choices!".to_string())
       .and_then(|a| {
            a.as_array()
             .ok_or("Choices field must be an array!".to_string())
       })
       .and_then(|array| {
            let mut choices = Vec::new();
            for choice in array.iter() {
                match choice.as_string() {
                    Some(c) => {
                        choices.push(c.to_string());
                    }
                    None => return Err("All choices must be strings.".to_string())
                }
            }
            Ok(FieldGenerator::Choice(choices))
       })
}
