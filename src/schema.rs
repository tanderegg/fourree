use std::fmt;
use rand;

use generators::*;

trait Generator {
    fn generate<R: rand::Rng>(&self, rng: &mut R) -> String;
}

pub enum FieldGenerator {
    NoGen,
    Integer { min: i64, max: i64 },
    Gauss { mean: i32, std_dev: i32 },
    Date,
    String { length: usize },
    Choice { choices: Vec<String>, choice_length: usize, length: usize }
}

pub struct Field {
    pub name: String,
    pub data_type: String,
    pub length: Option<usize>,
    pub padding: Option<String>,
    pub generator: FieldGenerator
}

impl Generator for Field {
    fn generate<R: rand::Rng>(&self, rng: &mut R) -> String {
        match self.generator {
            FieldGenerator::Integer{ min, max } => {
                generate_integer(rng, min, max).to_string()
            }
            FieldGenerator::Gauss{ mean, std_dev } => {
                generate_gauss(rng, mean, std_dev).to_string()
            }
            FieldGenerator::String{ length } => {
                generate_string(rng, length)
            }
            FieldGenerator::Date => {
                generate_date(rng).to_string()
            }
            FieldGenerator::Choice{ ref choices, choice_length, length } => {
                generate_choice(rng, choices.as_slice(), choice_length, length).to_string()
            }
            _ => "None".to_string()
        }
    }
}

pub struct Schema {
    pub table_name: String,
    pub delimiter: String,
    pub fields: Vec<Field>
}

impl Schema {
    pub fn add_field(&mut self, f: Field) {
        self.fields.push(f);
    }

    pub fn generate_row(&self, rng: &mut rand::ThreadRng) -> Result<String, String> {
        let mut result = Vec::with_capacity(self.fields.len());

        for field in self.fields.iter() {
            let mut field_data = field.generate(rng);

            if self.delimiter == "fixed" {
                let field_length = field.length.ok_or(
                    format!("'length' is required for a fixed file
                             format, but is missing for field {}", field.name))?;

                let length_diff = field_length - field_data.len();
                match field.padding {
                    Some(ref p) => {
                        let mut temp_string = p.repeat(length_diff);
                        temp_string.push_str(field_data.as_str());
                        field_data = temp_string;
                    },
                    None => {
                        if !length_diff == 0 {
                            return Err(format!(
                                "'padding' is undefined for field {} but
                                field_data is less than 'length'.", field.name))
                        }
                    }
                }
            }
            result.push(field_data);
        }

        let delim = match self.delimiter.as_str() {
            "fixed" => "",
            d => d
        };

        Ok(result.join(delim))
    }

    pub fn generate_rows(&self, rng: &mut rand::ThreadRng, size: u64) -> Result<String, String> {
        let mut output = String::new();

        for _ in 0..size {
            let row = self.generate_row(rng)?;
            debug!("{}", row);
            output.push_str(&row);
            output.push('\n');
        }
        Ok(output)
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.table_name)
    }
}
