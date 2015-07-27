
use std::fmt;
use rand;

use generators::*;

trait Generator {
    fn generate<R: rand::Rng>(&self, rng: &mut R) -> String;
}

pub enum FieldGenerator {
    NoGen,
    Integer(i64, i64),
    Gauss(f64, f64),
    Date,
    String(usize),
    Choice(Vec<String>)
}

pub struct Field {
    pub name: String,
    pub data_type: String,
    pub generator: FieldGenerator
}

impl Generator for Field {
    fn generate<R: rand::Rng>(&self, rng: &mut R) -> String {
        match self.generator {
            FieldGenerator::Integer(min, max) => {
                generate_integer(rng, min, max).to_string()
            }
            FieldGenerator::Gauss(mean, std_dev) => {
                generate_gauss(rng, mean, std_dev).to_string()
            }
            FieldGenerator::String(length) => {
                generate_string(rng, length)
            }
            FieldGenerator::Date => {
                generate_date(rng).to_string()
            }
            FieldGenerator::Choice(ref choices) => {
                generate_choice(rng, choices.as_slice()).to_string()
            }
            _ => "None".to_string()
        }
    }
}

pub struct Schema {
    pub table_name: String,
    pub fields: Vec<Field>
}

impl Schema {
    pub fn add_field(&mut self, f: Field) {
        self.fields.push(f);
    }

    pub fn generate_row(&self, rng: &mut rand::ThreadRng, delim: &str) -> String {
        let mut result = String::new();

        for field in self.fields.iter() {
            result = result + &field.generate(rng);
            result = result + delim
        }
        result
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.table_name)
    }
}
