
use std::fmt;
use rand;

use generators::*;

trait Generator {
    fn generate<R: rand::Rng>(&self, rng: &mut R) -> String;
}

pub enum FieldGenerator {
    NoGen,
    Integer(i64, i64),
    Gauss(u64, u64),
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
        let mut result = Vec::with_capacity(self.fields.len());

        for field in self.fields.iter() {
            result.push(field.generate(rng));
        }
        result.as_slice().join(delim)
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.table_name)
    }
}
