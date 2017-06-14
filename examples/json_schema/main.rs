extern crate fourree;
extern crate rand;

#[macro_use]
extern crate log;

use fourree::json::load_schema_from_file;

/// A script that loads a JSON schema file,
/// generates some data based on it, and outputs
/// the result to stdout.
///
/// # Example
///
/// cargo run --example json_schema
///
fn main() {
    // Create our generator
    let mut rng = rand::thread_rng();

    match load_schema_from_file("examples/json_schema/example.json") {
        Ok(schema)  => {
            println!("Schema \"{}\" successfully loaded.", schema);
            println!("{}", schema.generate_row(&mut rng).unwrap());
        }
        Err(err)    => {
            println!("{}", err);
            return
        }
    }
}
