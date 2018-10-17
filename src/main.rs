extern crate rand;
extern crate fourree;
extern crate time;

#[macro_use]
extern crate log;

use std::env;

use fourree::config;
use fourree::json::{load_schema_from_file};
use fourree::util::{generate_data};

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = match config::load(args) {
        Ok(config) => config,
        Err(error) => {
            error!("{}", error);
            return
        }
    };

    info!("Loading schema from: {:?}", config.input_file);

    let start_time = time::precise_time_s();

    // Load and generate the data, sending it to OutputMode
    let schema = match load_schema_from_file(&config.input_file) {
        Ok(s) => s,
        Err(err) => {
            error!("{}", err);
            return;
        }
    };

    generate_data(&config, schema);

    let end_time = time::precise_time_s();
    info!("Elapsed time: {} s", end_time-start_time);
}
