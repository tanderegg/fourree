extern crate rand;
extern crate fourree;
extern crate time;

#[macro_use]
extern crate log;

use std::env;

use fourree::config;
use fourree::json::{parse_json};
use fourree::util::{generate_data};

fn main() {
    // Configure based on command line parameters
    let args: Vec<String> = env::args().collect();

    let config = match config::load(args) {
        Ok(config) => config,
        Err(error) => {
            error!("{}", error);
            return
        }
    };

    // Load schema from source file
    debug!("Loading schema from: {:?}", config.input_file);
    let start_time = time::precise_time_s();

    // Load and generate the data, sending it to OutputMode
    let schema = match parse_json(&config.input_file) {
        Ok(s) => s,
        Err(err) => {
            error!("{}", err);
            return;
        }
    };

    // Generate the data based on configuration and schema
    info!("Beginning data generation.");
    match generate_data(&config, schema) {
        Ok(_) => info!("Data successfully generated."),
        Err(e) => error!("{}", e)
    };

    let end_time = time::precise_time_s();
    info!("Elapsed time: {} s", end_time-start_time);
}
