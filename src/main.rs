extern crate rand;
extern crate getopts;
extern crate time;
extern crate fourree;

use std::env;
use getopts::Options;

use fourree::json::{load_schema_from_file};

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}

const NUM_ROWS_DEFAULT: u64 = 100;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("n", "num_rows", "specify number of records to generate", "NUM_ROWS");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    let num_rows: u64;
    if matches.opt_present("n") {
        println!("Received option: num_rows = {}", matches.opt_str("n").unwrap());
        num_rows = match matches.opt_str("n").unwrap().trim().parse::<u64>() {
            Err(err) => {
                println!("ERROR: {}, using default value {}", err, NUM_ROWS_DEFAULT);
                NUM_ROWS_DEFAULT
            },
            Ok(num_rows) => num_rows
        }
    } else {
        num_rows = NUM_ROWS_DEFAULT;
    }

    let input_file = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        print_usage(&program, opts);
        return;
    };

    println!("Loading schema from: {:?}", input_file);

    let start_time = time::now();

    match load_schema_from_file(&input_file) {
        Ok(schema) => {
            let mut rng = rand::thread_rng();

            for _ in 1..num_rows {
                //println!("{}", schema.generate_row(&mut rng, "\t"));
                schema.generate_row(&mut rng, "\t");
            }
        }
        Err(err) => {
            println!("{}", err);
            return;
        }
    }

    let end_time = time::now();
    println!("\nElapsed time: {} ms\n", (end_time-start_time).num_milliseconds());
}
