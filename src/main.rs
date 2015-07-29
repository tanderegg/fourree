extern crate rand;
extern crate getopts;
extern crate time;
extern crate fourree;

#[macro_use]
extern crate log;

use std::env;
use std::thread;
use std::sync::Arc;
use getopts::Options;
use log::LogLevelFilter;

use fourree::json::{load_schema_from_file};
use fourree::logger::init_logger;

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}", opts.usage(&brief));
}

const NUM_ROWS_DEFAULT: u64 = 1000;
const BATCH_SIZE_DEFAULT: u64 = 100;
const MAX_THREADS: u64 = 128;

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("n", "num_rows", "specify number of records to generate", "NUM_ROWS");
    opts.optopt("b", "batch_size", "specify the size of each batch to be processed", "BATCH_SIZE");
    opts.optopt("l", "log_file", "specify a file to write the log to", "LOG_FILE_PATH");
    opts.optopt("t", "threads", "specify the number of threads to use (default 1)", "NUM_THREADS");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => {
            println!("ERROR - {}", f);
            print_usage(&program, opts);
            return;
        }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return;
    }

    if matches.opt_present("l") {
        let file_path = matches.opt_str("l").unwrap().trim().to_string();
        init_logger(LogLevelFilter::Info, Some(file_path.clone())).ok().expect("Failed to initalize logger!");
    } else {
        init_logger(LogLevelFilter::Info, None).ok().expect("Failed to initialize logger!");
    }

    let num_rows = if matches.opt_present("n") {
        let rows_opt = matches.opt_str("n").unwrap().trim().to_string();
        info!("Received option: num_rows = {}", rows_opt);
        match rows_opt.parse::<u64>() {
            Err(err) => {
                warn!("{}, using default value {}", err, NUM_ROWS_DEFAULT);
                NUM_ROWS_DEFAULT
            },
            Ok(nrows) => nrows
        }
    } else {
        NUM_ROWS_DEFAULT
    };

    let batch_size = if matches.opt_present("b") {
        let batch_opt = matches.opt_str("b").unwrap().trim().to_string();
        info!("Received option: batch_size = {}", batch_opt);
        match batch_opt.parse::<u64>() {
            Err(err) => {
                warn!("{}, using default value {}", err, BATCH_SIZE_DEFAULT);
                BATCH_SIZE_DEFAULT
            },
            Ok(bsize) => { bsize }
        }
    } else {
        BATCH_SIZE_DEFAULT
    };

    let input_file = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        print_usage(&program, opts);
        return;
    };

    let num_threads = if matches.opt_present("t") {
        let thread_opt = matches.opt_str("t").unwrap().trim().to_string();
        info!("Received option: threads = {}", thread_opt);
        match thread_opt.parse::<u64>() {
            Err(err) => {
                warn!("{}, using default value {}", err, 1);
                1
            }
            Ok(threads) => {
                if threads > MAX_THREADS {
                    warn!("Can't have more than {} threads, using {}", MAX_THREADS, MAX_THREADS);
                    MAX_THREADS
                } else {
                    threads
                }
            }
        }
    } else {
        1
    };

    info!("Loading schema from: {:?}", input_file);

    let start_time = time::precise_time_s();

    match load_schema_from_file(&input_file) {
        Ok(schema) => {

            if num_threads > 1 {
                let batches = num_rows / batch_size;
                let batches_per_thread = batches / num_threads;
                let mut threads = Vec::with_capacity(num_threads as usize);
                let schema_ref = Arc::new(schema);

                for _ in 0..num_threads {
                    let thread_schema = schema_ref.clone();
                    threads.push(thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        for _ in 0..batches_per_thread.clone() {
                            let mut batch = String::new();
                            let batch_start = time::precise_time_s();
                            for _ in 0..batch_size.clone() {
                                let row = thread_schema.generate_row(&mut rng, "\t");
                                debug!("{}", row);
                                batch.push_str(&row);
                                batch.push('\n');
                            }
                            let batch_elapsed = time::precise_time_s();
                            info!("{} rows proccessed, {} s elapsed", batch_size, batch_elapsed-batch_start);
                        }
                    }));
                }

                for thread in threads {
                    info!("{:?} completed.", thread.join().unwrap());
                }
            } else {
                let mut rng = rand::thread_rng();
                let mut batch_start = time::precise_time_s();
                let mut batch = String::new();

                for i in 0..num_rows {
                    let row = schema.generate_row(&mut rng, "\t");
                    debug!("{}", row);
                    batch.push_str(&row);
                    batch.push('\n');

                    if i % batch_size == 0 {
                        let batch_elapsed = time::precise_time_s();
                        info!("{} rows proccessed, {} s elapsed", batch_size, batch_elapsed-batch_start);
                        batch = String::new();
                        batch_start = time::precise_time_s();
                    }
                }
            }
        }
        Err(err) => {
            error!("{}", err);
            return;
        }
    }

    let end_time = time::precise_time_s();
    info!("Elapsed time: {} s", end_time-start_time);
}
