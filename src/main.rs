extern crate rand;
extern crate getopts;
extern crate time;
extern crate fourree;

#[macro_use]
extern crate log;

use std::env;
use std::thread;
use std::sync::Arc;
use std::sync::mpsc::{channel, Sender};
use getopts::Options;
use log::LogLevelFilter;

use fourree::json::{load_schema_from_file};
use fourree::logger::init_logger;

const NUM_ROWS_DEFAULT: u64 = 1000;
const BATCH_SIZE_DEFAULT: u64 = 100;
const MAX_THREADS: u64 = 128;

#[derive(Clone)]
enum OutputMode {
    None,
    Stdout,
    File,
    Postgresql
}

#[derive(PartialEq)]
enum LogType {
    Console,
    File
}

/// Prints the command line usage options
fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}\n", opts.usage(&brief));
}

/// Creates the thread used to write data to the output (file, database, stdout, etc.)
///
/// # Example:
/// ```
/// let (output_channel, output_thread) = initialize_output_thread(OutputMode::Stdout, None)
/// ```
fn initialize_output_thread(output_mode: OutputMode, _: Option<String>) ->
        (Sender<String>, std::thread::JoinHandle<()>) {
    let (sender, receiver) = channel();
    let thread = thread::spawn(move || {
        loop {
            let output = match receiver.recv() {
                Ok(message) => {
                    message
                }
                Err(_) => {
                    info!("Schema generation complete.");
                    break;
                }
            };

            match output_mode {
                OutputMode::Stdout => print!("{}", output),
                _ => ()
            }
        }
    });
    (sender, thread)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("n", "num_rows", "specify number of records to generate", "NUM_ROWS");
    opts.optopt("b", "batch_size", "specify the size of each batch to be processed", "BATCH_SIZE");
    opts.optopt("l", "log_file", "specify a file to write the log to", "LOG_FILE_PATH");
    opts.optopt("t", "threads", "specify the number of threads to use (default: 1)", "NUM_THREADS");
    opts.optopt("o", "output", "specify the desired output (default: stdout)", "OUTPUT");

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

    // Determine input file, quit if none given
    let input_file = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        print_usage(&program, opts);
        return;
    };

    // Setup logging
    let log_type = if matches.opt_present("l") {
        let file_path = matches.opt_str("l").unwrap().trim().to_string();
        init_logger(LogLevelFilter::Info, Some(file_path.clone())).ok().expect("Failed to initalize logger!");
        LogType::File
    } else {
        init_logger(LogLevelFilter::Info, Some("fourree.log".to_string())).ok().expect("Failed to initialize logger!");
        LogType::File
    };

    // Setup number of rows to produce
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

    // Set the batch size per flush to I/O
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

    // Setup number of threads to use for data generation
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

    // Set the output mode
    let output_mode = if matches.opt_present("o") {
        let output_opt = matches.opt_str("o").unwrap().trim().to_string();
        info!("Received option: output mode = {}", output_opt);
        match output_opt.as_ref() {
            "stdout"     => {
                if log_type == LogType::Console {
                    println!("ERROR: To use stdout as the output destination, you must enable logging to file with the '-l' option.");
                    return;
                }
                OutputMode::Stdout
            }
            "file"       => {
                warn!("File output is not yet implemented, will be no-op!");
                OutputMode::File
            },
            "postgresql" => {
                warn!("PostgreSQL output is not yet implemented, will be no-op!");
                OutputMode::Postgresql
            },
            _ => {
                warn!("Unupported output requested: {}, defaulting to 'None'", output_opt);
                OutputMode::None
            }
        }
    } else {
        OutputMode::Stdout
    };

    info!("Loading schema from: {:?}", input_file);

    let start_time = time::precise_time_s();

    // Load and generate the data, sending it to OutputMode
    let schema = match load_schema_from_file(&input_file) {
        Ok(s) => s,
        Err(err) => {
            error!("{}", err);
            return;
        }
    };

    // Define output_thread out of scope, so it will live beyond the data generation threads
    // and the output_channel.
    let output_thread;
    {
        let (output_channel, ot) = initialize_output_thread(output_mode, None);
        output_thread = ot;

        let num_batches = num_rows / batch_size;

        if num_threads > 1 {
            let batches_per_thread = num_batches / num_threads;
            let mut handles = Vec::with_capacity(num_threads as usize);
            let schema_ref = Arc::new(schema);

            for _ in 0..num_threads {
                let thread_schema = schema_ref.clone();
                let thread_channel = output_channel.clone();
                handles.push(thread::spawn(move || {
                    let mut rng = rand::thread_rng();

                    // Use caluclated number of batches to run per thread
                    for _ in 0..batches_per_thread.clone() {
                        let batch_start = time::precise_time_s();
                        let rows = thread_schema.generate_rows(&mut rng, batch_size.clone()).unwrap();
                        thread_channel.send(rows).unwrap();
                        let batch_elapsed = time::precise_time_s();
                        info!("{} rows proccessed, {} s elapsed", batch_size, batch_elapsed-batch_start);
                    }
                }));
            }

            // Wait for generator threads to complete
            for handle in handles {
                //let name = handle.thread().name().unwrap();
                handle.join().unwrap();
                info!("Thread completed.");
            }

            // output_channel goes out of scope here, thus causing the output thread to terminate
        } else {
            let mut rng = rand::thread_rng();

            for _ in 0..num_batches {
                let batch_start = time::precise_time_s();
                info!("Flushing output queue.");
                let rows = schema.generate_rows(&mut rng, batch_size).unwrap();
                output_channel.send(rows).unwrap();
                let batch_elapsed = time::precise_time_s();
                info!("{} rows proccessed, {} s elapsed", batch_size, batch_elapsed-batch_start);
            }
        }
    }

    // Now wait for output thread to complete
    output_thread.join().unwrap();
    info!("Output thread completed.");

    let end_time = time::precise_time_s();
    info!("Elapsed time: {} s", end_time-start_time);
}
