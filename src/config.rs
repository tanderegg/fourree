use log::LogLevelFilter;
use getopts::Options;

use logger::init_logger;

const NUM_ROWS_DEFAULT: u64 = 1000;
const BATCH_SIZE_DEFAULT: u64 = 100;
const MAX_THREADS: u64 = 128;

#[derive(Clone, Copy, PartialEq)]
pub enum OutputMode {
    None,
    Stdout,
    File,
    PostgreSQL,
    S3
}

#[derive(Clone, Copy, PartialEq)]
pub enum LogType {
    Console,
    File
}

pub struct Config {
    pub num_rows: u64,
    pub batch_size: u64,
    pub log_type: LogType,
    pub num_threads: u64,
    pub output_mode: OutputMode,
    pub input_file: String,
    pub output_file: Option<String>
}

/// Prints the command line usage options
fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} FILE [options]", program);
    print!("{}\n", opts.usage(&brief));
}

pub fn load(args: Vec<String>) -> Result<Config, String> {
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("n", "num_rows", "specify number of records to generate", "NUM_ROWS");
    opts.optopt("b", "batch_size", "specify the size of each batch to be processed", "BATCH_SIZE");
    opts.optopt("l", "log_file", "specify a file to write the log to", "LOG_FILE_PATH");
    opts.optopt("t", "threads", "specify the number of threads to use (default: 1)", "NUM_THREADS");
    opts.optopt("o", "output", "specify the desired output (default: stdout)", "OUTPUT");
    opts.optopt("f", "output_file", "specify the file to output to, when in file output mode", "OUTPUT_FILE");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(error) => {
            print_usage(&program, opts);
            return Err(format!("{}", error));
        }
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
        return Err("".to_string());
    }

    // Determine input file, quit if none given
    let input_file = if !matches.free.is_empty() {
        matches.free[0].clone()
    } else {
        print_usage(&program, opts);
        return Err("An input file must be provided.".to_string());
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
                    return Err("To use stdout as the output destination, you must enable logging to file with the '-l' option.".to_string());
                }
                OutputMode::Stdout
            }
            "file"       => {
                OutputMode::File
            },
            "postgresql" => {
                OutputMode::PostgreSQL
            },
            "s3" => {
                OutputMode::S3
            },
            _ => {
                warn!("Unupported output requested: {}, defaulting to 'None'", output_opt);
                OutputMode::None
            }
        }
    } else {
        OutputMode::Stdout
    };

    let output_file = if output_mode == OutputMode::File && matches.opt_present("f"){
        let output_file_opt = matches.opt_str("f").unwrap().trim().to_string();
        Some(output_file_opt)
    } else {
        None
    };

    let num_batches = num_rows / batch_size;
    if num_batches % num_threads != 0 {
        return Err("Number of batches must be evenly divisible by number of threads.".to_string())
    }

    Ok(Config {
        num_rows: num_rows,
        num_threads: num_threads,
        log_type: log_type,
        output_mode: output_mode,
        batch_size: batch_size,
        input_file: input_file,
        output_file: output_file
    })
}
