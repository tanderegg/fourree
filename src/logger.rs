use std::fs::File;
use std::io::Write;

use log;
use log::{LogRecord, LogLevel, LogLevelFilter, LogMetadata, SetLoggerError};

struct ConsoleLogger;

impl log::Log for ConsoleLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= LogLevel::Debug
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }
}

struct FileLogger {
    handle: File
}

impl FileLogger {
    pub fn new(p: String) -> FileLogger {
        let file = match File::create(p.clone()) {
            Ok(f) => f,
            Err(err) => panic!(err.to_string())
        };

        FileLogger {
            handle: file
        }
    }
}

impl log::Log for FileLogger {
    fn enabled(&self, metadata: &LogMetadata) -> bool {
        metadata.level() <= LogLevel::Debug
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            write!(&self.handle, "{} - {}\n", record.level(), record.args())
                   .ok()
                   .expect("Failed to write to log file!");
        }
    }
}

pub fn init_logger(level: LogLevelFilter, log_path: Option<String>) -> Result<(), SetLoggerError> {
    log::set_logger(|l| {
        l.set(level.clone());

        match log_path {
            Some(ref path) => Box::new(FileLogger::new(path.clone())),
            None => Box::new(ConsoleLogger)
        }
    })
}
