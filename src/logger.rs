use std::fs::File;
use std::io::{self, Write};

use log;
use log::{LogRecord, LogLevel, LogLevelFilter, LogMetadata, SetLoggerError};

pub enum LoggerError {
    Io(io::Error),
    SetLogger(SetLoggerError)
}

impl From<io::Error> for LoggerError {
    fn from(err: io::Error) -> LoggerError {
        LoggerError::Io(err)
    }
}

impl From<SetLoggerError> for LoggerError {
    fn from(err: SetLoggerError) -> LoggerError {
        LoggerError::SetLogger(err)
    }
}

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
    pub fn new(p: String) -> Result<FileLogger, io::Error> {
        let file = File::create(p.clone())?;

        Ok(FileLogger {
            handle: file
        })
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

pub fn init_logger(level: LogLevelFilter, log_path: Option<String>) -> Result<(), LoggerError> {
    let logger: Box<log::Log> = match log_path {
        Some(ref path) => {
            Box::new(FileLogger::new(path.clone())?)
        },
        None => Box::new(ConsoleLogger)
    };

    Ok(log::set_logger(|l| {
        l.set(level.clone());
        logger
    })?)
}
