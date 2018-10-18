extern crate pad;
extern crate rand;
extern crate serde;
extern crate serde_json;
extern crate time;
extern crate getopts;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate reqwest;

#[macro_use]
extern crate log;

pub mod generators;
pub mod json;
pub mod schema;
pub mod logger;
pub mod util;
pub mod config;

/// Macro for taking the result of many generators and building a string
///
/// # Examples
///
/// let row = gen_row![
///     "\t";
///     generate_integer(&mut rng, 0, 10000),
///     ...
/// ];
#[macro_export]
macro_rules! gen_row {
    ( $deliminator:expr; $( $generator:expr ),+ ) => {{
        let result = [
        $(
            $generator.to_string(),
        )+
        ];
        result.join($deliminator)
    }}
}
