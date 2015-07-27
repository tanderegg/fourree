#![feature(str_char)]
#![feature(convert)]

extern crate rand;
extern crate serde;

pub mod generators;
pub mod json;
pub mod schema;

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
