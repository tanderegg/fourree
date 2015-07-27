extern crate rand;

use rand::Rng;
use rand::distributions::{IndependentSample, Range, Normal};

const UPPERCASE_CHARS: &'static str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

pub struct Date {
    day: u32,
    month: u32,
    year: u32
}

impl ToString for Date {
    fn to_string(&self) -> String {
        self.month.to_string() + "/" + &self.day.to_string() + "/" + &self.year.to_string()
    }
}

/// Generates a random integer from min to max, inclusive
///
/// # Examples
///
/// let x = generate_integer(&mut rng, 0, 10000);
///
pub fn generate_integer<R: rand::Rng>(rng: &mut R, min: i64, max: i64) -> i64 {
    let dist = Range::new(min, max);
    dist.ind_sample(rng)
}

/// Generates a random string of 'length'.
/// Currently selects from the uppercase alphabet.
///
/// # Examples
///
/// let x = generate_string(&mut rng, 0, 25);
///
pub fn generate_string<R: rand::Rng>(rng: &mut R, length: usize) -> String {
    let dist = Range::new(0, 26);
    let mut result = String::new();

    for _ in 0..length {
        let index = dist.ind_sample(rng);
        result.push((UPPERCASE_CHARS.char_at(index as usize)));
    }

    result.to_string()
}

/// Generates an integer from a normal (Gaussian) distribution
///
/// # Examples
///
/// let x = generate_gauss(&mut rng, 10, 2);
pub fn generate_gauss<R: rand::Rng>(rng: &mut R, mean: f64, std_dev: f64) -> f64 {
    let dist = Normal::new(mean, std_dev);
    dist.ind_sample(rng)
}

/// Generates a date (as a string for now)
///
/// # Examples
///
/// let x = generate_date(&mut rng);
///
// TODO: Perhaps convert to generate a date as a single int in seconds,
// then convert to a proper date type using a crate for dates.
pub fn generate_date<R: rand::Rng>(rng: &mut R) -> Date {
    let year = generate_integer(rng, 1900, 2016);
    let month = generate_integer(rng, 1, 12);
    let day = match month {
        1 | 3 | 5 | 7...8 | 10 | 12 => generate_integer(rng, 1, 31),
        2 => generate_integer(rng, 1, 28),
        _ => generate_integer(rng, 1, 30)
    };
    Date {
        month: month as u32,
        day: day as u32,
        year: year as u32
    }
}

/// Generate a value from an array of chars
///
/// # Examples
///
/// let x = vec!["A", "B", "C"];
/// let y = generate_choice(&mut rng, &x);
///
pub fn generate_choice<'a, R: rand::Rng, T>(rng: &mut R, choices: &'a [T]) -> &'a T {
    &choices[generate_integer(rng, 0, (choices.len() as i64 - 1)) as usize]
}
