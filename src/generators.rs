extern crate rand;
extern crate pad;

// use std::str::from_utf8;
use rand::Rng;
use rand::distributions::{IndependentSample, Range, Normal};
use self::pad::PadStr;

const UPPERCASE_CHARS: &'static str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Convenience struct for representing a date in the form MM/DD/YYYY
pub struct Date {
    day: u8,
    month: u8,
    year: u16
}

impl ToString for Date {
    fn to_string(&self) -> String {
        self.year.to_string() + "-" + &self.month.to_string().pad_to_width_with_char(2, '0') + "/" + &self.day.to_string().pad_to_width_with_char(2, '0')
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
    let mut result: String = String::with_capacity(length);

    for _ in 0..length {
        let index = dist.ind_sample(rng);
        result.push(UPPERCASE_CHARS.char_at(index as usize));
    }
    result

    // 1/15th the time, but generates any u8 character instead
    // of just capital letters.
    /*let mut bytes = Vec::with_capacity(length);
    rng.fill_bytes(&mut bytes);
    from_utf8(&bytes).unwrap().to_string()*/
}

/// Generates an integer from a normal (Gaussian) distribution
///
/// # Examples
///
/// let x = generate_gauss(&mut rng, 10, 2);
///
pub fn generate_gauss<R: rand::Rng>(rng: &mut R, mean: u64, std_dev: u64) -> u64 {
    let dist = Normal::new(mean as f64, std_dev as f64);
    dist.ind_sample(rng) as u64
}

/// Generates a date (as a string for now)
///
/// # Examples
///
/// let x = generate_date(&mut rng);
///
pub fn generate_date<R: rand::Rng>(rng: &mut R) -> Date {
    let mut bytes = [0u8; 3];
    rng.fill_bytes(&mut bytes);

    let month: u8 = ((bytes[0] as u16 * 12) / 255) as u8;
    let year: u16 = (((bytes[1] as u16 * 116) / 255) + 1900) as u16;

    let day_range = match month {
        1 | 3 | 5 | 7...8 | 10 | 12 => 31,
        2 => 28,
        _ => 30
    };

    let day = ((bytes[2] as u16 * day_range) / 255) as u8;

    Date {
        month: month as u8,
        day: day as u8,
        year: year as u16
    }
}

/// Generate a value from an array of chars
///
/// # Examples
///
/// let x = vec!["A", "B", "C"];
/// let y = generate_choice(&mut rng, &x);
///
pub fn generate_choice<R: rand::Rng, T: ToString>(rng: &mut R, choices: &[T], length: usize) -> String {
    let mut output = String::with_capacity(length);
    for _ in 0..length {
        output.push_str(&rng.choose(choices).unwrap().to_string());
    }
    output
}
