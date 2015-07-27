#![allow(dead_code)]

#[macro_use]
extern crate fourree;
extern crate rand;

use fourree::generators::*;

fn main() {
    // Create our generator
    let mut rng = rand::thread_rng();
    println!("{}", generate_integer(&mut rng, 0, 100000));
}
