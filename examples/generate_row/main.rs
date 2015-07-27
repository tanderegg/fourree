#[macro_use]
extern crate fourree;
extern crate rand;

use fourree::generators::*;

static CHOICES: [&'static str; 7] = ["A", "B", "C", "D", "E", "F", "G"];

fn main() {
    // Create our generator
    let mut rng = rand::thread_rng();

    let row = gen_row![
        ", ";
        generate_gauss(&mut rng, 1000.0, 300.0),
        generate_date(&mut rng),
        generate_gauss(&mut rng, 1000.0, 100.0),
        generate_gauss(&mut rng, 4000.0, 1000.0),
        generate_integer(&mut rng, 0, 100000),
        generate_string(&mut rng, 10),
        generate_date(&mut rng),
        generate_choice(&mut rng, &CHOICES),
        generate_date(&mut rng),
        generate_integer(&mut rng, 0, 100),
        generate_integer(&mut rng, 0, 1000000),
        generate_integer(&mut rng, 0, 1000)
    ];

    println!("\nFake Data");
    println!("=========\n");
    println!("Row: {}\n", row);
}
