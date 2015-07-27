extern crate fourree;
extern crate rand;

use fourree::generators::*;

fn main() {
    // Create our generator
    let mut rng = rand::thread_rng();

    // Generate a random int
    let my_int = generate_integer(&mut rng, 0, 1000000);
    println!("Random int: {}", my_int);

    // Generate a random string
    let my_string = generate_string(&mut rng, 10);
    println!("Random string: {}", my_string);

    // Generate a random integer from a Gaussian distribution
    let my_gaussian = generate_gauss(&mut rng, 10.0, 2.0);
    println!("Random gaussian: {}", my_gaussian);

    // Generate a date
    let my_date = generate_date(&mut rng);
    println!("Random date: {}", my_date.to_string());

    // Generate a choice from an array
    let my_vector = vec!["01", "02"];
    let my_choice = generate_choice(&mut rng, &my_vector);
    println!("Random choice from {:?}: {}", my_vector, my_choice);
}
