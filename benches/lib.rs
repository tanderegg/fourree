#![feature(test)]

#![allow(non_upper_case_globals)]
extern crate test;
extern crate rand;

#[macro_use]
extern crate fourree;

use std::thread;
use std::sync::Arc;
use std::sync::mpsc::{channel};
use std::io::Write;
use std::io::BufWriter;
use std::fs;
use std::fs::File;

use test::Bencher;

use fourree::generators::*;
use fourree::json::load_schema_from_file;

static choices: [&'static str; 108] = ["00", "01", "02", "03", "04", "05", "06", "07", "08",
                                       "09", "10", "11", "12", "13", "14", "15", "16", "17",
                                       "18", "19", "20", "21", "22", "23", "24", "25", "26",
                                       "27", "28", "29", "30", "31", "32", "33", "34", "35",
                                       "36", "37", "38", "39", "40", "41", "42", "43", "44",
                                       "45", "46", "47", "48", "49", "50", "51", "52", "53",
                                       "54", "55", "56", "57", "58", "59", "60", "61", "62",
                                       "63", "64", "65", "66", "67", "68", "69", "70", "71",
                                       "72", "73", "74", "75", "76", "77", "78", "79", "80",
                                       "81", "82", "83", "84", "85", "86", "87", "88", "89",
                                       "90", "91", "92", "93", "94", "95", "96", "97", "98",
                                       "99", "AA", "BB", "CC", "DD", "EE", "FF", "GG", "HH"];

fn gen_simple_row<R: rand::Rng>(rng: &mut R) -> String {
    gen_row![
        "\t";
        generate_integer(rng, 0, 100000),
        generate_gauss(rng, 10000, 1000),
        generate_gauss_f32(rng, 10000.0, 1000.0),
        generate_string(rng, 64),
        generate_date(rng),
        generate_choice(rng, &choices, 2, 2)
    ]
}

fn gen_complex_row<R: rand::Rng>(rng: &mut R) -> String {
    gen_row![
        "\t";
        generate_gauss(rng, 4000, 1000),
        generate_gauss_f32(rng, 4000.0, 1000.0),
        generate_integer(rng, 0, 1000000),
        generate_choice(rng, &choices, 2, 2),
        generate_string(rng, 32),
        generate_integer(rng, 0, 1000000),
        generate_integer(rng, 0, 1000000),
        generate_integer(rng, 0, 1000000),
        generate_integer(rng, 0, 1000000),
        generate_string(rng, 32),
        generate_date(rng),
        generate_choice(rng, &choices, 2, 48),
        generate_date(rng),
        generate_gauss(rng, 4000, 1000),
        generate_date(rng),
        generate_integer(rng, 0, 1000000),
        generate_choice(rng, &choices, 2, 2),
        generate_integer(rng, 0, 1000000),
        generate_gauss(rng, 4000, 1000),
        generate_gauss(rng, 4000, 1000),
        generate_date(rng),
        generate_integer(rng, 0, 1000000),
        generate_date(rng),
        generate_date(rng),
        generate_date(rng),
        generate_choice(rng, &choices, 2, 2),
        generate_string(rng, 32),
        generate_choice(rng, &choices, 2, 2),
        generate_choice(rng, &choices, 2, 2),
        generate_choice(rng, &choices, 2, 64),
        generate_integer(rng, 0, 1000000),
        generate_integer(rng, 0, 1000000),
        generate_integer(rng, 0, 1000000),
        generate_choice(rng, &choices, 2, 2),
        generate_string(rng, 32),
        generate_integer(rng, 0, 1000000),
        generate_integer(rng, 0, 1000000),
        generate_string(rng, 32),
        generate_string(rng, 64),
        generate_choice(rng, &choices, 2, 2),
        generate_choice(rng, &choices, 2, 2),
        generate_string(rng, 32),
        generate_date(rng),
        generate_string(rng, 32),
        generate_date(rng),
        generate_string(rng, 32),
        generate_choice(rng, &choices, 2, 2),
        generate_string(rng, 64),
        generate_choice(rng, &choices, 2, 2),
        generate_choice(rng, &choices, 2, 2),
        generate_date(rng),
        generate_string(rng, 128)
    ]
}

#[bench]
fn bench_generate_integer(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    b.iter(|| { generate_integer(&mut rng, 0, 1000000).to_string(); });
}

#[bench]
fn bench_generate_string(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    b.iter(|| { generate_string(&mut rng, 25); });
}

#[bench]
fn bench_generate_gauss(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    b.iter(|| { generate_gauss(&mut rng, 100, 20).to_string(); });
}

#[bench]
fn bench_generate_gauss_f32(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    b.iter(|| { generate_gauss_f32(&mut rng, 100.0, 20.0).to_string(); });
}

#[bench]
fn bench_generate_date(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    b.iter(|| { generate_date(&mut rng).to_string(); });
}

#[bench]
fn bench_generate_choice(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    let ex_choices = vec!["X", "A", "H", "B", "C", "D", "E", "F", "G"];
    b.iter(|| { generate_choice(&mut rng, ex_choices.as_slice(), 2, 2).to_string(); });
}

#[bench]
fn bench_generate_simple_row(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    b.iter(||
        gen_simple_row(&mut rng)
    );
}

#[bench]
fn bench_generate_complex_row(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    b.iter(||
        gen_complex_row(&mut rng)
    );
}

#[bench]
fn bench_generate_1000_complex_rows(b: &mut Bencher) {
    let mut rng = rand::thread_rng();
    b.iter(|| {
        let mut result = String::new();
        for i in 0..1000 {
            result = result + &gen_complex_row(&mut rng);
            if i < 999 {
                result.push('\n');
            }
        }
    });
}

#[bench]
fn bench_generate_simple_row_from_file(b: &mut Bencher) {
    let schema = load_schema_from_file("benches/example.json").ok().unwrap();
    let mut rng = rand::thread_rng();

    b.iter(|| {
        let file = File::create("/tmp/fourree-bench-tmp").unwrap();
        let mut writer = BufWriter::new(file);
        let rows = schema.generate_row(&mut rng).unwrap();
        writer.write(rows.as_bytes()).unwrap();
        fs::remove_file("/tmp/fourree-bench-tmp").unwrap();
    });
}

#[bench]
fn bench_generate_complex_row_from_file(b: &mut Bencher) {
    let schema = load_schema_from_file("benches/complex.json").ok().unwrap();
    let mut rng = rand::thread_rng();

    b.iter(|| {
        let file = File::create("/tmp/fourree-bench-tmp").unwrap();
        {
            let mut writer = BufWriter::new(file);
            let rows = schema.generate_row(&mut rng).unwrap();
            writer.write(rows.as_bytes()).unwrap();
        }
        fs::remove_file("/tmp/fourree-bench-tmp").unwrap();
    });
}

#[bench]
fn bench_generate_1000_complex_rows_from_file(b: &mut Bencher) {
    let schema = load_schema_from_file("benches/complex.json").ok().unwrap();
    let mut rng = rand::thread_rng();

    b.iter(|| {
        let file = File::create("/tmp/fourree-bench-tmp").unwrap();
        {
            let mut writer = BufWriter::new(file);
            let rows = schema.generate_rows(&mut rng, 1000).unwrap();
            writer.write(rows.as_bytes()).unwrap();
        }
        fs::remove_file("/tmp/fourree-bench-tmp").unwrap();
    });
}

#[bench]
fn bench_generate_1000_complex_rows_threaded(b: &mut Bencher) {
    let schema = load_schema_from_file("benches/complex.json").ok().unwrap();
    let schema_ref = Arc::new(schema);

    b.iter(|| {
        let file = File::create("/tmp/fourree-bench-tmp").unwrap();

        {
            let mut writer = BufWriter::new(file);
            let mut handles = Vec::new();
            let (sender, receiver) = channel();
            let output_thread = thread::spawn(move || {
                loop {
                    let output: String = match receiver.recv() {
                        Ok(message) => {
                            message
                        }
                        Err(_) => {
                            break;
                        }
                    };
                    writer.write(output.as_bytes()).unwrap();
                }
            });

            for _ in 1..4 {
                let thread_schema = schema_ref.clone();
                let thread_channel = sender.clone();
                handles.push(thread::spawn(move || {
                    let mut rng = rand::thread_rng();

                    // Use caluclated number of batches to run per thread
                    let rows = thread_schema.generate_rows(&mut rng, 250).unwrap();
                    thread_channel.send(rows).unwrap();
                }));
            }

            drop(sender);

            // Wait for generator threads to complete
            for handle in handles {
                //let name = handle.thread().name().unwrap();
                handle.join().unwrap();
                print!("Thread completed.");
            }

            output_thread.join().unwrap();
        }
        fs::remove_file("/tmp/fourree-bench-tmp").unwrap();
    })
}
