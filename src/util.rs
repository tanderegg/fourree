use time;
use rand;
use std::io;
use std::io::Write;
use std::io::BufWriter;
use std::fs::File;
use std::default::Default;

use rusoto_core::Region;
use rusoto_s3::{S3, S3Client, CreateMultipartUploadRequest, UploadPartRequest,
                CompletedPart, StreamingBody, CompleteMultipartUploadRequest,
                CompletedMultipartUpload, AbortMultipartUploadRequest};

use std::thread;
use std::thread::JoinHandle;
use std::sync::Arc;
use std::sync::mpsc::{channel, Sender, Receiver};

use config::{Config, OutputMode};
use schema::Schema;

/// Creates the thread used to write data to the output (file, database, stdout, etc.)
pub fn initialize_output_thread(config: &Config) ->
        Result<(Sender<String>, JoinHandle<()>), String> {
    let (sender, receiver) = channel();

    let thread = match config.output_mode {
        OutputMode::Stdout => stdout_thread(receiver)?,
        OutputMode::File => file_thread(config, receiver)?,
        OutputMode::PostgreSQL => {
            return Err("PostgreSQL output not yet implemented!".to_string())
        },
        OutputMode::S3 => s3_thread(config, receiver)?,
        OutputMode::None => {
            return Err("An invalid output mode was specified.".to_string())
        }
    };

    Ok((sender, thread))
}

/// Returns a thread that outputs to Stdout
pub fn stdout_thread(receiver: Receiver<String>) -> Result<JoinHandle<()>, String> {
    let thread = thread::spawn(move || {
        let stdout = io::stdout();
        let mut stdout_lock = stdout.lock();

        loop {
            let output = match receiver.recv() {
                Ok(message) => {
                    message
                }
                Err(_) => {
                    info!("Schema generation complete.");
                    break;
                }
            };

            write!(stdout_lock, "{}", output).unwrap();
        }
    });
    Ok(thread)
}

/// Returns a thread that outputs to a file
pub fn file_thread(config: &Config, receiver: Receiver<String>) -> Result<JoinHandle<()>, String> {
    let output_file = match config.output_file.clone() {
        Some(f) => f,
        None => return Err("output_file required when OutputMode == File!".to_string())
    };

    Ok(thread::spawn(move || {
        let file = File::create(output_file).unwrap();
        let mut writer = BufWriter::new(file);

        loop {
            let output: String = match receiver.recv() {
                Ok(message) => {
                    message
                }
                Err(_) => {
                    info!("Schema generation complete.");
                    break;
                }
            };

            // Panic will be caught when main attempts to join()
            writer.write(output.as_bytes()).unwrap();
        }
    }))
}

/// Returns a thread that outputs to an S3 bucket
pub fn s3_thread(config: &Config, receiver: Receiver<String>) -> Result<JoinHandle<()>, String> {
    let output_location = match config.output_file.clone() {
        Some(f) => f,
        None => return Err("output_file required when OutputMode == S3!".to_string())
    };

    let split_location: Vec<&str> = output_location.split(':').collect();

    if split_location.len() < 2 {
        return Err(
            "output_file must follow the format bucket:path when OutputMode == S3!".to_string()
        );
    }

    let bucket = split_location[0].to_string();
    let output_file = split_location[1].to_string();

    // Initiate multipart upload process
    let client = S3Client::new(Region::UsEast1);
    let create_multipart_req = CreateMultipartUploadRequest {
        bucket: bucket.to_owned(),
        key: output_file.to_owned(),
        ..Default::default()
    };

    info!("Initiating multipart S3 upload.");
    let response = match client.create_multipart_upload(create_multipart_req).sync() {
        Ok(r) => r,
        Err(e) => return Err(format!("{:#?}", e))
    };

    debug!("{:#?}", response);
    let upload_id = match response.upload_id {
        Some(id) => id,
        None => return Err("No UploadID returned from S3!".to_string())
    };

    Ok(thread::spawn(move || {
        let mut part_number = 1;
        let mut data = String::new();
        let mut completed_parts = Vec::new();

        loop {
            let message: String = match receiver.recv() {
                Ok(message) => {
                    message
                }
                Err(_) => {
                    "done".to_string()
                }
            };

            if &message != "done" {
                data.push_str(&message);
            }

            if data.len() > 5242880 || &message == "done" {
                info!("Writing part to S3...");

                let byte_data = data.clone().into_bytes();
                data.clear();

                let create_upload_part = UploadPartRequest {
                    body: Some(StreamingBody::from(byte_data)),
                    bucket: bucket.to_owned(),
                    key: output_file.to_owned(),
                    upload_id: upload_id.to_owned(),
                    part_number: part_number,
                    ..Default::default()
                };

                let response = match client.upload_part(create_upload_part).sync() {
                    Ok(r) => r,
                    Err(error) => {
                        info!("Multipart upload failed, aborting...");
                        abort_s3_upload(&client, &bucket, &output_file, &upload_id);
                        panic!(error)
                    }
                };

                debug!("{:#?}", response);
                completed_parts.push(CompletedPart {
                    e_tag: response.e_tag.clone(),
                    part_number: Some(part_number)
                });

                part_number += 1;
            }

            if &message == "done" {
                break;
            }
        }

        info!("Completing multipart upload...");
        let completed_upload = CompletedMultipartUpload { parts: Some(completed_parts) };

        let complete_req = CompleteMultipartUploadRequest {
            bucket: bucket.to_owned(),
            key: output_file.to_owned(),
            upload_id: upload_id.to_owned(),
            multipart_upload: Some(completed_upload),
            ..Default::default()
        };

        match client.complete_multipart_upload(complete_req).sync() {
            Ok(r) => {
                debug!("{:#?}", r);
                info!("Multipart upload completed.");
            },
            Err(error) => {
                info!("Multipart upload failed, aborting...");
                abort_s3_upload(&client, &bucket, &output_file, &upload_id);
                panic!(error)
            }
        };
    }))
}

pub fn abort_s3_upload(client: &S3Client, bucket: &String, key: &String, upload_id: &String) {
    let abort_multipart_upload_req = AbortMultipartUploadRequest {
        bucket: bucket.to_owned(),
        key: key.to_owned(),
        upload_id: upload_id.to_owned(),
        ..Default::default()
    };

    match client.abort_multipart_upload(abort_multipart_upload_req).sync() {
        Ok(r) => {
            debug!("{:#?}", r);
            info!("Multipart upload aborted.");
        },
        Err(error) => {
            error!("{}", error);
            info!("Failed to abort upload, please abort via S3 API.");
        }
    }
}


/// Generates a batch of data based on the provided parameters.
pub fn generate_batch(schema: &Schema, batch_size: u64,
                  channel: &Sender<String>, rng: &mut rand::ThreadRng) {
    let batch_start = time::precise_time_s();
    let rows = schema.generate_rows(rng, batch_size).unwrap();
    channel.send(rows).unwrap();
    let batch_elapsed = time::precise_time_s();
    info!("{} rows proccessed, {} s elapsed", batch_size, batch_elapsed-batch_start);
}

/// Generate data from a schema
pub fn generate_data(config: &Config, schema: Schema) -> Result<(), String> {
    // Define output_thread out of scope, so it will live beyond the data generation threads
    // and the output_channel.
    let output_thread;
    {
        let (output_channel, ot) = initialize_output_thread(config)?;
        output_thread = ot;

        let num_batches = config.num_rows / config.batch_size;
        let batch_size = config.batch_size;
        let batches_per_thread = num_batches / config.num_threads;

        if config.display_header {
            output_channel.send(schema.generate_header()).unwrap();
        }

        if config.num_threads > 1 {
            // Prepare for multithreading
            let mut handles = Vec::with_capacity(config.num_threads as usize);
            let schema_ref = Arc::new(schema);

            // Generate config.num_thread threads
            for _ in 0..config.num_threads {
                let thread_schema = schema_ref.clone();
                let thread_channel = output_channel.clone();
                handles.push(thread::spawn(move || {
                    let mut rng = rand::thread_rng();

                    // Use caluclated number of batches to run per thread
                    for _ in 0..batches_per_thread {
                        generate_batch(&thread_schema, batch_size, &thread_channel, &mut rng);
                    }
                }));
            }

            // Wait for generator threads to complete
            for handle in handles {
                match handle.join() {
                    Ok(_) => info!("Thread completed."),
                    Err(e) => return Err(format!("{:#?}", e))
                };
            }

            // output_channel goes out of scope here, thus causing the output thread to terminate
        } else {
            let mut rng = rand::thread_rng();

            for _ in 0..num_batches {
                generate_batch(&schema, config.batch_size, &output_channel, &mut rng);
            }
        }
    }

    // Now wait for output thread to complete
    match output_thread.join() {
        Ok(_) => {
            info!("Output thread completed.");
            Ok(())
        },
        Err(e) => Err(format!("{:#?}", e))
    }
}
