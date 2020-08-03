use std::env;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::SystemTime;

#[macro_use]
use serde::Serialize;
extern crate serde_millis;

use csv::{Writer, WriterBuilder};
use hex;
use sha1::{Digest, Sha1};
use walkdir::{DirEntry, WalkDir};

#[derive(Serialize)]
struct Doc {
    hash: String,
    name: String,
    path: String,
    #[serde(with = "serde_millis")]
    mod_date: SystemTime
}

fn help() {
    println!("Usage: 
path - The directory path in which to track file changes");
}


fn create_csv_writer(path: &String) -> csv::Result<Writer<File>> {
    let path = path.to_owned() + ".dirdiff.csv";
    println!("Creating CSV {}", &path);
    WriterBuilder::new().has_headers(false).from_path(path)
}


fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with("."))
         .unwrap_or(false)
}

fn gen_dir_struct(path: &String) -> io::Result<Vec<Doc>> {
    let mut dir_entries = Vec::new();

    let mod_date = SystemTime::now();
    let path = Path::new(path);

    println!("Getting files from directory {}", &path.display());

    if !path.is_dir() {
        println!("Path must be a directory");
        exit(1);
    }

    for entry in WalkDir::new(path).into_iter().filter_entry(|e| !is_hidden(e)) {
        if let Ok(dir_entry) = entry {
            let path = &dir_entry.path();
            let dir_path = path.display();

            if dir_entry.metadata()?.file_type().is_dir() {
                println!("Entry {} is dir!", dir_path);
                continue
            }

            let mut data = Vec::new();

            let mut fd = File::open(dir_entry.path()).unwrap();

            fd.read_to_end(&mut data)?;

            let hash = Sha1::digest(&data);

            println!("{}", &dir_path);

            dir_entries.push(Doc {
                hash: hex::encode(hash),
                name: String::from(dir_entry.file_name().to_str().unwrap()),
                path: String::from(path.to_str().unwrap()),
                mod_date: mod_date
            });

        } else {
            println!("Weird directory name");
        }
    }

    Ok(dir_entries)
}

fn main() {
    let args: Vec<String> = env::args().collect();  

    match args.len() {
        2 => { 
            let root = &args[1];

            match gen_dir_struct(root) {
                Err(error) => 
                    println!("Messed up here: {}", &error),
                Ok(dir_entries) => {
                    let mut writer = create_csv_writer(root).expect("Error creating CSV writer");
                    for entry in dir_entries {
                        writer.serialize(entry).expect("Error writing CSV record");
                    }
                    writer.flush().unwrap();
                }
            }

        }
        _ => { help(); }
    }
}
