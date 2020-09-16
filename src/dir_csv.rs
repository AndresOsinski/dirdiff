use crate::docs::Doc;
use std::io;
use std::io::prelude::*;
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::process::exit;
use std::time::{UNIX_EPOCH, Duration, SystemTime};

use csv::{Position, Reader, ReaderBuilder, Writer, WriterBuilder};
use sha1::{Digest, Sha1};

use walkdir::{DirEntry, WalkDir};
use std::borrow::BorrowMut;

pub fn create_csv_writer(path: &Path, verbose: bool) -> csv::Result<Writer<File>> {
    let path = path.join(".dirdiff.csv");
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)?;

    if verbose { println!("Creating CSV {}", &path.display()); }
    Ok(WriterBuilder::new()
        .has_headers(false)
        .from_writer(file))
}

pub fn create_csv_reader(path: &Path, verbose: bool) -> Result<Reader<File>, csv::Error> {
    let path = path.join(".dirdiff.csv");
    //let file = OpenOptions::new().open(&path)?;

    if verbose { println!("Opening CSV {}", &path.display()); }

    ReaderBuilder::new()
        .has_headers(false)
        .from_path(path)
}

pub fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

pub fn gen_dir_struct(path: &Path) -> io::Result<Vec<Doc>> {
    let mut dir_entries = Vec::new();

    let mod_date = SystemTime::now();

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
                path: String::from(path.parent().unwrap().to_str().unwrap()),
                mod_date: mod_date
            });

        } else {
            println!("Weird directory name");
        }
    }

    Ok(dir_entries)
}

pub fn load_csv_entries(mut reader: Reader<File>, verbose: bool, debug: bool) -> Vec<Doc> {
    // reader.into_deserialize().map(|e| { let record: Doc = e.expect("Cannot parse CSV record"); record }).collect::<Vec<Doc>>()
    let mut results = Vec::new();
    for record in reader.records() {
        let record = record.unwrap();
        let record = Doc {
            hash: record[0].to_string(),
            name: record[1].to_string(),
            path: record[2].to_string(),
            mod_date: UNIX_EPOCH + (Duration::from_millis(record[3].to_string().parse::<u64>().unwrap()))
        };
        results.push(record);
    }

    if debug { println!("Loaded {} CSV records", results.len()); }

    results
}

// Same as `load_csv_entries` but only loads the last revision
pub fn load_csv_latest_entries(mut reader: Reader<File>, verbose:bool, debug: bool) -> Vec<Doc> {
    let mut millis: Vec<SystemTime> = Vec::new();
    for item in reader.records() {
        let item = item.unwrap();
        millis.push(UNIX_EPOCH + Duration::from_millis(item[3].parse::<u64>().unwrap()));
    }

    millis.sort();

    let latest_rev = *millis.last().expect("Could not get last revision date");

    if debug {
        println!("Latest revision: {:?}, from {} records", latest_rev, millis.len());
    }

    let mut results = Vec::new();

    reader.seek(Position::new());

    for record in reader.records() {
        let record = record.unwrap();
        let mod_date = UNIX_EPOCH + (
            Duration::from_millis(
                record[3].to_string().parse::<u64>().unwrap()));

        if mod_date == latest_rev {
            let record = Doc {
                hash: record[0].to_string(),
                name: record[1].to_string(),
                path: record[2].to_string(),
                mod_date: mod_date
            };

            results.push(record);
        } else if verbose {
            println!("Skipped record with mod date {:?}", UNIX_EPOCH + Duration::from_millis(record[3].parse::<u64>().unwrap()))
        }
    }

    if debug { println!("Loaded {} CSV records", results.len()); }

    results
}

