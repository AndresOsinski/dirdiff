use std::env;
use std::error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::process::exit;
use std::time::{UNIX_EPOCH, Duration, SystemTime};

#[macro_use]
use serde::{Deserialize, Serialize};
extern crate serde_millis;

use chrono::NaiveDateTime;
use csv::{Reader, ReaderBuilder, Writer, WriterBuilder};
use hex;
use rusqlite::{params, Connection, Result as SqlResult};
use sha1::{Digest, Sha1};
use walkdir::{DirEntry, WalkDir};

#[derive(Deserialize, Serialize)]
struct Doc {
    hash: String,
    name: String,
    path: String,
    #[serde(with = "serde_millis")]
    mod_date: SystemTime
}

fn help() {
    println!("Usage: 
record [path] - The directory path in which to track file changes
compare_local [path] - Compare current changes with the last revision
compare [path] [remote_host] [remote_directory] - Compare and track file changes");
}

fn create_csv_writer(path: &String) -> csv::Result<Writer<File>> {
    let path = path.to_owned() + ".dirdiff.csv";
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)?;
    println!("Creating CSV {}", &path);
    Ok(WriterBuilder::new()
       .has_headers(false)
       .from_writer(file))
}

fn create_csv_reader(path: &String) -> Result<Reader<File>, csv::Error> {
    let path = path.to_owned() + ".dirdiff.csv";
    //let file = OpenOptions::new().open(&path)?;
    println!("Opening CSV {}", &path);
    ReaderBuilder::new()
       .has_headers(false)
       .from_path(path)
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

fn load_csv_entries(mut reader: Reader<File>) -> Vec<Doc> {
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

    results
}


fn make_local_sqlite() -> Connection {
    Connection::open_in_memory().expect("Cannot create in-memory SQLite")
}

fn load_to_local_sqlite(conn: &mut Connection, entries: Vec<Doc>) -> SqlResult<()> {
    &conn.execute("CREATE TABLE dir_entries (
        id  INTEGER PRIMARY KEY,
        hash TEXT NOT NULL,
        name TEXT NOT NULL,
        path TEXT NOT NULL,
        mod_date INTEGER)", params![])?;

    {
        let mut stmt = conn.prepare("INSERT INTO dir_entries (hash, name, path, mod_date) VALUES (?1, ?2, ?3, ?4)").unwrap();

        for entry in entries {
            let start_epoch = entry.mod_date.duration_since(UNIX_EPOCH).expect("Date oopsie").as_secs();
            stmt.execute(&[entry.hash, entry.name, entry.path, start_epoch.to_string()]).unwrap();
        }
    }

    Ok(())
}

fn list_revisions(conn: &Connection) -> Vec<NaiveDateTime> {
    let revisions_sql = "SELECT DISTINCT mod_date  FROM dir_entries";
    let stmt = &mut conn.prepare(revisions_sql).expect("Oopsie when getting revision dates");
    let revisions = stmt.query_map(params![], |row| {
        let val: i64 = row.get(0).unwrap();
        Ok(val)
    }).unwrap().filter_map(Result::ok).map(|e| NaiveDateTime::from_timestamp(e, 0)).collect::<Vec<NaiveDateTime>>();
    println!("Found the following revision dates: {:?}", revisions);

    revisions
}

fn compare_local(conn: &Connection) -> () {
    list_revisions(&conn);
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let args: Vec<String> = env::args().collect();  

    match args.len() {
        3 => { 
            let command = &args[1];

            match command.as_str() {
                "record" => {
                    let root = &args[2];

                    match gen_dir_struct(&root) {
                        Err(error) => 
                            println!("Messed up here: {}", &error),
                        Ok(dir_entries) => {
                            let mut writer = create_csv_writer(&root).expect("Error creating CSV writer");
                            for entry in dir_entries {
                                writer.serialize(entry).expect("Error writing CSV record");
                            }
                            writer.flush().unwrap();
                        }
                    }
                },
                "compare_local" => {
                    let root = &args[2];

                    let mut conn = make_local_sqlite();
                    let reader = create_csv_reader(root)?;
                    let entries = load_csv_entries(reader);
                    load_to_local_sqlite(&mut conn, entries)?;
                    let revisions = list_revisions(&conn);
                    compare_local(&mut conn);
                },
                _ => { help(); }
            }

        }
        _ => { help(); }
    }

    Ok(())
}
