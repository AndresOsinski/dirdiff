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
use rusqlite::{NO_PARAMS, params, Connection, Result as SqlResult, Row};
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
                path: String::from(path.parent().unwrap().to_str().unwrap()),
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
            let start_epoch = entry.mod_date
                .duration_since(UNIX_EPOCH).expect("Date oopsie")
                .as_secs();
            stmt.execute(&[entry.hash, entry.name, entry.path, start_epoch.to_string()]).unwrap();
        }
    }

    Ok(())
}

// Get revision times in milliseconds
fn revision_millis(conn: &Connection) -> Vec<i64> {
    let revisions_sql = "SELECT DISTINCT mod_date  FROM dir_entries ORDER BY mod_date DESC";
    let stmt = &mut conn.prepare(revisions_sql).expect("Oopsie when getting revision dates");

    stmt.query_map(params![], |row| {
        let val: i64 = row.get(0).unwrap();
        Ok(val)
    }).unwrap().map(|e| e.unwrap()).collect()
}

fn list_revisions(rev_millis: Vec<i64>) -> Vec<NaiveDateTime> {
    let revisions = rev_millis.iter()
        .map(|e| NaiveDateTime::from_timestamp(*e, 0))
        .collect::<Vec<NaiveDateTime>>();
    println!("Found the following revision dates: {:?}", revisions);

    revisions
}

fn setup_working_table(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize> {
    conn.execute("CREATE TABLE working_entries (
        id  INTEGER PRIMARY KEY,
        hash TEXT NOT NULL,
        name TEXT NOT NULL,
        path TEXT NOT NULL,
        mod_date INTEGER)", params![])
}

// Files which do not exist in revision and do not match any of the previous criteria?
fn missing_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    Vec::new()
}

// No equivalent hash, doesn't match as a changed, moved or renamed file
fn new_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    Vec::new()
}

// Same name and path, different hash
fn changed_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    Vec::new()
}

// Same hash, different path
fn moved_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    let moved_sql = "SELECT * 
    FROM 
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path != w2.path AND w1.name = w2.name
    WHERE w1.mod_date = ?1 AND w2.mod_date = ?2";

    let mut stmt = conn.prepare(moved_sql).unwrap();
    let moved = stmt.query_map(params![latest.timestamp(), previous.timestamp()], |row| {
        Ok(Doc {
            hash: row.get_unwrap(1),
            name: row.get_unwrap(2),
            path: row.get_unwrap(3),
            //mod_date: NaiveDateTime::from_timestamp(row.get_unwrap::<usize, i64>(4), 0)
            mod_date: UNIX_EPOCH + (Duration::from_millis(row.get_unwrap::<usize, i64>(4) as u64))
        })
    }).unwrap().map(|i| i.unwrap()).collect();

    moved
}

fn remove_moved(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize> {
    let moved_sql = "DELETE FROM working_entries WHERE id IN (
        SELECT w1.id
        FROM 
            working_entries w1 INNER JOIN working_entries w2
            ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path != w2.path AND w1.name = w2.name
        WHERE w1.mod_date = ?1 AND w2.mod_date = ?2)";
    conn.execute(moved_sql, params![latest.timestamp(), previous.timestamp()])
}

// Same hash and path, different name
fn renamed_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    let renamed_sql = "SELECT * 
    FROM (
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name != w2.name AND w1.path = w2.path
        )
    WHERE w1.mod_date = ?1 and w2.mod_date = ?2";
    let mut stmt = conn.prepare(renamed_sql).unwrap();
    let renamed = stmt.query_map(params![latest.timestamp(), previous.timestamp()], |row| {
        Ok(Doc {
            hash: row.get_unwrap(1),
            name: row.get_unwrap(2),
            path: row.get_unwrap(3),
            //mod_date: NaiveDateTime::from_timestamp(row.get_unwrap::<usize, i64>(4), 0)
            mod_date: UNIX_EPOCH + (Duration::from_millis(row.get_unwrap::<usize, i64>(4) as u64))
        })
    }).unwrap().map(|i| i.unwrap()).collect();

    renamed
}

fn remove_renamed(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize>{
    // Yes, hacky
    let renamed_sql_1 = "DELETE FROM working_entries WHERE id IN (
        SELECT w1.id FROM (
            working_entries w1 INNER JOIN working_entries w2
            ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name != w2.name AND w1.path = w2.path
        ) WHERE w1.mod_date = ?1 and w2.mod_date = ?2
    )";
    conn.execute(renamed_sql_1, params![latest.timestamp(), previous.timestamp()])?;

    let renamed_sql_2 = "DELETE FROM working_entries WHERE id IN (
        SELECT w2.id FROM (
            working_entries w1 INNER JOIN working_entries w2
            ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name != w2.name AND w1.path = w2.path
        ) WHERE w1.mod_date = ?1 and w2.mod_date = ?2
    )";
    conn.execute(renamed_sql_2, params![latest.timestamp(), previous.timestamp()])
}

fn load_working_table(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> SqlResult<usize> {
    let load_sql = "INSERT INTO working_entries (id, hash, name, path, mod_date) 
    SELECT id, hash, name, path, mod_date
    FROM dir_entries
    WHERE mod_date IN (?1, ?2)";

    conn.execute(load_sql, params![latest.timestamp(), previous.timestamp()])
}

fn remove_unchanged_from_working_table(previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize> {
    let unchanged_sql = "DELETE FROM working_entries WHERE id IN (
        SELECT w1.id from working_entries w1 INNER JOIN working_entries w2 ON
        (w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name = w2.name AND w1.path = w2.path)
        WHERE w1.mod_date = ?1)";
    conn.execute(unchanged_sql, params![previous.timestamp()])
    
}

fn print_docs(docs: Vec<Doc>) -> () {
    for doc in docs {
        println!("{}, {}, {}", doc.name, doc.path, doc.hash);
    }
}

fn print_working_entries(conn: &mut Connection) -> () {
    let mut stmt = conn.prepare("SELECT * FROM working_entries").unwrap();
    let working_entries = stmt
        .query_map(NO_PARAMS, |row| {
            let doc: (i64, String, String, String, NaiveDateTime) = (
                row.get_unwrap::<usize, i64>(0),
                row.get_unwrap::<usize, String>(1),
                row.get_unwrap::<usize, String>(2),
                row.get_unwrap::<usize, String>(3),
                NaiveDateTime::from_timestamp(row.get_unwrap::<usize, i64>(4), 0)
            );
            Ok(doc)
        }).unwrap().map(|i| i.unwrap());

    for entry in working_entries {
        println!("{:?}", entry);
    }
}

fn compare_local(conn: &mut Connection) -> () {
    let revision_millis = revision_millis(conn);
    let revisions = list_revisions(revision_millis);
    let latest_revision = revisions[0];
    let prior_revision = revisions[1];

    println!("Latest revision at {}", &latest_revision);
    println!("Prior revision at {}", &prior_revision);

    setup_working_table(&latest_revision, &prior_revision, conn).expect("Could not create working table for revision comparison");
    let inserted = load_working_table(&latest_revision, &prior_revision, conn).expect("Could not load directory entries to working table");
    println!("Inserted {} records into working table", inserted);

    println!("Initial working records");
    print_working_entries(conn);

    remove_unchanged_from_working_table(&prior_revision, conn).expect("Could not remove unchanged directory entries from working table");

    println!("Remaining entries after removing unchanged");
    print_working_entries(conn);

    let renamed = renamed_files(&latest_revision, &prior_revision, conn);
    remove_renamed(&latest_revision, &prior_revision, conn).expect("Could not remove renamed entries from working table");
    println!("Renamed docs:");
    print_docs(renamed);

    println!("Remaining after removing renamed");
    print_working_entries(conn);

    let moved = moved_files(&latest_revision, &prior_revision, conn);
    println!("Moved files");
    print_docs(moved);

    remove_moved(&latest_revision, &prior_revision, conn);
    println!("Remaining after moved");
    print_working_entries(conn);

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
                    println!("Comparing latest revision with prior to check for changes.");
                    let root = &args[2];

                    let mut conn = make_local_sqlite();
                    let reader = create_csv_reader(root)?;
                    let entries = load_csv_entries(reader);
                    load_to_local_sqlite(&mut conn, entries)?;
                    compare_local(&mut conn);
                },
                _ => { help(); }
            }

        }
        _ => { help(); }
    }

    Ok(())
}
