use std::env;
use std::error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::time::{UNIX_EPOCH, Duration, SystemTime};

use chrono::NaiveDateTime;
use csv::{Reader, ReaderBuilder, Writer, WriterBuilder};
use hex;
use rusqlite::{NO_PARAMS, params, Connection, Result as SqlResult, Row};

mod dir_csv;
mod docs;
mod db;

use crate::dir_csv::*;
use crate::docs::*;
use crate::db::*;

fn help() {
    println!("Usage: 
record [path] - The directory path in which to track file changes
compare_local [path] - Compare current changes with the last revision
compare [path] [remote_host] [remote_directory] - Compare and track file changes");
}


fn list_revisions(rev_millis: Vec<i64>) -> Vec<NaiveDateTime> {
    let revisions = rev_millis.iter()
        .map(|e| NaiveDateTime::from_timestamp(*e, 0))
        .collect::<Vec<NaiveDateTime>>();
    println!("Found the following revision dates: {:?}", revisions);

    revisions
}

// No equivalent hash, doesn't match as a changed, moved or renamed file
fn new_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    Vec::new()
}

// Same name and path, different hash
fn changed_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    Vec::new()
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

    setup_working_tables(&latest_revision, &prior_revision, conn).expect("Could not create working table for revision comparison");
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

    let missing = missing_files(&latest_revision, &prior_revision, conn);
    println!("Missing files");
    print_docs(missing);

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
