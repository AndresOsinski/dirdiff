use std::env;
use std::error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::time::{UNIX_EPOCH, Duration, SystemTime};

use chrono::NaiveDateTime;
use clap::{Arg, App};
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
    revisions
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
    println!("\n");
}

fn compare_local(conn: &mut Connection, verbose: bool, debug: bool) -> () {
    let revision_millis = revision_millis(conn);

    let revisions = list_revisions(revision_millis);

    if debug { println!("Found the following revision dates: {:?}", revisions); }

    let latest_revision = revisions[0];
    let prior_revision = revisions[1];

    if verbose {
        println!("Latest revision at {}", &latest_revision);
        println!("Prior revision at {}", &prior_revision);
    }

    setup_working_tables(&latest_revision, &prior_revision, conn)
        .expect("Could not create working table for revision comparison");
    let inserted = load_working_table(&latest_revision, &prior_revision, conn)
        .expect("Could not load directory entries to working table");

    if  debug{
        println!("Inserted {} records into working table", inserted);

        println!("Initial working records:");
        print_working_entries(conn);
    }

    remove_unchanged_from_working_table(&prior_revision, conn)
        .expect("Could not remove unchanged directory entries from working table");

    if  debug{
        println!("Remaining entries after removing unchanged:");
        print_working_entries(conn);
    }

    let renamed = renamed_files(&latest_revision, &prior_revision, conn);
    remove_renamed(&latest_revision, &prior_revision, conn)
        .expect("Could not remove renamed entries from working table");

    println!("Renamed files:");
    print_docs(renamed);

    if debug{
        println!("Remaining after removing renamed:");
        print_working_entries(conn);
    }

    let moved = moved_files(&latest_revision, &prior_revision, conn);

    println!("Moved files:");
    print_docs(moved);

    remove_moved(&latest_revision, &prior_revision, conn);

    if debug {
        println!("Remaining after moved:");
        print_working_entries(conn);
    }

    let missing = missing_files(&latest_revision, &prior_revision, conn);

    println!("Missing files:");
    print_docs(missing);

    let added = added_files(&latest_revision, conn);

    println!("Added files:");
    print_docs(added);
}

fn main() -> Result<(), Box<dyn error::Error>> {
    let args = App::new("Dirdiff")
        .author("Andres Osinski <andres.osinski@gmail.com>")
        .about("Compare local and remote directory file revisions")
        .arg(Arg::with_name("v")
            .short('v')
            .about("Verbosity"))
        .arg(Arg::with_name("d")
            .short('d')
            .about("Debug"))
        .subcommand(App::new("record")
            .about("Record local directory revision")
            .arg(Arg::with_name("record_dir")
                .about("The directory to record revision for")
                .index(1)
                .required(true)))
        .subcommand(App::new("compare_local")
            .about("Compare the latest directory revision with the previous one")
            .arg(Arg::with_name("comp_dir")
                .about("The directory to compare revisions")
                .index(1)
                .required(true)
            ))
        .subcommand(App::new("compare_remote")
            .about("Compare the latest revisions of two different directories")
            .arg(Arg::with_name("local_directory")
                .index(1)
                .required(true))
            .arg(Arg::with_name("remote_host")
                .index(2)
                .required(true))
            .arg(Arg::with_name("remote_directory")
                .index(3)
                .required(true)))
        .get_matches();

    let verbose = args.value_of("v").is_some();
    let debug = args.value_of("d").is_some();

    if let Some(record) = args.subcommand_matches("record") {
        let root = Path::new(record.value_of_os("directory").unwrap());

        match gen_dir_struct(&root) {
            Err(error) =>
                println!("Messed up here: {}", &error),
            Ok(dir_entries) => {
                let mut writer = create_csv_writer(&root, verbose)
                    .expect("Error creating CSV writer");
                for entry in dir_entries {
                    writer.serialize(entry).expect("Error writing CSV record");
                }
                writer.flush().unwrap();
            }
        }
    } else if let Some(command) = args.subcommand_matches("compare_local") {
        if verbose {
            println!("Comparing latest revision with prior to check for changes.");
        }

        let root = Path::new(command.value_of_os("comp_dir").unwrap());

        let mut conn = make_local_sqlite();
        let reader = create_csv_reader(root, verbose)?;
        let entries = load_csv_entries(reader, verbose);
        load_to_local_sqlite(&mut conn, entries)?;
        compare_local(&mut conn, verbose, debug);
    }

    Ok(())
}
