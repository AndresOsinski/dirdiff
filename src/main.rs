use std::env;
use std::error;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::time::{UNIX_EPOCH, Duration, SystemTime};

use chrono::NaiveDateTime;
use clap::{Arg, ArgMatches, App};
use csv::{Reader, ReaderBuilder, Writer, WriterBuilder};
use hex;
use rusqlite::{NO_PARAMS, params, Connection, Result as SqlResult, Row};

mod dir_csv;
mod docs;
mod db;

use crate::dir_csv::*;
use crate::docs::*;
use crate::db::*;

const RECORD:&str = "record";
const HISTORY:&str = "history";
const COMPARE_LOCAL:&str = "local";
const COMPARE_REMOTE:&str = "remote";

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

// Compare latest revision with prior revision of same local directory
fn history(conn: &mut Connection, verbose: bool, debug: bool) -> () {
    let revision_millis = revision_millis(conn);

    if debug { println!("Got the following revision millis from DB data: {:?}", revision_millis) ;}

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

        if inserted > 0 {
            let working_records = get_doclist_from_table("working_entries", conn);
            println!("Initial working records:");
            print_docs(working_records);
        }
    }

    remove_unchanged_from_working_table(&prior_revision, conn)
        .expect("Could not remove unchanged directory entries from working table");

    if  debug {
        println!("Remaining entries after removing unchanged:");
        db::print_working_entries(conn);
    }

    let renamed = renamed_files(&latest_revision, &prior_revision, conn);
    remove_renamed(&latest_revision, &prior_revision, conn)
        .expect("Could not remove renamed entries from working table");

    if renamed.len() > 0 {
        println!("Renamed files:");
        print_docs(renamed);
    } else {
        println!("No renamed filed");
    }

    if debug {
        println!("Remaining after removing renamed:");
        db::print_working_entries(conn);
    }

    let moved = moved_files(&latest_revision, &prior_revision, conn);

    if moved.len() > 0 {
        println!("Moved files:");
        print_moved_docs(moved);
    } else {
        println!("No moved files");
    }

    remove_moved(&latest_revision, &prior_revision, conn);

    if debug {
        println!("Remaining after moved:");
        db::print_working_entries(conn);
    }

    let missing = missing_files(&latest_revision, &prior_revision, conn);

    if missing.len() > 0 {
        println!("Missing files:");
        print_docs(missing);
    } else {
        println!("No missing files");
    }

    let added = added_files(&latest_revision, conn);

    if added.len() > 0 {
        println!("Added files:");
        print_docs(added);
    } else {
        println!("No added files");
    }
}

// Compare the latest revision of two different local directories
fn compare_directories(conn: &mut Connection, verbose: bool, debug: bool) -> () {}

fn setup_history(command: &ArgMatches,
        verbose: bool, debug: bool)  -> Result<(), Box<dyn error::Error>> {
    if verbose {
        println!("Comparing latest revision with prior to check for changes.");
    }

    let root = Path::new(command.value_of_os("comp_dir").unwrap());

    let mut conn = make_local_sqlite();
    let reader = create_csv_reader(root, verbose)?;
    let entries = load_csv_entries(reader, verbose, debug);

    create_dir_entries_table(&mut conn)?;
    load_to_local_sqlite(&mut conn, entries)?;
    history(&mut conn, verbose, debug);

    Ok(())
}

fn setup_compare_local(command: &ArgMatches, verbose: bool, debug: bool)
                       -> Result<(), Box<dyn error::Error>> {
    if verbose {
        println!("Compare the latest revision of directories");
    }

    let first = Path::new(command.value_of_os("first").unwrap());
    let first_reader = create_csv_reader(first, verbose)?;
    let first_entries = load_csv_latest_entries(first_reader, verbose, debug);

    let second = Path::new(command.value_of_os("second").unwrap());
    let second_reader = create_csv_reader(second, verbose)?;
    let second_entries = load_csv_latest_entries(second_reader, verbose, debug);

    let mut conn = make_local_sqlite();
    create_dir_entries_table(&mut conn)?;
    load_to_local_sqlite(&mut conn, first_entries)?;
    load_to_local_sqlite(&mut conn, second_entries)?;

    if debug { db::print_dir_entries(&mut conn); }

    history(&mut conn, verbose, debug);

    Ok(())
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
        .subcommand(App::new(RECORD)
            .about("Record local directory revision")
            .arg(Arg::with_name("directory")
                .about("The directory to record revision for")
                .index(1)
                .required(true)))
        .subcommand(App::new(HISTORY)
            .about("Compare the latest directory revision with the previous one")
            .arg(Arg::with_name("comp_dir")
                .about("The directory to compare revisions")
                .index(1)
                .required(true)
            ))
        .subcommand(App::new(COMPARE_LOCAL)
            .about("Compare two directories in this host")
            .arg(Arg::with_name("first")
                .index(1)
                .required(true))
            .arg(Arg::with_name("second")
                .index(2)
                .required(true)))
        .subcommand(App::new(COMPARE_REMOTE)
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

    let verbose = args.is_present("v");
    let debug = args.is_present("d");

    if let Some(record) = args.subcommand_matches(RECORD) {
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
    } else if let Some(command) = args.subcommand_matches(HISTORY) {
        setup_history(command, verbose, debug);
    } else if let Some(command) = args.subcommand_matches(COMPARE_LOCAL) {
        setup_compare_local(command, verbose, debug);
    } else {
    }

    Ok(())
}


