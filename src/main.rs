use std::env;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::SystemTime;

use sha1::{Digest, Sha1};
use walkdir::{DirEntry, WalkDir};


struct Doc {
    hash: [u8; 20],
    name: String,
    path: PathBuf,
    mod_date: SystemTime
}

fn help() {
    println!("Usage: 
path - The directory path in which to track file changes");
}


fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with("."))
         .unwrap_or(false)
}

fn gen_dir_struct(path: &String) -> io::Result<()> {
    let path = Path::new(path);
    println!("Getting files from directory {}", &path.display());

    if !path.is_dir() {
        println!("Path must be a directory");
        exit(1);
    }

    for entry in WalkDir::new(path).into_iter().filter_entry(|e| !is_hidden(e)) {
        let mut data = Vec::new();

        let mut fd = File::open(&entry?.path()).unwrap();

        fd.read_to_end(&mut data)?;

        let hash = Sha1::digest(&data);
        println!("{}", entry?.path().display());
        println!("{:?}", hash.as_slice());
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();  

    match args.len() {
        1 => { help(); }
        2 => { gen_dir_struct(&args[1]); }
        _ => { help(); }
    }
}
