use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::SystemTime;

use walkdir::WalkDir;


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

fn gen_dir_struct(path: &String) -> io::Result<()> {
    let path = Path::new(path);
    println!("Getting files from directory {}", &path.display());

    if !path.is_dir() {
        println!("Path must be a directory");
        exit(1);
    }

    for entry in WalkDir::new(path) {
        println!("{}", entry?.path().display());
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
