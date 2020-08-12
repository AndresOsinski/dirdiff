use serde::{Deserialize, Serialize};
use std::time::{UNIX_EPOCH, Duration, SystemTime};

extern crate serde_millis;

#[derive(Deserialize, Serialize)]
pub struct Doc {
    pub hash: String,
    pub name: String,
    pub path: String,
    #[serde(with = "serde_millis")]
    pub mod_date: SystemTime
}

pub fn print_docs(docs: Vec<Doc>) -> () {
    for doc in docs {
        println!("{}, {}, {}", doc.name, doc.path, doc.hash);
    }
}