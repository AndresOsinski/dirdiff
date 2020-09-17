use std::time::{UNIX_EPOCH, Duration};

use chrono::NaiveDateTime;
use rusqlite::{NO_PARAMS, params, Connection, Result as SqlResult};

use crate::docs::{Doc, MovedDoc};

pub fn setup_working_tables(conn: &mut Connection) -> SqlResult<usize> {
    conn.execute("CREATE TABLE working_entries (
    id  INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    mod_date INTEGER)", params![])?;
    conn.execute("CREATE TABLE touched_entries (
    id INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    mod_date INTEGER)", params![])
}

// Files which do not exist in revision and do not match any of the previous criteria?
// Files that were renamed, moved, or had their content altered should be excluded
pub fn missing_files(previous: &NaiveDateTime,
                 conn: &Connection) -> Vec<Doc> {
    let missing_sql = "SELECT *
    FROM touched_entries EXCEPT
    SELECT w1.id, w1.hash, w1.name, w1.path, w1.mod_date
    FROM working_entries w1 LEFT JOIN working_entries w2
    ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path = w2.path AND w1.name = w2.name
    WHERE w1.mod_date IS NULL AND w2.id = ?1";

    let mut stmt = conn.prepare(missing_sql).unwrap();
    stmt.query_map(params![previous.timestamp()], |row| {
        Ok(Doc {
            hash: row.get_unwrap(1),
            name: row.get_unwrap(2),
            path: row.get_unwrap(3),
            //mod_date: NaiveDateTime::from_timestamp(row.get_unwrap::<usize, i64>(4), 0)
            mod_date: UNIX_EPOCH + (Duration::from_millis(
                row.get_unwrap::<usize, i64>(4) as u64))
        })
    }).unwrap().map(|i| i.unwrap()).collect()
}

// Files that exist in the latest revision but do not exist in the previous working items
pub fn added_files(latest: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    let added_sql = "SELECT w1.hash, w1.name, w1.path, w1.mod_date
    FROM
    working_entries w1 LEFT JOIN touched_entries w2
    ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path = w2.path AND w1.name = w2.name
    WHERE w1.mod_date = ?1 AND w2.mod_date IS NULL";

    let mut stmt = conn.prepare(added_sql).unwrap();
    stmt.query_map(params![latest.timestamp()], |row| {
        Ok(Doc {
            hash: row.get_unwrap(0),
            name: row.get_unwrap(1),
            path: row.get_unwrap(2),
            mod_date: UNIX_EPOCH + (Duration::from_millis
                (row.get_unwrap::<usize, i64>(3) as u64))
        })
    }).unwrap().map(|i| i.unwrap()).collect()
}

// Same hash, different path
pub fn moved_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<MovedDoc> {
    let moved_sql = "SELECT w1.hash, w1.name, w1.path, w1.mod_date, w2.path
    FROM
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path != w2.path AND w1.name = w2.name
    WHERE w1.mod_date = ?1 AND w2.mod_date = ?2";

    let mut stmt = conn.prepare(moved_sql).unwrap();
    stmt.query_map(params![latest.timestamp(), previous.timestamp()], |row| {
        Ok(MovedDoc {
            doc: Doc {
                hash: row.get_unwrap(0),
                name: row.get_unwrap(1),
                path: row.get_unwrap(2),
                //mod_date: NaiveDateTime::from_timestamp(row.get_unwrap::<usize, i64>(4), 0)
                mod_date: UNIX_EPOCH + (Duration::from_millis(row.get_unwrap::<usize, i64>(3) as u64))
            },
            dest_path: row.get_unwrap(4)
        })
    }).unwrap().map(|i| i.unwrap()).collect()
}

pub fn remove_moved(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize> {
    let moved_sql = "DELETE FROM working_entries WHERE id IN (
    SELECT w1.id
    FROM
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path != w2.path AND w1.name = w2.name
    WHERE w1.mod_date = ?1 AND w2.mod_date = ?2)";
    conn.execute(moved_sql, params![latest.timestamp(), previous.timestamp()])?;

    let worked_entries_sql = "INSERT INTO touched_entries (id, hash, name, path, mod_date)
    SELECT w1.id, w1.hash, w1.name, w1.path, w1.mod_date
    FROM
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path != w2.path AND w1.name = w2.name
    WHERE w1.mod_date = ?1 AND w2.mod_date = ?2";
    conn.execute(worked_entries_sql, params![previous.timestamp(), latest.timestamp()])
}

// Same hash and path, different name
pub fn renamed_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
    let renamed_sql = "SELECT *
    FROM (
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name != w2.name AND w1.path = w2.path
        )
    WHERE w1.mod_date = ?1 and w2.mod_date = ?2";
    let mut stmt = conn.prepare(renamed_sql).unwrap();

    stmt.query_map(params![latest.timestamp(), previous.timestamp()], |row| {
        Ok(Doc {
            hash: row.get_unwrap(1),
            name: row.get_unwrap(2),
            path: row.get_unwrap(3),
            //mod_date: NaiveDateTime::from_timestamp(row.get_unwrap::<usize, i64>(4), 0)
            mod_date: UNIX_EPOCH + (Duration::from_millis(row.get_unwrap::<usize, i64>(4) as u64))
        })
    }).unwrap().map(|i| i.unwrap()).collect()
}

pub fn remove_renamed(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize>{
    let work_renamed_sql = "
    INSERT INTO touched_entries (id, hash, name, path, mod_date)
    SELECT w1.id, w1.hash, w1.name, w1.path, w1.mod_date FROM
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name != w2.name AND w1.path = w2.path
    WHERE w1.mod_date = ?1 and w2.mod_date = ?2";
    conn.execute(work_renamed_sql, params![previous.timestamp(), latest.timestamp()])
        .expect("Could not update touched entries");

    // Yes, hacky
    let renamed_sql_1 = "DELETE FROM working_entries WHERE id IN (
    SELECT w1.id FROM (
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name != w2.name AND w1.path = w2.path
    ) WHERE w1.mod_date = ?1 and w2.mod_date = ?2)";
    conn.execute(renamed_sql_1, params![latest.timestamp(), previous.timestamp()])
        .expect("Could not delete outdated working entries");

    let renamed_sql_2 = "DELETE FROM working_entries WHERE id IN (
    SELECT w2.id FROM
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name != w2.name AND w1.path = w2.path
    WHERE w1.mod_date = ?1 and w2.mod_date = ?2)";
    conn.execute(renamed_sql_2, params![latest.timestamp(), previous.timestamp()])

}

pub fn load_working_table(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> SqlResult<usize> {
    let load_sql = "INSERT INTO working_entries (id, hash, name, path, mod_date)
    SELECT id, hash, name, path, mod_date
    FROM dir_entries
    WHERE mod_date IN (?1, ?2)";

    conn.execute(load_sql, params![latest.timestamp(), previous.timestamp()])
}

pub fn remove_unchanged_from_working_table(previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize> {
    let insert_to_moved_sql = " INSERT INTO touched_entries (id, hash, name, path, mod_date)
    SELECT w1.id, w1.hash, w1.name, w1.path, w1.mod_date
    FROM working_entries w1 INNER JOIN working_entries w2 ON
    (w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name = w2.name AND w1.path = w2.path)
    WHERE w1.mod_date = ?1";
    conn.execute(insert_to_moved_sql, params![previous.timestamp()]).expect("Boom 1");

    let unchanged_sql = "DELETE FROM working_entries WHERE id IN
    (SELECT w1.id FROM working_entries w1 INNER JOIN working_entries w2 ON
    w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name = w2.name AND w1.path = w2.path
    WHERE w1.mod_date = ?1)";
    Ok(conn.execute(unchanged_sql, params![previous.timestamp()]).expect("Boom 2"))
}

pub fn make_local_sqlite() -> Connection {
    Connection::open_in_memory().expect("Cannot create in-memory SQLite")
}

pub fn create_dir_entries_table(conn: &mut Connection) -> SqlResult<usize> {
    conn.execute("CREATE TABLE dir_entries (
    id  INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    mod_date INTEGER)", params![])
}

pub fn load_to_local_sqlite(conn: &mut Connection, entries: Vec<Doc>) -> SqlResult<()> {
    let mut stmt = conn.prepare("INSERT INTO dir_entries (hash, name, path, mod_date) VALUES (?1, ?2, ?3, ?4)").unwrap();

    for entry in entries {
        let start_epoch = entry.mod_date
            .duration_since(UNIX_EPOCH).expect("Date oopsie")
            .as_secs();
        stmt.execute(&[entry.hash, entry.name, entry.path, start_epoch.to_string()]).unwrap();
    }

    Ok(())
}

// Get revision times in milliseconds
pub fn revision_millis(conn: &Connection) -> Vec<i64> {
    let revisions_sql = "SELECT DISTINCT mod_date FROM dir_entries ORDER BY mod_date DESC";
    let stmt = &mut conn.prepare(revisions_sql).expect("Oopsie when getting revision dates");

    stmt.query_map(params![], |row| {
        let val: i64 = row.get(0).unwrap();
        Ok(val)
    }).unwrap().map(|e| e.unwrap()).collect()
}

pub fn get_doclist_from_table(table_name: &str, conn: &mut Connection) -> Vec<Doc> {
    let mut stmt = String::from("SELECT * FROM ");
    stmt += table_name;

    let mut stmt = conn.prepare(&stmt).unwrap();

    stmt.query_map(NO_PARAMS, |row| {
        Ok(Doc {
            hash: row.get_unwrap(1),
            name: row.get_unwrap(2),
            path: row.get_unwrap(3),
            mod_date: UNIX_EPOCH + (Duration::from_millis(row.get_unwrap::<usize, i64>(4) as u64))
        })
    }).unwrap().map(|i| i.unwrap()).collect()
}

// Print entries on a DB table that looks like a dir entry
fn _print_entry_like(table_name: &str, conn: &mut Connection) {
    let mut stmt = String::from("SELECT * FROM ");
    stmt += table_name;

    let mut stmt = conn.prepare(&stmt).unwrap();
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

pub fn print_dir_entries(conn: &mut Connection) {
    const TABLE_NAME: &str = "dir_entries";
    _print_entry_like(TABLE_NAME, conn);
}

pub fn print_working_entries(conn: &mut Connection) {
    const TABLE_NAME: &str = "working_entries";
    _print_entry_like(TABLE_NAME, conn);
}

