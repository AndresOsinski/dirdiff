use std::time::{UNIX_EPOCH, Duration, SystemTime};

use chrono::NaiveDateTime;
use rusqlite::{NO_PARAMS, params, Connection, Result as SqlResult, Row};

use crate::docs::Doc;

pub fn setup_working_tables(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize> {
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
pub fn missing_files(latest: &NaiveDateTime, previous: &NaiveDateTime,
                 conn: &Connection) -> Vec<Doc> {
    let missing_sql = "SELECT *
    FROM touched_entries MINUS
    (SELECT * FROM working_entries w1 LEFT JOIN working_entries w2
    ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path = w2.path AND w1.name = w2.name
    WHERE w1.mod_date = ?1 AND w2.id IS NULL)";

    let mut stmt = conn.prepare(missing_sql).unwrap();
    let missing = stmt.query_map(params![previous.timestamp()], |row| {
        Ok(Doc {
            hash: row.get_unwrap(1),
            name: row.get_unwrap(2),
            path: row.get_unwrap(3),
            //mod_date: NaiveDateTime::from_timestamp(row.get_unwrap::<usize, i64>(4), 0)
            mod_date: UNIX_EPOCH + (Duration::from_millis(row.get_unwrap::<usize, i64>(4) as u64))
        })
    }).unwrap().map(|i| i.unwrap()).collect();

    missing
}

// Same hash, different path
pub fn moved_files(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> Vec<Doc> {
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

pub fn remove_moved(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize> {
    let moved_sql = "DELETE FROM working_entries WHERE id IN (
    SELECT w1.id
    FROM
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path != w2.path AND w1.name = w2.name
    WHERE w1.mod_date = ?1 AND w2.mod_date = ?2)";
    conn.execute(moved_sql, params![latest.timestamp(), previous.timestamp()])?;

    let worked_entries_sql = "INSERT INTO touched_entries (id, hash, name, path, mod_date)
    VALUES (
        SELECT w1.id, w1.hash, w1.name, w1.path, w1.mod_date
        FROM
            working_entries w1 INNER JOIN working_entries w2
            ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.path != w2.path AND w1.name = w2.name
        WHERE w1.mod_date = ?1 AND w2.mod_date = ?2
)";
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

pub fn remove_renamed(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize>{
    let work_renamed_sql = "
INSERT INTO touched_entries (id, hash, name, path, mod_date ) VALUES(
    SELECT w1.id, w1.hash, w1.name, w1.path FROM (
        working_entries w1 INNER JOIN working_entries w2
        ON w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name != w2.name AND w1.path = w2.path
    ) WHERE w1.mod_date = ?1 and w2.mod_date = ?2
)";
    conn.execute(work_renamed_sql, params![previous.timestamp(), latest.timestamp()])?;

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

pub fn load_working_table(latest: &NaiveDateTime, previous: &NaiveDateTime, conn: &Connection) -> SqlResult<usize> {
    let load_sql = "INSERT INTO working_entries (id, hash, name, path, mod_date)
SELECT id, hash, name, path, mod_date
FROM dir_entries
WHERE mod_date IN (?1, ?2)";

    conn.execute(load_sql, params![latest.timestamp(), previous.timestamp()])
}

pub fn remove_unchanged_from_working_table(previous: &NaiveDateTime, conn: &mut Connection) -> SqlResult<usize> {
    let insert_to_moved_sql = " INSERT INTO touched_entries (id, hash, name, path, mod_date) VALUES(
    SELECT w1.id, w1.hash, w1.name, w1.path, w1.mod_date
    FROM working_entries w1 INNER JOIN working_entries w2 ON
    (w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name = w2.name AND w1.path = w2.path)
    WHERE w1.mod_date = ?1)";
    conn.execute(insert_to_moved_sql, params![previous.timestamp()])?;

    let unchanged_sql = "DELETE FROM working_entries WHERE id IN (
    SELECT w1.id from working_entries w1 INNER JOIN working_entries w2 ON
    (w1.mod_date != w2.mod_date AND w1.hash = w2.hash AND w1.name = w2.name AND w1.path = w2.path)
    WHERE w1.mod_date = ?1)";
    conn.execute(unchanged_sql, params![previous.timestamp()])

}

pub fn make_local_sqlite() -> Connection {
    Connection::open_in_memory().expect("Cannot create in-memory SQLite")
}

pub fn load_to_local_sqlite(conn: &mut Connection, entries: Vec<Doc>) -> SqlResult<()> {
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
pub fn revision_millis(conn: &Connection) -> Vec<i64> {
    let revisions_sql = "SELECT DISTINCT mod_date  FROM dir_entries ORDER BY mod_date DESC";
    let stmt = &mut conn.prepare(revisions_sql).expect("Oopsie when getting revision dates");

    stmt.query_map(params![], |row| {
        let val: i64 = row.get(0).unwrap();
        Ok(val)
    }).unwrap().map(|e| e.unwrap()).collect()
}