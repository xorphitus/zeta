use chrono::{Utc, DateTime};

use duckdb::{params, Connection, Result};

use itertools::Itertools;

use futures::TryStreamExt;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

use std::env;
use std::path::Path;

pub struct Record {
    pub destination: String,
    pub time: DateTime<Utc>,
    pub values: Vec<f64>,
}

pub fn merge_new_records(parquet_path: &str, new_records: Vec<Record>) -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("INSTALL parquet; LOAD parquet;")?;

    let fields =  match new_records.get(0) {
        Some(first) => {
            first.values.iter().fold(0, |acc, _| acc + 1)
        },
        None => {
            // TODO must return an error
            return Ok(());
        }
    };

    let table = "tmp";
    let sql = if Path::exists(Path::new(parquet_path)) {
        println!("{} was found. Load the Parquet file.", parquet_path);
        format!("CREATE TEMP TABLE {} AS SELECT * FROM read_parquet('{}')", table, parquet_path)
    } else {
        println!("{} does not exit. Define a new table.", parquet_path);
        let mut columns = "time TIMESTAMP PRIMARY KEY".to_string();
        for i in 0..fields {
            columns += &format!(", f{} DOUBLE", i);
        }
        format!("CREATE TEMP TABLE {} ( {} )", table, columns)
    };

    conn.execute(&sql, params![])?;

    let sql = compose_insert_query(table, fields, new_records);
    conn.execute(&sql, params![])?;

    let sql = &format!("COPY (SELECT * FROM {} ORDER BY time ASC) TO '{}' (FORMAT 'parquet')", table, parquet_path);
    conn.execute(&sql, params![])?;

    Ok(())
}

fn compose_insert_query(table: &str, fields: usize, records: Vec<Record>) -> String {
    let sql = &format!("INSERT INTO {} VALUES", table);

    let rows: Vec<String> = records.iter().map(|record| {
        let colls: Vec<String> = (0..fields).map(|i| {
            if let Some(v) = record.values.get(i) {
                format!("{}", v)
            } else {
                "NULL".to_string()
            }
        }).collect();
        let time = record.time.format("%Y-%m-%d %H:%M:%S%.3f");
        format!("('{}', {})", time, colls.join(", "))
    }).collect();

    format!("{} {}", sql, rows.join(", "))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_a() {
        let parquet = "./test.parquet";
        let path = Path::new(parquet);
        if Path::exists(path) {
            std::fs::remove_file(path).unwrap();
        }

        let records = vec![
            Record{
                destination: "".to_string(),
                time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
                values: vec![1.0, 2.0, 3.0],
            },
            Record{
                destination: "".to_string(),
                time: Utc.with_ymd_and_hms(2023, 1, 2, 0, 0, 0).unwrap(),
                values: vec![4.0, 5.0, 6.0],
            },
            Record{
                destination: "".to_string(),
                time: Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap(),
                values: vec![7.0, 8.0, 9.0],
            },
        ];
        let _ = merge_new_records(parquet, records).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL parquet; LOAD parquet;").unwrap();
        let sql = format!("SELECT * FROM read_parquet('{}')", parquet);
        let mut stmt = conn.prepare(&sql).unwrap();
        let iter = stmt.query_map([], |row| {
            // println!("{}", row.get(0).unwrap());
            let f0: f64 = row.get(1).unwrap();
            let f1: f64 = row.get(2).unwrap();
            let f2: f64 = row.get(3).unwrap();
            Ok(format!("{} {} {}", f0, f1, f2))
        }).unwrap();

        let mut result = "".to_string();
        for i in iter {
            result += &format!("{}, ", &i.unwrap());
        }
        assert_eq!(result, "1 2 3, 4 5 6, 7 8 9, ");

        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_compose_insert_query() {
        let sql = compose_insert_query("foo", 0,  vec![]);
        assert_eq!(sql, "INSERT INTO foo VALUES ");

        let sql = compose_insert_query("foo", 1,  vec![]);
        assert_eq!(sql, "INSERT INTO foo VALUES ");

        let sql = compose_insert_query("foo", 3,  vec![
            Record{
                destination: "".to_string(),
                time: Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap(),
                values: vec![1.0, 2.0, 3.0],
            },
            Record{
                destination: "".to_string(),
                time: Utc.with_ymd_and_hms(2023, 1, 2, 0, 0, 0).unwrap(),
                values: vec![1.0, 2.0],
            },
            Record{
                destination: "".to_string(),
                time: Utc.with_ymd_and_hms(2023, 1, 3, 0, 0, 0).unwrap(),
                values: vec![1.0, 2.0, 3.0, 4.0],
            },
        ]);
        assert_eq!(sql, "INSERT INTO foo VALUES ('2023-01-01 00:00:00.000', 1, 2, 3), ('2023-01-02 00:00:00.000', 1, 2, NULL), ('2023-01-03 00:00:00.000', 1, 2, 3)");
    }
}


async fn load_wal() -> Result<()> {
    let data_root = &get_data_root();
    let root_path = Path::new(data_root);
    let db_url = if let Some(path) = root_path.join("wal.sqlite").to_str() {
        format!("sqlite://{}", path)
    } else {
        // TODO must return an error
        return Ok(());
    };
    let pool = SqlitePool::connect(&db_url).await.map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("Database connection error: {}", e))
    }).unwrap();

    let new_rows: Vec<Record> = vec![];
    let mut rows = sqlx::query("SELECT * FROM wal").fetch(&pool);
    while let Some(row) = rows.try_next().await? {
        let id: String = row.try_get("project_id")?;
        let schema: String = row.try_get("schema")?;
        let joined = root_path.join(id).join(schema);
        let parquet_path = if let Some(path) = joined.to_str() {
            path
        } else {
            // TODO must return an error
            return Ok(());
        };

        // TODO must generate new_rows from the payload
        let payload: String = row.try_get("payload")?;
        let str_vals: Vec<&str> = payload.split(",").map(|f| f.trim()).collect();
        let mut values: Vec<f64> = vec![];
        for val in str_vals {
            match val.parse::<f64>() {
                Ok(v) => {
                    values.push(v);
                }
                Err(_) => {
                    // TODO show the error and dispose the row
                    return Ok(());
                }
            }
        }
        let record = Record{
            destination: parquet_path.to_string(),
            time: "a",
            values,
        };
        new_rows.push(record);
    }

    let new_row_groups = new_rows.into_iter().into_group_map_by(|r| r.destination);

    for (k, v) in new_row_groups {
        merge_new_records(&k, v)?
    }

    Ok(())
}

fn get_data_root() -> String {
     env::var("DATA_ROOT").unwrap_or_else(|_| env::current_dir().unwrap().to_str().unwrap().to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_root = get_data_root();
    let pool = SqlitePool::connect("sqlite::memory:").await.map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("Database connection error: {}", e))
    })?;

    loop {
        load_wal().await?;

        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}
