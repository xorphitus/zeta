use arrow::array::AsArray;
use arrow::datatypes::{Date64Type, Float64Type};
use futures::TryStreamExt;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{ConvertedType, Type as PhysicalType};
use parquet::data_type::{Int64Type, DoubleType};
use parquet::file::writer::SerializedFileWriter;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::Type;

use std::env;
use std::fs::File;
use std::sync::Arc;

use sqlx::Row;
use sqlx::sqlite::SqlitePool;

/// Represents a new row to be added or merged into the Parquet file.
pub struct Record {
    pub time: i64,
    pub values: Vec<f64>,
}

fn load_parquet_rows(path: &str, columns: usize) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    if std::path::Path::new(path).exists() {
        return Ok(vec![]);
    }

    let file = File::open(path)?;
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;

    let mut rows: Vec<Record> = vec![];
    for batch in arrow_reader {
        while let Ok(ref b) = batch {
            let time_array = b
                .column(0)
                .as_primitive::<Date64Type>()
                .clone();
            let mut values_vecs: Vec<Vec<f64>> = vec![vec![]; columns];
            for i in 0..columns {
                let values_array = b
                    .column(i + 1)
                    .as_primitive::<Float64Type>();
                for j in 0..values_array.len() {
                    values_vecs[i].push(values_array.value(j));
                }
            }

            for i in 0..time_array.len() {
                let mut values: Vec<f64> = vec![];
                for j in 0..values_vecs.len() {
                    values.push(values_vecs[j][i]);
                }
                rows.push(Record {
                    time: time_array.value(i),
                    values,
                });
            }

        }
    }

    Ok(rows)
}

fn write_parquet(path: &str, columns: usize, rows: Vec<Record>)  -> Result<(), Box<dyn std::error::Error>> {
    // Define the schema for the data
    let time_field = Type::primitive_type_builder("time", PhysicalType::INT64)
        .with_converted_type(ConvertedType::TIMESTAMP_MILLIS)
        .build()?;

    let mut fields = vec![Arc::new(time_field)];
    for i in 0..columns {
        let name = std::format!("c{}", i);
        let field = Type::primitive_type_builder(&name, PhysicalType::DOUBLE)
            .build()?;
        fields.push(Arc::new(field));
    }

    let schema = Type::group_type_builder("schema")
        .with_fields(&mut fields)
        .build()?;

    // Write to Parquet
    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = SerializedFileWriter::new(file, Arc::new(schema), Arc::new(props))?;
    let mut row_group_writer = writer.next_row_group()?;

    if  let Some(mut col_writer) = row_group_writer.next_column()? {
        let time_values: Vec<i64> = rows.iter().map(|row| row.time).collect();
        col_writer
            .typed::<Int64Type>()
            .write_batch(&time_values, None, None)?;
        col_writer.close()?
    }

    for i in 1..columns {
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let values: Vec<f64> = rows.iter().map(|row| row.values[i]).collect();
            col_writer.typed::<DoubleType>()
                .write_batch(&values, None, None)?;
            col_writer.close()?
        }
    }

    row_group_writer.close()?;
    writer.close()?;

    Ok(())
}

fn update_or_create_parquet(path: &str, new_rows: Vec<Record>) -> Result<(), Box<dyn std::error::Error>> {
    let columns = new_rows[0].values.len();
    let mut rows = load_parquet_rows(path, columns)?;

    // Merge new rows
    rows.extend(new_rows);
    rows.sort_by_key(|row| row.time);

    write_parquet(path, columns, rows)
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
        let mut rows = sqlx::query("SELECT * FROM wal").fetch(&pool);
        while let Some(row) = rows.try_next().await? {
            let id: String = row.try_get("project_id")?;
            let schema_name: String = row.try_get("schema")?;
            let path = format!("{}/{}/{}", data_root, id, schema_name);

            // TODO must generate new_rows from the payload
            let _p: String = row.try_get("payload")?;
            let new_rows: Vec<Record> = vec![];
            if let Err(e) = update_or_create_parquet(&path, new_rows) {
                eprintln!("Error saving to parquet: {}", e);
                continue;
            }
        }

        std::thread::sleep(std::time::Duration::from_secs(10));
    }
}
