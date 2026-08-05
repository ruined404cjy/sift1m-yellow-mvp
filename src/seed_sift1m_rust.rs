//! 使用 DataInfra 锁定的 Rust Iceberg SDK 写入非分区 SIFT1M fixture。
//!
//! 本程序作为 iceberg-rust-bridge 的临时 example 编译，复用其 Cargo.lock、
//! 本地 SDK path dependency 和离线 Cargo 缓存。

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_cast::cast;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{DataFileFormat, ListType, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const DIMENSION: usize = 128;
const RECORD_BYTES: usize = 4 + DIMENSION * 4;
const EXPECTED_ROWS: usize = 1_000_000;
const VECTOR_DIM_PROPERTY: &str = "vector_dim.embedding";

struct Args {
    input: PathBuf,
    warehouse: PathBuf,
    namespace: String,
    table: String,
    batch_rows: usize,
    compression_name: String,
    compression: Compression,
}

fn parse_compression(value: &str) -> Result<Compression, Box<dyn Error>> {
    match value {
        "uncompressed" => Ok(Compression::UNCOMPRESSED),
        "snappy" => Ok(Compression::SNAPPY),
        "gzip" => Ok(Compression::GZIP(Default::default())),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        _ => Err(format!("不支持的 Parquet 压缩格式: {value}").into()),
    }
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let values: Vec<String> = env::args().collect();
    if values.len() != 7 {
        return Err(format!(
            "用法: {} <input.fvecs> <warehouse> <namespace> <table> <batch_rows> <compression>",
            values
                .first()
                .map(String::as_str)
                .unwrap_or("sift1m_yellow_fixture")
        )
        .into());
    }
    let input = fs::canonicalize(&values[1])?;
    let warehouse = fs::canonicalize(&values[2])?;
    let batch_rows = values[5].parse::<usize>()?;
    if batch_rows == 0 {
        return Err("batch_rows 必须大于 0".into());
    }
    Ok(Args {
        input,
        warehouse,
        namespace: values[3].clone(),
        table: values[4].clone(),
        batch_rows,
        compression_name: values[6].clone(),
        compression: parse_compression(&values[6])?,
    })
}

fn build_schema() -> Result<Schema, Box<dyn Error>> {
    Ok(Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(
                2,
                "embedding",
                Type::List(ListType::new(
                    NestedField::list_element(3, Type::Primitive(PrimitiveType::Float), true)
                        .into(),
                )),
            )
            .into(),
        ])
        .build()?)
}

/// 读取最多 batch_rows 条 fvecs，并构造与 Iceberg schema 一致的 RecordBatch。
fn read_batch(
    reader: &mut BufReader<File>,
    arrow_schema: &Arc<arrow_schema::Schema>,
    first_id: i64,
    batch_rows: usize,
) -> Result<Option<RecordBatch>, Box<dyn Error>> {
    let mut ids = Vec::with_capacity(batch_rows);
    let mut embeddings = ListBuilder::new(Float32Builder::with_capacity(
        batch_rows.saturating_mul(DIMENSION),
    ));
    let mut record = [0_u8; RECORD_BYTES];

    for offset in 0..batch_rows {
        let first_byte = reader.read(&mut record[..1])?;
        if first_byte == 0 {
            break;
        }
        reader.read_exact(&mut record[1..])?;

        let dimension = i32::from_le_bytes(record[..4].try_into()?);
        if dimension != DIMENSION as i32 {
            return Err(format!(
                "第 {} 条向量维度为 {dimension}，期望 {DIMENSION}",
                first_id + offset as i64
            )
            .into());
        }
        ids.push(first_id + offset as i64);
        for raw in record[4..].chunks_exact(4) {
            embeddings
                .values()
                .append_value(f32::from_le_bytes(raw.try_into()?));
        }
        embeddings.append(true);
    }

    if ids.is_empty() {
        return Ok(None);
    }
    let embedding_type = arrow_schema
        .field_with_name("embedding")?
        .data_type()
        .clone();
    let embedding = cast(
        &(Arc::new(embeddings.finish()) as ArrayRef),
        &embedding_type,
    )?;
    Ok(Some(RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(Int64Array::from(ids)) as ArrayRef, embedding],
    )?))
}

fn directory_bytes(path: &Path) -> Result<u64, Box<dyn Error>> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += directory_bytes(&entry.path())?;
        } else if entry
            .path()
            .extension()
            .is_some_and(|value| value == "parquet")
        {
            total += metadata.len();
        }
    }
    Ok(total)
}

fn parquet_file_count(path: &Path) -> Result<usize, Box<dyn Error>> {
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            total += parquet_file_count(&entry.path())?;
        } else if entry
            .path()
            .extension()
            .is_some_and(|value| value == "parquet")
        {
            total += 1;
        }
    }
    Ok(total)
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let expected_bytes = EXPECTED_ROWS as u64 * RECORD_BYTES as u64;
    let actual_bytes = args.input.metadata()?.len();
    if actual_bytes != expected_bytes {
        return Err(format!("SIFT base 大小为 {actual_bytes}，期望 {expected_bytes}").into());
    }

    // MemoryCatalog 的本地默认表目录为 <warehouse>/<namespace>.db/<table>。
    let table_path = args
        .warehouse
        .join(format!("{}.db", args.namespace))
        .join(&args.table);
    if table_path.exists() {
        return Err(format!("表目录已存在，拒绝复用旧快照: {}", table_path.display()).into());
    }

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let catalog: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "sift1m-yellow-fixture",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        args.warehouse.display().to_string(),
                    )]),
                )
                .await?,
        );
        let namespace = NamespaceIdent::new(args.namespace.clone());
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        let schema = build_schema()?;
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(args.table.clone())
                    .schema(schema)
                    // 当前 SDK 不支持列级扩展；该表属性仅用于审计和人工核对。
                    .properties(HashMap::from([
                        (VECTOR_DIM_PROPERTY.to_string(), DIMENSION.to_string()),
                        (
                            "write.parquet.compression-codec".to_string(),
                            args.compression_name.clone(),
                        ),
                    ]))
                    .build(),
            )
            .await?;

        let schema_ref = table.metadata().current_schema().clone();
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema_ref)?);
        let mut reader = BufReader::new(File::open(&args.input)?);
        let mut data_files = Vec::new();
        let mut written = 0_usize;
        let mut batch_number = 0_usize;

        while written < EXPECTED_ROWS {
            let Some(batch) = read_batch(
                &mut reader,
                &arrow_schema,
                written as i64 + 1,
                args.batch_rows,
            )?
            else {
                break;
            };
            let batch_len = batch.num_rows();
            let parquet = ParquetWriterBuilder::new(
                WriterProperties::builder()
                    .set_compression(args.compression)
                    .build(),
                schema_ref.clone(),
            );
            let rolling = RollingFileWriterBuilder::new_with_default_file_size(
                parquet,
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata())?,
                DefaultFileNameGenerator::new(
                    format!("sift1m-{batch_number:03}"),
                    None,
                    DataFileFormat::Parquet,
                ),
            );
            let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
            writer.write(batch).await?;
            data_files.extend(writer.close().await?);
            written += batch_len;
            batch_number += 1;
            eprintln!("Rust fixture 已写入 {written}/{EXPECTED_ROWS}");
        }
        if written != EXPECTED_ROWS {
            return Err(format!("写入行数为 {written}，期望 {EXPECTED_ROWS}").into());
        }
        let mut trailing = [0_u8; 1];
        if reader.read(&mut trailing)? != 0 {
            return Err("SIFT base 在一百万行后仍有多余数据".into());
        }

        let transaction = Transaction::new(&table);
        let table = transaction
            .fast_append()
            .add_data_files(data_files)
            .apply(transaction)?
            .commit(catalog.as_ref())
            .await?;
        let identifier = TableIdent::from_strs([&args.namespace, &args.table])?;
        let table = catalog.load_table(&identifier).await.unwrap_or(table);
        let metadata_location = table
            .metadata_location()
            .ok_or("供数完成后没有 metadata location")?;
        let metadata_uri = if metadata_location.starts_with('/') {
            format!("file://{metadata_location}")
        } else {
            metadata_location.to_string()
        };
        let parquet_files = parquet_file_count(&table_path)?;
        let parquet_bytes = directory_bytes(&table_path)?;

        println!("MVP_PROVIDER=rust-iceberg-sdk");
        println!("MVP_ROW_COUNT={EXPECTED_ROWS}");
        println!("MVP_FIELD_VECTOR_DIM=unsupported");
        println!("MVP_VECTOR_DIM_PROPERTY={VECTOR_DIM_PROPERTY}={DIMENSION}");
        println!("MVP_PARTITION_BUCKETS=0");
        println!("MVP_PARQUET_FILES={parquet_files}");
        println!("MVP_PARQUET_BYTES={parquet_bytes}");
        println!("MVP_METADATA_LOCATION={metadata_uri}");
        Ok::<(), Box<dyn Error>>(())
    })
}
