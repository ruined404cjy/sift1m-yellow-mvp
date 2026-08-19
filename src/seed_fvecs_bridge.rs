//! 通过 DataInfra bridge ABI 生成参数化 fvecs Iceberg v3 fixture。
//!
//! 本程序作为 iceberg-rust-bridge 的临时 example 编译。向量数据先按
//! Iceberg bucket transform 分流到临时 Arrow IPC，再通过 bridge 分区写入
//! ABI 产生每 bucket 一个 Parquet，最后通过 catalog-free fast append 提交。

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;

use arrow_array::builder::{Float32Builder, ListBuilder};
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_cast::cast;
use arrow_ipc::writer::StreamWriter;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{
    ListType, NestedField, PrimitiveType, Schema, SchemaRef, Transform, Type, UnboundPartitionSpec,
};
use iceberg_rust_bridge::{
    IcebergBridgeDataFileList, IcebergBridgeError, IcebergBridgeNamespaceIdent,
    IcebergBridgeStatus, IcebergBridgeStorage, IcebergBridgeString, IcebergBridgeTable,
    IcebergBridgeTableIdent, iceberg_bridge_data_file_list_free,
    iceberg_bridge_data_file_list_get_json, iceberg_bridge_data_file_list_record_count,
    iceberg_bridge_data_file_list_size, iceberg_bridge_error_free, iceberg_bridge_error_message,
    iceberg_bridge_storage_open, iceberg_bridge_storage_release, iceberg_bridge_string_data,
    iceberg_bridge_string_free, iceberg_bridge_table_create,
    iceberg_bridge_table_current_snapshot_id, iceberg_bridge_table_fast_append_commit,
    iceberg_bridge_table_format_version, iceberg_bridge_table_free, iceberg_bridge_table_load,
    iceberg_bridge_table_metadata_location, iceberg_bridge_table_write_partitioned_data_files,
    iceberg_bridge_version,
};
use serde_json::{Value, json};
use uuid::Uuid;

const VECTOR_DIM_PROPERTY: &str = "vector_dim.embedding";
const FORMAT_VERSION: i32 = 3;

struct Args {
    input: PathBuf,
    warehouse: PathBuf,
    namespace: String,
    table: String,
    dataset: String,
    dimension: usize,
    rows: usize,
    id_base: i64,
    batch_rows: usize,
    compression: String,
    partition_buckets: u32,
    target_file_size_bytes: u64,
}

struct StagedWriter {
    path: PathBuf,
    writer: StreamWriter<File>,
}

struct StagingDir(PathBuf);

impl Drop for StagingDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some('a'..='z' | '_'))
        && chars.all(|character| matches!(character, 'a'..='z' | '0'..='9' | '_'))
}

fn parse_args() -> Result<Args, Box<dyn Error>> {
    let values: Vec<String> = env::args().collect();
    if values.len() != 13 {
        return Err(format!(
            "用法: {} <input.fvecs> <warehouse> <namespace> <table> <dataset> <dimension> <rows> <id_base> <batch_rows> <compression> <partition_buckets> <target_file_size_bytes>",
            values.first().map(String::as_str).unwrap_or("fvecs_bridge_fixture")
        )
        .into());
    }
    let input = fs::canonicalize(&values[1])?;
    let warehouse = fs::canonicalize(&values[2])?;
    let dimension = values[6].parse::<usize>()?;
    let rows = values[7].parse::<usize>()?;
    let id_base = values[8].parse::<i64>()?;
    let batch_rows = values[9].parse::<usize>()?;
    let partition_buckets = values[11].parse::<u32>()?;
    let target_file_size_bytes = values[12].parse::<u64>()?;
    if dimension == 0 || rows == 0 || batch_rows == 0 || target_file_size_bytes == 0 {
        return Err("dimension、rows、batch_rows 和 target_file_size_bytes 必须大于 0".into());
    }
    if !matches!(id_base, 0 | 1) {
        return Err("id_base 仅支持 0 或 1".into());
    }
    if partition_buckets == 0 {
        return Err("bridge provider 当前要求 partition_buckets 大于 0".into());
    }
    for (label, value) in [("namespace", &values[3]), ("table", &values[4])] {
        if !valid_identifier(value) {
            return Err(format!("{label} 仅支持小写字母、数字和下划线: {value}").into());
        }
    }
    if !matches!(
        values[10].as_str(),
        "uncompressed" | "snappy" | "zstd" | "lz4"
    ) {
        return Err(format!("不支持的 Parquet 压缩格式: {}", values[10]).into());
    }
    Ok(Args {
        input,
        warehouse,
        namespace: values[3].clone(),
        table: values[4].clone(),
        dataset: values[5].clone(),
        dimension,
        rows,
        id_base,
        batch_rows,
        compression: values[10].clone(),
        partition_buckets,
        target_file_size_bytes,
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

/// 读取最多 batch_rows 条 fvecs，并按配置的 ID 基值构造 RecordBatch。
fn read_batch(
    reader: &mut BufReader<File>,
    arrow_schema: &Arc<arrow_schema::Schema>,
    first_id: i64,
    batch_rows: usize,
    dimension: usize,
) -> Result<Option<RecordBatch>, Box<dyn Error>> {
    let value_capacity = batch_rows
        .checked_mul(dimension)
        .ok_or("Arrow 向量 builder 容量溢出")?;
    let mut ids = Vec::with_capacity(batch_rows);
    let mut embeddings = ListBuilder::new(Float32Builder::with_capacity(value_capacity));
    let record_bytes = 4_usize
        .checked_add(dimension.checked_mul(4).ok_or("fvecs 记录长度溢出")?)
        .ok_or("fvecs 记录长度溢出")?;
    let mut record = vec![0_u8; record_bytes];

    for offset in 0..batch_rows {
        let first_byte = reader.read(&mut record[..1])?;
        if first_byte == 0 {
            break;
        }
        reader.read_exact(&mut record[1..])?;
        let actual_dimension = i32::from_le_bytes(record[..4].try_into()?);
        if actual_dimension != dimension as i32 {
            return Err(format!(
                "第 {} 条向量维度为 {actual_dimension}，期望 {dimension}",
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

fn take_error(error: &mut *mut IcebergBridgeError) -> String {
    if error.is_null() {
        return String::new();
    }
    let message = iceberg_bridge_error_message(*error);
    let result = if message.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    };
    iceberg_bridge_error_free(*error);
    *error = ptr::null_mut();
    result
}

fn require_ok(
    status: IcebergBridgeStatus,
    error: &mut *mut IcebergBridgeError,
    operation: &str,
) -> Result<(), Box<dyn Error>> {
    if status == IcebergBridgeStatus::Ok {
        return Ok(());
    }
    Err(format!("{operation} 失败: {status:?}: {}", take_error(error)).into())
}

fn take_string(value: *mut IcebergBridgeString) -> Result<String, Box<dyn Error>> {
    if value.is_null() {
        return Err("bridge 返回空字符串句柄".into());
    }
    let data = iceberg_bridge_string_data(value);
    if data.is_null() {
        iceberg_bridge_string_free(value);
        return Err("bridge 返回空字符串数据".into());
    }
    let result = unsafe { CStr::from_ptr(data) }
        .to_string_lossy()
        .into_owned();
    iceberg_bridge_string_free(value);
    Ok(result)
}

fn open_storage() -> Result<*mut IcebergBridgeStorage, Box<dyn Error>> {
    let config = CString::new(r#"{"storage_scheme":"fs"}"#)?;
    let mut storage = ptr::null_mut();
    let mut error = ptr::null_mut();
    require_ok(
        iceberg_bridge_storage_open(config.as_ptr(), &mut storage, &mut error),
        &mut error,
        "storage_open",
    )?;
    Ok(storage)
}

fn create_table(
    storage: *mut IcebergBridgeStorage,
    args: &Args,
    schema: &Schema,
    partition_spec: &UnboundPartitionSpec,
    table_uri: &str,
) -> Result<(*mut IcebergBridgeTable, String), Box<dyn Error>> {
    let creation = json!({
        "name": args.table,
        "namespace": [args.namespace],
        "location": table_uri,
        "format_version": "V3",
        "schema": schema,
        "partition_spec": partition_spec,
        "properties": {
            VECTOR_DIM_PROPERTY: args.dimension.to_string(),
            "write.parquet.compression-codec": args.compression,
            "write.target-file-size-bytes": args.target_file_size_bytes.to_string(),
        },
    });
    let creation = CString::new(creation.to_string())?;
    let mut table = ptr::null_mut();
    let mut error = ptr::null_mut();
    require_ok(
        iceberg_bridge_table_create(storage, creation.as_ptr(), &mut table, &mut error),
        &mut error,
        "table_create",
    )?;
    if iceberg_bridge_table_format_version(table) != FORMAT_VERSION {
        iceberg_bridge_table_free(table);
        return Err("bridge table_create 未生成 Iceberg v3".into());
    }
    let mut location = ptr::null_mut();
    require_ok(
        iceberg_bridge_table_metadata_location(table, &mut location, &mut error),
        &mut error,
        "table_metadata_location",
    )?;
    Ok((table, take_string(location)?))
}

/// 将输入分区流化写入 IPC staging，避免 GIST1M 全量驻留内存。
fn stage_partition_streams(
    args: &Args,
    schema_ref: SchemaRef,
    arrow_schema: Arc<arrow_schema::Schema>,
    partition_spec: Arc<iceberg::spec::PartitionSpec>,
    staging_dir: &Path,
) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let splitter =
        RecordBatchPartitionSplitter::try_new_with_computed_values(schema_ref, partition_spec)?;
    let mut reader = BufReader::new(File::open(&args.input)?);
    let mut writers: HashMap<String, StagedWriter> = HashMap::new();
    let mut written = 0_usize;

    while written < args.rows {
        let first_id = args.id_base.checked_add(written as i64).ok_or("ID 溢出")?;
        let Some(batch) = read_batch(
            &mut reader,
            &arrow_schema,
            first_id,
            args.batch_rows.min(args.rows - written),
            args.dimension,
        )?
        else {
            break;
        };
        let batch_len = batch.num_rows();
        for (partition_key, partition_batch) in splitter.split(&batch)? {
            let key = partition_key.to_path();
            if !writers.contains_key(&key) {
                let path = staging_dir.join(format!("{}.arrow", Uuid::new_v4()));
                let writer =
                    StreamWriter::try_new(File::create(&path)?, partition_batch.schema().as_ref())?;
                writers.insert(key.clone(), StagedWriter { path, writer });
            }
            writers
                .get_mut(&key)
                .ok_or("IPC staging writer 丢失")?
                .writer
                .write(&partition_batch)?;
        }
        written += batch_len;
        eprintln!("Bridge fixture 已分流 {written}/{}", args.rows);
    }
    if written != args.rows {
        return Err(format!("读取行数为 {written}，期望 {}", args.rows).into());
    }
    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err("输入在配置行数后仍有多余数据".into());
    }
    if writers.len() != args.partition_buckets as usize {
        return Err(format!(
            "实际非空 bucket 数为 {}，期望 {}",
            writers.len(),
            args.partition_buckets
        )
        .into());
    }

    let mut paths = Vec::with_capacity(writers.len());
    for (_, mut staged) in writers {
        staged.writer.finish()?;
        paths.push(staged.path);
    }
    paths.sort();
    Ok(paths)
}

fn write_partition_files(
    table: *mut IcebergBridgeTable,
    staged_paths: &[PathBuf],
    expected_rows: usize,
) -> Result<Vec<Value>, Box<dyn Error>> {
    let mut data_files = Vec::with_capacity(staged_paths.len());
    let mut total_rows = 0_u64;
    for (index, path) in staged_paths.iter().enumerate() {
        let ipc = fs::read(path)?;
        let mut list: *mut IcebergBridgeDataFileList = ptr::null_mut();
        let mut error = ptr::null_mut();
        require_ok(
            iceberg_bridge_table_write_partitioned_data_files(
                table,
                ipc.as_ptr(),
                ipc.len(),
                &mut list,
                &mut error,
            ),
            &mut error,
            "table_write_partitioned_data_files",
        )?;
        let file_count = iceberg_bridge_data_file_list_size(list);
        if file_count != 1 {
            iceberg_bridge_data_file_list_free(list);
            return Err(
                format!("第 {index} 个 bucket 生成 {file_count} 个 Parquet，期望 1").into(),
            );
        }
        total_rows = total_rows
            .checked_add(iceberg_bridge_data_file_list_record_count(list))
            .ok_or("写入行数溢出")?;
        let mut value = ptr::null_mut();
        require_ok(
            iceberg_bridge_data_file_list_get_json(list, 0, &mut value, &mut error),
            &mut error,
            "data_file_list_get_json",
        )?;
        data_files.push(serde_json::from_str(&take_string(value)?)?);
        iceberg_bridge_data_file_list_free(list);
        eprintln!(
            "Bridge fixture 已写入 bucket {}/{}",
            index + 1,
            staged_paths.len()
        );
    }
    if total_rows != expected_rows as u64 {
        return Err(format!("bridge 分区写入行数为 {total_rows}，期望 {expected_rows}").into());
    }
    Ok(data_files)
}

fn fast_append(
    storage: *mut IcebergBridgeStorage,
    metadata_location: &str,
    args: &Args,
    data_files: &[Value],
) -> Result<(String, i64), Box<dyn Error>> {
    let metadata_location = CString::new(metadata_location)?;
    let table_ident =
        CString::new(json!({"namespace": [args.namespace], "name": args.table}).to_string())?;
    let data_files = CString::new(serde_json::to_string(data_files)?)?;
    let mut new_location = ptr::null_mut();
    let mut snapshot_id = 0_i64;
    let mut error = ptr::null_mut();
    require_ok(
        iceberg_bridge_table_fast_append_commit(
            storage,
            metadata_location.as_ptr(),
            table_ident.as_ptr(),
            data_files.as_ptr(),
            ptr::null(),
            &mut new_location,
            &mut snapshot_id,
            &mut error,
        ),
        &mut error,
        "table_fast_append_commit",
    )?;
    if snapshot_id == 0 {
        return Err("fast append 未返回 snapshot ID".into());
    }
    Ok((take_string(new_location)?, snapshot_id))
}

fn verify_table(
    storage: *mut IcebergBridgeStorage,
    metadata_location: &str,
    snapshot_id: i64,
    args: &Args,
) -> Result<(), Box<dyn Error>> {
    let namespace = CString::new(args.namespace.as_str())?;
    let table_name = CString::new(args.table.as_str())?;
    let levels = [namespace.as_ptr()];
    let ident = IcebergBridgeTableIdent {
        namespace_ident: IcebergBridgeNamespaceIdent {
            levels: levels.as_ptr(),
            level_count: levels.len(),
        },
        name: table_name.as_ptr(),
    };
    let location = CString::new(metadata_location)?;
    let mut table = ptr::null_mut();
    let mut error = ptr::null_mut();
    require_ok(
        iceberg_bridge_table_load(storage, location.as_ptr(), &ident, &mut table, &mut error),
        &mut error,
        "table_load",
    )?;
    if iceberg_bridge_table_format_version(table) != FORMAT_VERSION {
        iceberg_bridge_table_free(table);
        return Err("bridge 回读的表不是 Iceberg v3".into());
    }
    let mut actual_snapshot = 0_i64;
    let mut has_snapshot = false;
    require_ok(
        iceberg_bridge_table_current_snapshot_id(
            table,
            &mut actual_snapshot,
            &mut has_snapshot,
            &mut error,
        ),
        &mut error,
        "table_current_snapshot_id",
    )?;
    iceberg_bridge_table_free(table);
    if !has_snapshot || actual_snapshot != snapshot_id {
        return Err(format!("bridge 回读 snapshot={actual_snapshot}，期望 {snapshot_id}").into());
    }

    let metadata_path = metadata_location
        .strip_prefix("file://")
        .ok_or("metadata location 必须使用 file://")?;
    let metadata: Value = serde_json::from_reader(File::open(metadata_path)?)?;
    if metadata["format-version"] != FORMAT_VERSION {
        return Err("metadata JSON 的 format-version 不是 3".into());
    }
    if metadata["properties"][VECTOR_DIM_PROPERTY] != args.dimension.to_string() {
        return Err("metadata JSON 的表级向量维度属性不匹配".into());
    }
    Ok(())
}

fn parquet_stats(path: &Path) -> Result<(usize, u64), Box<dyn Error>> {
    let mut files = 0_usize;
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            let (child_files, child_bytes) = parquet_stats(&entry.path())?;
            files += child_files;
            bytes += child_bytes;
        } else if entry
            .path()
            .extension()
            .is_some_and(|value| value == "parquet")
        {
            files += 1;
            bytes += metadata.len();
        }
    }
    Ok((files, bytes))
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let record_bytes = 4_usize
        .checked_add(args.dimension.checked_mul(4).ok_or("fvecs 记录长度溢出")?)
        .ok_or("fvecs 记录长度溢出")?;
    let expected_bytes = args
        .rows
        .checked_mul(record_bytes)
        .ok_or("输入大小计算溢出")? as u64;
    let actual_bytes = args.input.metadata()?.len();
    if actual_bytes != expected_bytes {
        return Err(format!("输入大小为 {actual_bytes}，期望 {expected_bytes}").into());
    }

    let table_path = args.warehouse.join(&args.namespace).join(&args.table);
    if table_path.exists() {
        return Err(format!("表目录已存在，拒绝复用旧快照: {}", table_path.display()).into());
    }
    let table_uri = format!("file://{}", table_path.display());
    let schema = build_schema()?;
    let schema_ref = Arc::new(schema.clone());
    let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
    let unbound_spec = UnboundPartitionSpec::builder()
        .with_spec_id(0)
        .add_partition_field(1, "id_bucket", Transform::Bucket(args.partition_buckets))?
        .build();
    let partition_spec = Arc::new(unbound_spec.clone().bind(schema_ref.clone())?);

    let storage = open_storage()?;
    let (table, initial_metadata) =
        create_table(storage, &args, &schema, &unbound_spec, &table_uri)?;
    let staging_path = args.warehouse.join(format!(
        ".{}-bridge-staging-{}",
        args.dataset,
        Uuid::new_v4()
    ));
    fs::create_dir(&staging_path)?;
    let _staging = StagingDir(staging_path.clone());
    let staged_paths = stage_partition_streams(
        &args,
        schema_ref,
        arrow_schema,
        partition_spec,
        &staging_path,
    )?;
    let data_files = write_partition_files(table, &staged_paths, args.rows)?;
    if data_files.len() != args.partition_buckets as usize {
        iceberg_bridge_table_free(table);
        iceberg_bridge_storage_release(storage);
        return Err(format!(
            "DataFile 数为 {}，期望 {}",
            data_files.len(),
            args.partition_buckets
        )
        .into());
    }
    let (metadata_location, snapshot_id) =
        fast_append(storage, &initial_metadata, &args, &data_files)?;
    iceberg_bridge_table_free(table);
    verify_table(storage, &metadata_location, snapshot_id, &args)?;
    iceberg_bridge_storage_release(storage);

    let (parquet_files, parquet_bytes) = parquet_stats(&table_path)?;
    if parquet_files != args.partition_buckets as usize {
        return Err(format!(
            "Parquet 文件数为 {parquet_files}，期望 {}",
            args.partition_buckets
        )
        .into());
    }
    let version_ptr = iceberg_bridge_version();
    let version = if version_ptr.is_null() {
        "unknown".to_string()
    } else {
        unsafe { CStr::from_ptr(version_ptr) }
            .to_string_lossy()
            .into_owned()
    };
    println!("MVP_PROVIDER=bridge-abi-{version}");
    println!("MVP_DATASET={}", args.dataset);
    println!("MVP_ROW_COUNT={}", args.rows);
    println!("MVP_ICEBERG_FORMAT_VERSION={FORMAT_VERSION}");
    println!("MVP_FIELD_VECTOR_DIM=unsupported");
    println!(
        "MVP_VECTOR_DIM_PROPERTY={VECTOR_DIM_PROPERTY}={}",
        args.dimension
    );
    println!("MVP_PARTITION_BUCKETS={}", args.partition_buckets);
    println!("MVP_PARQUET_FILES={parquet_files}");
    println!("MVP_PARQUET_BYTES={parquet_bytes}");
    println!("MVP_METADATA_LOCATION={metadata_location}");
    Ok(())
}
