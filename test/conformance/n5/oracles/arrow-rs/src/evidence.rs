use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::process::Command;

use arrow_array::{
    Array, BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray,
    ListArray, MapArray, StringArray, StructArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_cast::display::array_value_to_string;
use arrow_json::WriterBuilder;
use arrow_json::writer::LineDelimited;
use arrow_schema::{DataType, Field};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::column::page::{Page, PageReader};
use parquet::column::reader::ColumnReader;
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::print_schema;
use parquet::schema::types::ColumnDescriptor;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::cases::{LIST_RULE3, LIST_RULE3_NEAR_NEIGHBOR, OwnedCase, PageVersion};
use crate::{DynError, Result};

pub const ARROW_RS_VERSION: &str = "59.2.0";
pub const ARROW_RS_COMMIT: &str = "782e5a685501a9db6cc8e9a3b7cbff894940c47a";
pub const RUST_TOOLCHAIN: &str = "1.96.1";
pub const RUSTC_VERSION_OUTPUT: &str = "rustc 1.96.1 (31fca3adb 2026-06-26)";
pub const CARGO_VERSION_OUTPUT: &str = "cargo 1.96.1 (356927216 2026-06-26)";

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OracleEvidence {
    pub evidence_version: u32,
    pub oracle: ToolEvidence,
    pub action: String,
    pub files: Vec<FileEvidence>,
}

impl OracleEvidence {
    pub fn new(action: &str, files: Vec<FileEvidence>) -> Result<Self> {
        return Ok(Self {
            evidence_version: 1,
            oracle: ToolEvidence::current()?,
            action: action.to_owned(),
            files,
        });
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolEvidence {
    pub name: String,
    pub version: String,
    pub commit: String,
    pub rust_toolchain: String,
    pub rustc: String,
    pub cargo: String,
}

impl ToolEvidence {
    pub(crate) fn current() -> Result<Self> {
        return Self::current_with(command_version);
    }

    fn current_with<F>(mut version: F) -> Result<Self>
    where
        F: FnMut(&str) -> Result<String>,
    {
        let rustc = version("rustc")
            .map_err(|error| format!("cannot obtain required rustc version: {error}"))?;
        if rustc != RUSTC_VERSION_OUTPUT {
            return Err(format!(
                "rustc version differs from pin: expected {RUSTC_VERSION_OUTPUT:?}, got {rustc:?}"
            )
            .into());
        }
        let cargo = version("cargo")
            .map_err(|error| format!("cannot obtain required cargo version: {error}"))?;
        if cargo != CARGO_VERSION_OUTPUT {
            return Err(format!(
                "cargo version differs from pin: expected {CARGO_VERSION_OUTPUT:?}, got {cargo:?}"
            )
            .into());
        }
        return Ok(Self {
            name: "arrow-rs".to_owned(),
            version: ARROW_RS_VERSION.to_owned(),
            commit: ARROW_RS_COMMIT.to_owned(),
            rust_toolchain: RUST_TOOLCHAIN.to_owned(),
            rustc,
            cargo,
        });
    }
}

fn command_version(program: &str) -> Result<String> {
    let output = Command::new(program)
        .arg("--version")
        .output()
        .map_err(|error| format!("cannot execute {program} --version: {error}"))?;
    if !output.status.success() {
        return Err(format!("{program} --version failed with {}", output.status).into());
    }
    let stdout = String::from_utf8(output.stdout)
        .map_err(|error| format!("{program} --version returned non-UTF-8 output: {error}"))?;
    let value = stdout.trim();
    if value.is_empty() {
        return Err(format!("{program} --version returned empty output").into());
    }
    return Ok(value.to_owned());
}

#[cfg(test)]
mod tool_evidence_tests {
    use std::io::{Error, ErrorKind};

    use super::*;

    #[test]
    fn tool_evidence_rejects_missing_or_different_versions() -> Result<()> {
        let correct = ToolEvidence::current_with(|program| {
            return Ok(match program {
                "rustc" => RUSTC_VERSION_OUTPUT,
                "cargo" => CARGO_VERSION_OUTPUT,
                _ => return Err(format!("unexpected program {program}").into()),
            }
            .to_owned());
        })?;
        assert_eq!(correct.rustc, RUSTC_VERSION_OUTPUT);
        assert_eq!(correct.cargo, CARGO_VERSION_OUTPUT);

        let missing_rustc = ToolEvidence::current_with(|_| {
            return Err(Error::new(ErrorKind::NotFound, "injected missing tool").into());
        })
        .unwrap_err();
        assert!(
            missing_rustc
                .to_string()
                .contains("cannot obtain required rustc version")
        );

        let wrong_rustc = ToolEvidence::current_with(|program| {
            return Ok(match program {
                "rustc" => "rustc 0.0.0 (wrong 1970-01-01)",
                "cargo" => CARGO_VERSION_OUTPUT,
                _ => return Err(format!("unexpected program {program}").into()),
            }
            .to_owned());
        })
        .unwrap_err();
        assert!(
            wrong_rustc
                .to_string()
                .contains("rustc version differs from pin")
        );

        let missing_cargo = ToolEvidence::current_with(|program| {
            if program == "rustc" {
                return Ok(RUSTC_VERSION_OUTPUT.to_owned());
            }
            return Err(Error::new(ErrorKind::NotFound, "injected missing tool").into());
        })
        .unwrap_err();
        assert!(
            missing_cargo
                .to_string()
                .contains("cannot obtain required cargo version")
        );

        let wrong_cargo = ToolEvidence::current_with(|program| {
            return Ok(match program {
                "rustc" => RUSTC_VERSION_OUTPUT,
                "cargo" => "cargo 0.0.0 (wrong 1970-01-01)",
                _ => return Err(format!("unexpected program {program}").into()),
            }
            .to_owned());
        })
        .unwrap_err();
        assert!(
            wrong_cargo
                .to_string()
                .contains("cargo version differs from pin")
        );
        return Ok(());
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileEvidence {
    pub case_id: String,
    pub file_name: String,
    pub sha256: String,
    pub file_bytes: u64,
    pub rows: i64,
    pub row_groups: usize,
    pub physical_schema: String,
    pub columns: Vec<ColumnEvidence>,
    pub arrow: ArrowEvidence,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ColumnEvidence {
    pub path: Vec<String>,
    pub physical_type: String,
    pub maximum_definition_level: i16,
    pub maximum_repetition_level: i16,
    pub row_groups: Vec<ColumnRowGroupEvidence>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ColumnRowGroupEvidence {
    pub row_group: usize,
    pub rows: usize,
    pub compression: String,
    pub repetition: Vec<i16>,
    pub definition: Vec<i16>,
    pub dense_values: Vec<Value>,
    pub pages: Vec<PageEvidence>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PageEvidence {
    pub kind: String,
    pub version: Option<String>,
    pub encoding: String,
    pub values: u32,
    pub rows: Option<usize>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArrowEvidence {
    pub status: String,
    pub schema: Option<Vec<ArrowFieldEvidence>>,
    pub canonical_rows: Vec<Value>,
    pub json_rows: Vec<String>,
    pub ordered_map_rows: Option<Vec<Value>>,
    pub diagnostic: Option<String>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArrowFieldEvidence {
    pub name: String,
    pub nullable: bool,
    pub data_type: Value,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug)]
struct RawPage {
    evidence: PageEvidence,
    declared_rows: Option<usize>,
}

#[derive(Debug)]
struct ColumnRead {
    records: usize,
    levels: usize,
    repetition: Vec<i16>,
    definition: Vec<i16>,
    values: Vec<Value>,
}

fn sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    return Ok(format!("{:x}", digest.finalize()));
}

fn byte_value(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(value) => Value::String(value.to_owned()),
        Err(_) => {
            let mut hex = String::with_capacity(bytes.len() * 2);
            for byte in bytes {
                write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
            }
            json!({"hex": hex})
        }
    }
}

fn int96_value(value: &Int96) -> Value {
    return Value::Array(value.data().iter().copied().map(Value::from).collect());
}

fn fixed_value(value: &FixedLenByteArray) -> Value {
    return byte_value(value.data());
}

fn read_column(
    reader: ColumnReader,
    rows: usize,
    descriptor: &ColumnDescriptor,
) -> Result<ColumnRead> {
    let maximum_definition = descriptor.max_def_level();
    let maximum_repetition = descriptor.max_rep_level();
    let mut definition = Vec::new();
    let mut repetition = Vec::new();

    macro_rules! read_typed {
        ($reader:expr, $value_type:ty, $convert:expr) => {{
            let mut reader = $reader;
            let mut values: Vec<$value_type> = Vec::new();
            let (records, values_read, levels) = if rows == 0 {
                (0, 0, 0)
            } else {
                reader.read_records(
                    rows,
                    (maximum_definition > 0).then_some(&mut definition),
                    (maximum_repetition > 0).then_some(&mut repetition),
                    &mut values,
                )?
            };
            if values_read != values.len() {
                return Err("column reader value count differs from its output".into());
            }
            let converted = values.iter().map($convert).collect();
            (records, levels, converted)
        }};
    }

    let (records, levels, values) = match reader {
        ColumnReader::BoolColumnReader(reader) => {
            read_typed!(reader, bool, |value: &bool| Value::Bool(*value))
        }
        ColumnReader::Int32ColumnReader(reader) => {
            read_typed!(reader, i32, |value: &i32| Value::from(*value))
        }
        ColumnReader::Int64ColumnReader(reader) => {
            read_typed!(reader, i64, |value: &i64| Value::from(*value))
        }
        ColumnReader::Int96ColumnReader(reader) => {
            read_typed!(reader, Int96, |value: &Int96| int96_value(value))
        }
        ColumnReader::FloatColumnReader(reader) => read_typed!(reader, f32, |value: &f32| {
            Value::String(format!("0x{:08x}", value.to_bits()))
        }),
        ColumnReader::DoubleColumnReader(reader) => read_typed!(reader, f64, |value: &f64| {
            Value::String(format!("0x{:016x}", value.to_bits()))
        }),
        ColumnReader::ByteArrayColumnReader(reader) => {
            read_typed!(reader, ByteArray, |value: &ByteArray| byte_value(
                value.data()
            ))
        }
        ColumnReader::FixedLenByteArrayColumnReader(reader) => {
            read_typed!(reader, FixedLenByteArray, |value: &FixedLenByteArray| {
                fixed_value(value)
            })
        }
    };
    if maximum_definition == 0 {
        definition.resize(levels, 0);
    }
    if maximum_repetition == 0 {
        repetition.resize(levels, 0);
    }
    if definition.len() != levels || repetition.len() != levels {
        return Err("column reader level output has an inconsistent length".into());
    }
    return Ok(ColumnRead {
        records,
        levels,
        repetition,
        definition,
        values,
    });
}

fn read_pages(mut reader: Box<dyn PageReader>) -> Result<Vec<RawPage>> {
    let mut pages = Vec::new();
    while let Some(page) = reader.get_next_page()? {
        let raw = match page {
            Page::DictionaryPage {
                num_values,
                encoding,
                ..
            } => RawPage {
                evidence: PageEvidence {
                    kind: "dictionary".to_owned(),
                    version: None,
                    encoding: format!("{encoding:?}"),
                    values: num_values,
                    rows: None,
                },
                declared_rows: None,
            },
            Page::DataPage {
                num_values,
                encoding,
                ..
            } => RawPage {
                evidence: PageEvidence {
                    kind: "data".to_owned(),
                    version: Some("v1".to_owned()),
                    encoding: format!("{encoding:?}"),
                    values: num_values,
                    rows: None,
                },
                declared_rows: None,
            },
            Page::DataPageV2 {
                num_values,
                num_rows,
                encoding,
                ..
            } => RawPage {
                evidence: PageEvidence {
                    kind: "data".to_owned(),
                    version: Some("v2".to_owned()),
                    encoding: format!("{encoding:?}"),
                    values: num_values,
                    rows: None,
                },
                declared_rows: Some(num_rows as usize),
            },
        };
        pages.push(raw);
    }
    return Ok(pages);
}

fn finish_pages(mut pages: Vec<RawPage>, repetition: &[i16]) -> Result<Vec<PageEvidence>> {
    let mut offset = 0_usize;
    for page in &mut pages {
        if page.evidence.kind != "data" {
            continue;
        }
        let end = offset
            .checked_add(page.evidence.values as usize)
            .ok_or_else(|| -> DynError { "page level count overflow".into() })?;
        let levels = repetition
            .get(offset..end)
            .ok_or_else(|| -> DynError { "page levels exceed column output".into() })?;
        let rows = levels.iter().filter(|level| **level == 0).count();
        if let Some(declared) = page.declared_rows {
            if declared != rows {
                return Err(format!(
                    "V2 page declares {declared} rows but repetition levels contain {rows}"
                )
                .into());
            }
        }
        page.evidence.rows = Some(rows);
        offset = end;
    }
    if offset != repetition.len() {
        return Err("data-page value counts do not span the column levels".into());
    }
    return Ok(pages.into_iter().map(|page| page.evidence).collect());
}

fn schema_text(reader: &SerializedFileReader<File>) -> Result<String> {
    let mut bytes = Vec::new();
    print_schema(
        &mut bytes,
        reader
            .metadata()
            .file_metadata()
            .schema_descr()
            .root_schema(),
    );
    return Ok(String::from_utf8(bytes)?);
}

fn field_evidence(field: &Field) -> ArrowFieldEvidence {
    let metadata = field
        .metadata()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    ArrowFieldEvidence {
        name: field.name().to_owned(),
        nullable: field.is_nullable(),
        data_type: data_type_evidence(field.data_type()),
        metadata,
    }
}

fn data_type_evidence(data_type: &DataType) -> Value {
    match data_type {
        DataType::List(field) => json!({"list": field_evidence(field)}),
        DataType::LargeList(field) => json!({"large_list": field_evidence(field)}),
        DataType::FixedSizeList(field, size) => {
            json!({"fixed_size_list": {"size": size, "field": field_evidence(field)}})
        }
        DataType::Struct(fields) => json!({
            "struct": fields.iter().map(|field| field_evidence(field)).collect::<Vec<_>>()
        }),
        DataType::Map(field, sorted) => {
            json!({"map": {"sorted": sorted, "entries": field_evidence(field)}})
        }
        DataType::Dictionary(key, value) => json!({
            "dictionary": {
                "key": data_type_evidence(key),
                "value": data_type_evidence(value)
            }
        }),
        other => Value::String(format!("{other:?}")),
    }
}

fn canonical_value(array: &dyn Array, index: usize) -> Result<Value> {
    if array.is_null(index) {
        return Ok(Value::Null);
    }
    let value = match array.data_type() {
        DataType::Boolean => Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("Boolean array downcast failed")?
                .value(index),
        ),
        DataType::Int8 => Value::from(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or("Int8 array downcast failed")?
                .value(index),
        ),
        DataType::Int16 => Value::from(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or("Int16 array downcast failed")?
                .value(index),
        ),
        DataType::Int32 => Value::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("Int32 array downcast failed")?
                .value(index),
        ),
        DataType::Int64 => Value::from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("Int64 array downcast failed")?
                .value(index),
        ),
        DataType::UInt8 => Value::from(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or("UInt8 array downcast failed")?
                .value(index),
        ),
        DataType::UInt16 => Value::from(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or("UInt16 array downcast failed")?
                .value(index),
        ),
        DataType::UInt32 => Value::from(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or("UInt32 array downcast failed")?
                .value(index),
        ),
        DataType::UInt64 => Value::from(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("UInt64 array downcast failed")?
                .value(index),
        ),
        DataType::Float32 => Value::String(format!(
            "0x{:08x}",
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("Float32 array downcast failed")?
                .value(index)
                .to_bits()
        )),
        DataType::Float64 => Value::String(format!(
            "0x{:016x}",
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Float64 array downcast failed")?
                .value(index)
                .to_bits()
        )),
        DataType::Utf8 => Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("Utf8 array downcast failed")?
                .value(index)
                .to_owned(),
        ),
        DataType::LargeUtf8 => Value::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or("LargeUtf8 array downcast failed")?
                .value(index)
                .to_owned(),
        ),
        DataType::Binary => byte_value(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or("Binary array downcast failed")?
                .value(index),
        ),
        DataType::LargeBinary => byte_value(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or("LargeBinary array downcast failed")?
                .value(index),
        ),
        DataType::List(_) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or("List array downcast failed")?;
            let child = list.value(index);
            Value::Array(
                (0..child.len())
                    .map(|child_index| canonical_value(child.as_ref(), child_index))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        DataType::LargeList(_) => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or("LargeList array downcast failed")?;
            let child = list.value(index);
            Value::Array(
                (0..child.len())
                    .map(|child_index| canonical_value(child.as_ref(), child_index))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        DataType::FixedSizeList(_, _) => {
            let list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or("FixedSizeList array downcast failed")?;
            let child = list.value(index);
            Value::Array(
                (0..child.len())
                    .map(|child_index| canonical_value(child.as_ref(), child_index))
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        DataType::Struct(fields) => {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or("Struct array downcast failed")?;
            Value::Array(
                fields
                    .iter()
                    .zip(structure.columns())
                    .map(|(field, child)| {
                        Ok(json!({
                            "field": field.name(),
                            "value": canonical_value(child.as_ref(), index)?
                        }))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        DataType::Map(_, _) => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or("Map array downcast failed")?;
            let offsets = map.value_offsets();
            let start = offsets[index] as usize;
            let stop = offsets[index + 1] as usize;
            let entries = map.entries();
            let mut pairs = Vec::with_capacity(stop - start);
            for entry in start..stop {
                pairs.push(json!({
                    "key": canonical_value(entries.column(0).as_ref(), entry)?,
                    "value": canonical_value(entries.column(1).as_ref(), entry)?
                }));
            }
            Value::Array(pairs)
        }
        _ => json!({
            "data_type": format!("{:?}", array.data_type()),
            "display": array_value_to_string(array, index)?
        }),
    };
    return Ok(value);
}

fn canonical_rows(batches: &[arrow_array::RecordBatch]) -> Result<Vec<Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let fields = batch
                .schema()
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, column)| {
                    Ok(json!({
                        "field": field.name(),
                        "value": canonical_value(column.as_ref(), row)?
                    }))
                })
                .collect::<Result<Vec<_>>>()?;
            rows.push(Value::Array(fields));
        }
    }
    return Ok(rows);
}

fn ordered_map_rows(batches: &[arrow_array::RecordBatch]) -> Result<Option<Vec<Value>>> {
    if batches.first().is_none_or(|batch| batch.num_columns() != 1) {
        return Ok(None);
    }
    let mut rows = Vec::new();
    for batch in batches {
        if !matches!(batch.column(0).data_type(), DataType::Map(_, _)) {
            return Ok(None);
        }
        for row in 0..batch.num_rows() {
            rows.push(canonical_value(batch.column(0).as_ref(), row)?);
        }
    }
    return Ok(Some(rows));
}

fn read_arrow(path: &Path) -> Result<ArrowEvidence> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let schema = builder.schema().as_ref().clone();
    let mut reader = builder.with_batch_size(1024).build()?;
    let mut batches = Vec::new();
    for batch in &mut reader {
        batches.push(batch?);
    }
    let canonical_rows = canonical_rows(&batches)?;
    let ordered_map_rows = ordered_map_rows(&batches)?;
    let mut json_bytes = Vec::new();
    let json_result = {
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, LineDelimited>(&mut json_bytes);
        let mut result = Ok(());
        for batch in &batches {
            if let Err(error) = writer.write(batch) {
                result = Err(error);
                break;
            }
        }
        if result.is_ok() {
            result = writer.finish();
        }
        result
    };
    let (json_rows, diagnostic) = match json_result {
        Ok(()) => {
            let text = String::from_utf8(json_bytes)?;
            (text.lines().map(str::to_owned).collect(), None)
        }
        Err(error) => (
            Vec::new(),
            Some(format!("Arrow JSON diagnostic unavailable: {error}")),
        ),
    };
    let fields = schema
        .fields()
        .iter()
        .map(|field| field_evidence(field))
        .collect();
    return Ok(ArrowEvidence {
        status: "ok".to_owned(),
        schema: Some(fields),
        canonical_rows,
        json_rows,
        ordered_map_rows,
        diagnostic,
    });
}

fn arrow_evidence(path: &Path) -> ArrowEvidence {
    match read_arrow(path) {
        Ok(evidence) => evidence,
        Err(error) => ArrowEvidence {
            status: "unsupported".to_owned(),
            schema: None,
            canonical_rows: Vec::new(),
            json_rows: Vec::new(),
            ordered_map_rows: None,
            diagnostic: Some(error.to_string()),
        },
    }
}

pub fn inspect_file(path: &Path, case_id: &str) -> Result<FileEvidence> {
    let file = File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();
    let schema_descriptor = file_metadata.schema_descr();
    let mut columns = Vec::with_capacity(schema_descriptor.num_columns());
    for column_index in 0..schema_descriptor.num_columns() {
        let descriptor = schema_descriptor.column(column_index);
        let mut groups = Vec::with_capacity(metadata.num_row_groups());
        for row_group_index in 0..metadata.num_row_groups() {
            let group = reader.get_row_group(row_group_index)?;
            let rows = usize::try_from(group.metadata().num_rows())?;
            let compression = format!(
                "{:?}",
                group.metadata().column(column_index).compression_codec()
            );
            let raw_pages = read_pages(group.get_column_page_reader(column_index)?)?;
            let output = read_column(group.get_column_reader(column_index)?, rows, &descriptor)?;
            if output.records != rows {
                return Err(format!(
                    "column {} row group {row_group_index} read {} of {rows} rows",
                    descriptor.path(),
                    output.records
                )
                .into());
            }
            if output.levels != output.repetition.len() {
                return Err("column level count differs from repetitions".into());
            }
            let pages = finish_pages(raw_pages, &output.repetition)?;
            groups.push(ColumnRowGroupEvidence {
                row_group: row_group_index,
                rows,
                compression,
                repetition: output.repetition,
                definition: output.definition,
                dense_values: output.values,
                pages,
            });
        }
        columns.push(ColumnEvidence {
            path: descriptor.path().parts().to_vec(),
            physical_type: format!("{:?}", descriptor.physical_type()),
            maximum_definition_level: descriptor.max_def_level(),
            maximum_repetition_level: descriptor.max_rep_level(),
            row_groups: groups,
        });
    }
    let file_name = path
        .file_name()
        .ok_or_else(|| -> DynError { "input path has no file name".into() })?
        .to_string_lossy()
        .into_owned();
    return Ok(FileEvidence {
        case_id: case_id.to_owned(),
        file_name,
        sha256: sha256(path)?,
        file_bytes: std::fs::metadata(path)?.len(),
        rows: file_metadata.num_rows(),
        row_groups: metadata.num_row_groups(),
        physical_schema: schema_text(&reader)?,
        columns,
        arrow: arrow_evidence(path),
    });
}

fn expected_dense_strings(values: &[&str]) -> Vec<Value> {
    return values
        .iter()
        .map(|value| Value::String((*value).to_owned()))
        .collect();
}

fn expected_dense_i32(values: &[i32]) -> Vec<Value> {
    return values.iter().copied().map(Value::from).collect();
}

fn expected_arrow_type(case: OwnedCase) -> Value {
    let value_nullable = case == OwnedCase::DuplicateKeys;
    return json!({
        "map": {
            "sorted": false,
            "entries": {
                "name": "key_value",
                "nullable": false,
                "metadata": {},
                "data_type": {
                    "struct": [
                        {
                            "name": "key",
                            "nullable": false,
                            "metadata": {},
                            "data_type": "Utf8"
                        },
                        {
                            "name": "value",
                            "nullable": value_nullable,
                            "metadata": {},
                            "data_type": "Int32"
                        }
                    ]
                }
            }
        }
    });
}

fn expected_canonical_rows(field: &str, values: Vec<Value>) -> Vec<Value> {
    return values
        .into_iter()
        .map(|value| json!([{"field": field, "value": value}]))
        .collect();
}

pub fn verify_owned(case: OwnedCase, version: PageVersion, file: &FileEvidence) -> Result<()> {
    if case == OwnedCase::ListRule3 {
        return verify_list_rule3(version, file);
    }
    if file.case_id != case.id() || file.file_name != case.file_name(version) {
        return Err("owned fixture identity differs from its case".into());
    }
    if file.rows != case.row_count() || file.row_groups != 1 || file.columns.len() != 2 {
        return Err("owned fixture row, row-group, or column count differs".into());
    }
    let expected_paths = [
        ["entries", "key_value", "key"],
        ["entries", "key_value", "value"],
    ];
    for (column, expected_path) in file.columns.iter().zip(expected_paths) {
        let expected: Vec<String> = expected_path
            .iter()
            .map(|value| (*value).to_owned())
            .collect();
        if column.path != expected || column.row_groups.len() != 1 {
            return Err("owned fixture leaf path or row-group evidence differs".into());
        }
        let group = &column.row_groups[0];
        if group.rows != case.row_count() as usize
            || group.compression != "UNCOMPRESSED"
            || group.repetition != case.expected_repetitions()
        {
            return Err("owned fixture repetition stream differs".into());
        }
        let page_versions: Vec<&str> = group
            .pages
            .iter()
            .filter_map(|page| page.version.as_deref())
            .collect();
        if page_versions.is_empty()
            || page_versions
                .iter()
                .any(|actual| *actual != version.label())
        {
            return Err("owned fixture data-page version differs".into());
        }
    }
    let keys = &file.columns[0].row_groups[0];
    let values = &file.columns[1].row_groups[0];
    if file.columns[0].physical_type != "BYTE_ARRAY"
        || file.columns[0].maximum_definition_level != case.expected_key_maximum_definition()
        || file.columns[0].maximum_repetition_level != 1
        || file.columns[1].physical_type != "INT32"
        || file.columns[1].maximum_definition_level != case.expected_value_maximum_definition()
        || file.columns[1].maximum_repetition_level != 1
        || keys.definition != case.expected_key_definitions()
        || keys.dense_values != expected_dense_strings(case.expected_keys())
        || values.definition != case.expected_value_definitions()
        || values.dense_values != expected_dense_i32(case.expected_values())
    {
        return Err("owned fixture definitions or dense values differ".into());
    }
    let schema = file
        .arrow
        .schema
        .as_ref()
        .filter(|schema| schema.len() == 1)
        .ok_or_else(|| -> DynError { "owned fixture has no exact Arrow schema".into() })?;
    if file.arrow.status != "ok"
        || schema[0].name != "entries"
        || !schema[0].nullable
        || !schema[0].metadata.is_empty()
        || schema[0].data_type != expected_arrow_type(case)
        || file.arrow.canonical_rows != expected_canonical_rows("entries", case.expected_rows())
        || file.arrow.ordered_map_rows.as_ref() != Some(&case.expected_rows())
    {
        return Err("owned fixture Arrow schema or ordered rows differ".into());
    }
    return Ok(());
}

fn expected_list_rule3_arrow_type(element_name: &str) -> Value {
    return json!({
        "list": {
            "name": element_name,
            "nullable": false,
            "metadata": {},
            "data_type": {
                "list": {
                    "name": element_name,
                    "nullable": false,
                    "metadata": {},
                    "data_type": "Int32"
                }
            }
        }
    });
}

fn verify_list_rule3_case(
    case_id: &str,
    path: [&str; 3],
    arrow_element_name: &str,
    physical_schema: &str,
    version: PageVersion,
    file: &FileEvidence,
) -> Result<()> {
    let expected_name = format!("{}_{}.parquet", case_id, version.label());
    if file.case_id != case_id
        || file.file_name != expected_name
        || file.rows != 4
        || file.row_groups != 1
        || file.columns.len() != 1
        || file.physical_schema != physical_schema
    {
        return Err("rule-3 fixture identity or shape differs".into());
    }
    let column = &file.columns[0];
    let group = column
        .row_groups
        .first()
        .ok_or_else(|| -> DynError { "rule-3 fixture has no row-group evidence".into() })?;
    if column.path != path
        || column.physical_type != "INT32"
        || column.maximum_definition_level != 3
        || column.maximum_repetition_level != 2
        || group.rows != 4
        || group.compression != "UNCOMPRESSED"
        || group.repetition != [0, 0, 0, 0, 2, 1, 1]
        || group.definition != [0, 1, 2, 3, 3, 2, 3]
        || group.dense_values != [Value::from(1), Value::from(2), Value::from(3)]
    {
        return Err("rule-3 physical evidence differs".into());
    }
    let page_versions: Vec<&str> = group
        .pages
        .iter()
        .filter_map(|page| page.version.as_deref())
        .collect();
    if page_versions.is_empty()
        || page_versions
            .iter()
            .any(|actual| *actual != version.label())
    {
        return Err("rule-3 page version differs".into());
    }
    let expected_rows = [
        "{\"values\":null}",
        "{\"values\":[]}",
        "{\"values\":[[]]}",
        "{\"values\":[[1,2],[],[3]]}",
    ];
    let expected_values = vec![
        Value::Null,
        json!([]),
        json!([[]]),
        json!([[1, 2], [], [3]]),
    ];
    let schema = file
        .arrow
        .schema
        .as_ref()
        .filter(|schema| schema.len() == 1)
        .ok_or_else(|| -> DynError { "rule-3 fixture has no exact Arrow schema".into() })?;
    if file.arrow.status != "ok"
        || schema[0].name != "values"
        || !schema[0].nullable
        || !schema[0].metadata.is_empty()
        || schema[0].data_type != expected_list_rule3_arrow_type(arrow_element_name)
        || file.arrow.canonical_rows != expected_canonical_rows("values", expected_values)
        || file.arrow.json_rows != expected_rows
        || file.arrow.ordered_map_rows.is_some()
        || file.arrow.diagnostic.is_some()
    {
        return Err("Arrow RecordBatch did not report the expected rule-3 outcome".into());
    }
    return Ok(());
}

pub fn verify_list_rule3(version: PageVersion, file: &FileEvidence) -> Result<()> {
    const PHYSICAL_SCHEMA: &str = "message schema {\n  OPTIONAL group values (LIST) {\n    REPEATED group array (LIST) {\n      REPEATED INT32 array;\n    }\n  }\n}\n";
    return verify_list_rule3_case(
        LIST_RULE3,
        ["values", "array", "array"],
        "array",
        PHYSICAL_SCHEMA,
        version,
        file,
    );
}

pub fn verify_list_rule3_near_neighbor(version: PageVersion, file: &FileEvidence) -> Result<()> {
    const PHYSICAL_SCHEMA: &str = "message schema {\n  OPTIONAL group values (LIST) {\n    REPEATED group list {\n      REPEATED INT32 element;\n    }\n  }\n}\n";
    return verify_list_rule3_case(
        LIST_RULE3_NEAR_NEIGHBOR,
        ["values", "list", "element"],
        "element",
        PHYSICAL_SCHEMA,
        version,
        file,
    );
}
