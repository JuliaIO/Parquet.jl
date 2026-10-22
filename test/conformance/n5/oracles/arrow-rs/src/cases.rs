use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::ByteArray;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use serde_json::{Value, json};

use crate::{DynError, Result};

pub const DUPLICATE_KEYS: &str = "arrow-rs-duplicate-keys";
pub const OPTIONAL_KEY_PRESENT: &str = "arrow-rs-optional-key-present";
pub const LIST_RULE3: &str = "arrow-rs-list-rule3";
pub const LIST_RULE3_NEAR_NEIGHBOR: &str = "arrow-rs-list-rule3-unannotated-near-neighbor";

const DUPLICATE_SCHEMA: &str = r#"
message schema {
  OPTIONAL group entries (MAP) {
    REPEATED group key_value {
      REQUIRED BINARY key (STRING);
      OPTIONAL INT32 value;
    }
  }
}
"#;

const OPTIONAL_KEY_SCHEMA: &str = r#"
message schema {
  OPTIONAL group entries (MAP) {
    REPEATED group key_value {
      OPTIONAL BINARY key (STRING);
      REQUIRED INT32 value;
    }
  }
}
"#;

const LIST_RULE3_SCHEMA: &str = r#"
message schema {
  OPTIONAL group values (LIST) {
    REPEATED group array (LIST) {
      REPEATED INT32 array;
    }
  }
}
"#;

const LIST_RULE3_NEAR_NEIGHBOR_SCHEMA: &str = r#"
message schema {
  OPTIONAL group values (LIST) {
    REPEATED group list {
      REPEATED INT32 element;
    }
  }
}
"#;

const LIST_RULE3_REPETITION: &[i16] = &[0, 0, 0, 0, 2, 1, 1];
const LIST_RULE3_DEFINITION: &[i16] = &[0, 1, 2, 3, 3, 2, 3];
const LIST_RULE3_VALUES: &[i32] = &[1, 2, 3];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PageVersion {
    V1,
    V2,
}

impl PageVersion {
    pub fn label(self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        }
    }

    fn writer_version(self) -> WriterVersion {
        match self {
            Self::V1 => WriterVersion::PARQUET_1_0,
            Self::V2 => WriterVersion::PARQUET_2_0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnedCase {
    DuplicateKeys,
    OptionalKeyPresent,
    ListRule3,
}

impl OwnedCase {
    pub const ALL: [Self; 3] = [
        Self::DuplicateKeys,
        Self::OptionalKeyPresent,
        Self::ListRule3,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Self::DuplicateKeys => DUPLICATE_KEYS,
            Self::OptionalKeyPresent => OPTIONAL_KEY_PRESENT,
            Self::ListRule3 => LIST_RULE3,
        }
    }

    fn schema(self) -> &'static str {
        match self {
            Self::DuplicateKeys => DUPLICATE_SCHEMA,
            Self::OptionalKeyPresent => OPTIONAL_KEY_SCHEMA,
            Self::ListRule3 => LIST_RULE3_SCHEMA,
        }
    }

    pub fn file_name(self, version: PageVersion) -> String {
        format!("{}_{}.parquet", self.id(), version.label())
    }

    pub fn row_count(self) -> i64 {
        match self {
            Self::DuplicateKeys => 5,
            Self::OptionalKeyPresent => 4,
            Self::ListRule3 => 4,
        }
    }

    pub fn expected_repetitions(self) -> &'static [i16] {
        match self {
            Self::DuplicateKeys => &[0, 0, 0, 0, 1, 1, 0],
            Self::OptionalKeyPresent => &[0, 0, 0, 0, 1],
            Self::ListRule3 => LIST_RULE3_REPETITION,
        }
    }

    pub fn expected_key_definitions(self) -> &'static [i16] {
        match self {
            Self::DuplicateKeys => &[0, 1, 2, 2, 2, 2, 2],
            Self::OptionalKeyPresent => &[0, 1, 3, 3, 3],
            Self::ListRule3 => unreachable!("LIST rule 3 has no map key definition stream"),
        }
    }

    pub fn expected_value_definitions(self) -> &'static [i16] {
        match self {
            Self::DuplicateKeys => &[0, 1, 2, 3, 3, 3, 3],
            Self::OptionalKeyPresent => &[0, 1, 2, 2, 2],
            Self::ListRule3 => unreachable!("LIST rule 3 has no map value definition stream"),
        }
    }

    pub fn expected_key_maximum_definition(self) -> i16 {
        match self {
            Self::DuplicateKeys => 2,
            Self::OptionalKeyPresent => 3,
            Self::ListRule3 => unreachable!("LIST rule 3 has no map key definition maximum"),
        }
    }

    pub fn expected_value_maximum_definition(self) -> i16 {
        match self {
            Self::DuplicateKeys => 3,
            Self::OptionalKeyPresent => 2,
            Self::ListRule3 => unreachable!("LIST rule 3 has no map value definition maximum"),
        }
    }

    pub fn expected_keys(self) -> &'static [&'static str] {
        match self {
            Self::DuplicateKeys => &["a", "a", "a", "b", "c"],
            Self::OptionalKeyPresent => &["a", "b", "c"],
            Self::ListRule3 => unreachable!("LIST rule 3 has no map keys"),
        }
    }

    pub fn expected_values(self) -> &'static [i32] {
        match self {
            Self::DuplicateKeys => &[1, 2, 3, 4],
            Self::OptionalKeyPresent => &[1, 2, 3],
            Self::ListRule3 => LIST_RULE3_VALUES,
        }
    }

    pub fn expected_rows(self) -> Vec<Value> {
        match self {
            Self::DuplicateKeys => vec![
                Value::Null,
                json!([]),
                json!([{"key": "a", "value": null}]),
                json!([
                    {"key": "a", "value": 1},
                    {"key": "a", "value": 2},
                    {"key": "b", "value": 3}
                ]),
                json!([{"key": "c", "value": 4}]),
            ],
            Self::OptionalKeyPresent => vec![
                Value::Null,
                json!([]),
                json!([{"key": "a", "value": 1}]),
                json!([
                    {"key": "b", "value": 2},
                    {"key": "c", "value": 3}
                ]),
            ],
            Self::ListRule3 => vec![
                Value::Null,
                json!([]),
                json!([[]]),
                json!([[1, 2], [], [3]]),
            ],
        }
    }
}

fn properties(version: PageVersion) -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_writer_version(version.writer_version())
            .set_created_by("Parquet.jl N5 Arrow Rust oracle 59.2.0".to_owned())
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_data_page_size_limit(1024 * 1024)
            .set_write_batch_size(1024)
            .build(),
    )
}

fn byte_arrays(values: &[&str]) -> Vec<ByteArray> {
    values.iter().map(|value| ByteArray::from(*value)).collect()
}

pub fn write_case(case: OwnedCase, version: PageVersion, path: &Path) -> Result<()> {
    if case == OwnedCase::ListRule3 {
        return write_list_rule3_schema(LIST_RULE3_SCHEMA, version, path);
    }
    let schema = Arc::new(parse_message_type(case.schema())?);
    let file = File::create(path)?;
    let mut writer = SerializedFileWriter::new(file, schema, properties(version))?;
    let mut row_group = writer.next_row_group()?;

    let mut key_writer = row_group
        .next_column()?
        .ok_or_else(|| -> DynError { "schema has no key column".into() })?;
    match key_writer.untyped() {
        ColumnWriter::ByteArrayColumnWriter(writer) => {
            let values = byte_arrays(case.expected_keys());
            let written = writer.write_batch(
                &values,
                Some(case.expected_key_definitions()),
                Some(case.expected_repetitions()),
            )?;
            if written != values.len() {
                return Err(
                    format!("key writer accepted {written} of {} values", values.len()).into(),
                );
            }
        }
        _ => return Err("key column is not BYTE_ARRAY".into()),
    }
    key_writer.close()?;

    let mut value_writer = row_group
        .next_column()?
        .ok_or_else(|| -> DynError { "schema has no value column".into() })?;
    match value_writer.untyped() {
        ColumnWriter::Int32ColumnWriter(writer) => {
            let values = case.expected_values();
            let written = writer.write_batch(
                values,
                Some(case.expected_value_definitions()),
                Some(case.expected_repetitions()),
            )?;
            if written != values.len() {
                return Err(
                    format!("value writer accepted {written} of {} values", values.len()).into(),
                );
            }
        }
        _ => return Err("value column is not INT32".into()),
    }
    value_writer.close()?;
    if row_group.next_column()?.is_some() {
        return Err("schema has more than two physical columns".into());
    }
    row_group.close()?;
    writer.close()?;
    return Ok(());
}

pub fn generate_all(directory: &Path) -> Result<Vec<(OwnedCase, PageVersion, PathBuf)>> {
    std::fs::create_dir_all(directory)?;
    let mut files = Vec::with_capacity(OwnedCase::ALL.len() * 2);
    for case in OwnedCase::ALL {
        for version in [PageVersion::V1, PageVersion::V2] {
            let path = directory.join(case.file_name(version));
            write_case(case, version, &path)?;
            files.push((case, version, path));
        }
    }
    return Ok(files);
}

fn write_list_rule3_schema(schema_text: &str, version: PageVersion, path: &Path) -> Result<()> {
    let schema = Arc::new(parse_message_type(schema_text)?);
    let file = File::create(path)?;
    let mut writer = SerializedFileWriter::new(file, schema, properties(version))?;
    let mut row_group = writer.next_row_group()?;
    let mut column = row_group
        .next_column()?
        .ok_or_else(|| -> DynError { "rule-3 schema has no physical column".into() })?;
    match column.untyped() {
        ColumnWriter::Int32ColumnWriter(writer) => {
            let written = writer.write_batch(
                LIST_RULE3_VALUES,
                Some(LIST_RULE3_DEFINITION),
                Some(LIST_RULE3_REPETITION),
            )?;
            if written != LIST_RULE3_VALUES.len() {
                return Err("rule-3 writer did not consume every dense value".into());
            }
        }
        _ => return Err("rule-3 leaf is not INT32".into()),
    }
    column.close()?;
    if row_group.next_column()?.is_some() {
        return Err("rule-3 schema has more than one physical column".into());
    }
    row_group.close()?;
    writer.close()?;
    return Ok(());
}

pub fn generate_list_rule3_near_neighbor(directory: &Path) -> Result<Vec<(PageVersion, PathBuf)>> {
    std::fs::create_dir_all(directory)?;
    let mut files = Vec::with_capacity(2);
    for version in [PageVersion::V1, PageVersion::V2] {
        let path = directory.join(format!(
            "{}_{}.parquet",
            LIST_RULE3_NEAR_NEIGHBOR,
            version.label()
        ));
        write_list_rule3_schema(LIST_RULE3_NEAR_NEIGHBOR_SCHEMA, version, &path)?;
        files.push((version, path));
    }
    return Ok(files);
}
