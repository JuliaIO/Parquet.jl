use std::env;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use parquet::basic::ColumnOrder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde::Serialize;

type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T> = std::result::Result<T, DynError>;

const PRODUCER: &str = "arrow-rs";
const PRODUCER_VERSION: &str = "59.2.0";
const SOURCE_REVISION: &str = "782e5a685501a9db6cc8e9a3b7cbff894940c47a";
const RUST_TOOLCHAIN: &str = "1.96.1";
const RUSTC: &str = "rustc 1.96.1 (31fca3adb 2026-06-26)";
const CARGO: &str = "cargo 1.96.1 (356927216 2026-06-26)";

#[derive(Serialize)]
struct ToolEvidence {
    name: &'static str,
    version: &'static str,
    commit: &'static str,
    rust_toolchain: &'static str,
    rustc: &'static str,
    cargo: &'static str,
}

#[derive(Serialize)]
struct ColumnEvidence {
    path: Vec<String>,
    physical_type: String,
    column_order: &'static str,
    num_values: i64,
}

#[derive(Serialize)]
struct RowGroupEvidence {
    row_group: usize,
    row_count: i64,
    columns: Vec<ColumnEvidence>,
}

#[derive(Serialize)]
struct MetadataEvidence {
    oracle: ToolEvidence,
    action: &'static str,
    file_name: String,
    file_bytes: u64,
    row_group_count: usize,
    leaf_count: usize,
    row_groups: Vec<RowGroupEvidence>,
}

fn checked_input(value: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    let metadata = fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err("metadata input must be a regular non-link file".into());
    }
    return Ok(path);
}

fn column_order_name(order: ColumnOrder) -> &'static str {
    return match order {
        ColumnOrder::TYPE_DEFINED_ORDER(_) => "TYPE_ORDER",
        ColumnOrder::UNDEFINED => "UNDEFINED",
        ColumnOrder::UNKNOWN => "UNKNOWN",
    };
}

fn inspect(path: &Path) -> Result<MetadataEvidence> {
    let file_bytes = fs::metadata(path)?.len();
    let reader = SerializedFileReader::new(File::open(path)?)?;
    let metadata = reader.metadata();
    let file_metadata = metadata.file_metadata();
    let leaf_count = file_metadata.schema_descr().num_columns();
    let mut row_groups = Vec::with_capacity(metadata.num_row_groups());
    for row_group in 0..metadata.num_row_groups() {
        let group = metadata.row_group(row_group);
        if group.num_columns() != leaf_count {
            return Err("row-group leaf count differs from the schema".into());
        }
        let mut columns = Vec::with_capacity(leaf_count);
        for leaf in 0..leaf_count {
            let column = group.column(leaf);
            columns.push(ColumnEvidence {
                path: column.column_path().parts().to_vec(),
                physical_type: format!("{:?}", column.column_type()),
                column_order: column_order_name(file_metadata.column_order(leaf)),
                num_values: column.num_values(),
            });
        }
        row_groups.push(RowGroupEvidence {
            row_group,
            row_count: group.num_rows(),
            columns,
        });
    }
    let file_name = path
        .file_name()
        .ok_or("metadata input has no file name")?
        .to_str()
        .ok_or("metadata input file name is not UTF-8")?
        .to_owned();
    return Ok(MetadataEvidence {
        oracle: ToolEvidence {
            name: PRODUCER,
            version: PRODUCER_VERSION,
            commit: SOURCE_REVISION,
            rust_toolchain: RUST_TOOLCHAIN,
            rustc: RUSTC,
            cargo: CARGO,
        },
        action: "type-order",
        file_name,
        file_bytes,
        row_group_count: row_groups.len(),
        leaf_count,
        row_groups,
    });
}

fn run() -> Result<()> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    if arguments.len() != 3 || arguments[0] != "type-order" || arguments[1] != "--input" {
        return Err("usage: arrow-rs-metadata type-order --input FILE".into());
    }
    let evidence = inspect(&checked_input(&arguments[2])?)?;
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, &evidence)?;
    use std::io::Write;
    output.write_all(b"\n")?;
    return Ok(());
}

fn main() {
    if let Err(error) = run() {
        eprintln!("arrow-rs-metadata: {error}");
        std::process::exit(1);
    }
}
