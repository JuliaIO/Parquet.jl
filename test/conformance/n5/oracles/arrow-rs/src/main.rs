mod cases;
mod evidence;

use std::env;
use std::fs::{self, File};
use std::io::{self, Write};
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};

use cases::{LIST_RULE3_NEAR_NEIGHBOR, generate_all, generate_list_rule3_near_neighbor};
use evidence::{
    FileEvidence, OracleEvidence, ToolEvidence, inspect_file, verify_list_rule3_near_neighbor,
    verify_owned,
};

pub type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T> = std::result::Result<T, DynError>;

fn usage() -> &'static str {
    "usage:\n  arrow-rs-oracle generate --output DIR [--evidence FILE]\n  arrow-rs-oracle diagnose-rule3-near-neighbor --output DIR [--evidence FILE]\n  arrow-rs-oracle inspect --input FILE --case-id ID [--evidence FILE]\n  arrow-rs-oracle audit --input FILE_OR_DIR [--evidence FILE]\n  arrow-rs-oracle verify --input FILE --expected FILE --case-id ID [--evidence FILE]\n  arrow-rs-oracle versions [--evidence FILE]"
}

fn option(args: &[String], name: &str) -> Result<Option<String>> {
    let mut value = None;
    let mut index = 0;
    while index < args.len() {
        if args[index] == name {
            if value.is_some() {
                return Err(format!("duplicate option {name}").into());
            }
            let next = args
                .get(index + 1)
                .ok_or_else(|| format!("missing value for {name}"))?;
            value = Some(next.clone());
            index += 2;
        } else {
            index += 1;
        }
    }
    return Ok(value);
}

fn required(args: &[String], name: &str) -> Result<String> {
    return option(args, name)?.ok_or_else(|| format!("required option {name} is absent").into());
}

fn validate_options(args: &[String], allowed: &[&str]) -> Result<()> {
    let mut index = 0;
    while index < args.len() {
        let current = &args[index];
        if !allowed.contains(&current.as_str()) {
            return Err(format!("unknown option {current}").into());
        }
        if index + 1 >= args.len() {
            return Err(format!("missing value for {current}").into());
        }
        index += 2;
    }
    return Ok(());
}

fn output_evidence(evidence: &OracleEvidence, path: Option<String>) -> Result<()> {
    match path {
        Some(path) => {
            let mut file = File::create(path)?;
            serde_json::to_writer_pretty(&mut file, evidence)?;
            file.write_all(b"\n")?;
        }
        None => {
            let stdout = io::stdout();
            let mut output = stdout.lock();
            serde_json::to_writer_pretty(&mut output, evidence)?;
            output.write_all(b"\n")?;
        }
    }
    return Ok(());
}

#[derive(serde::Serialize)]
struct AuditEvidence {
    evidence_version: u32,
    oracle: ToolEvidence,
    action: String,
    file_count: usize,
    supported_count: usize,
    unsupported_count: usize,
    files: Vec<AuditFileEvidence>,
}

#[derive(serde::Serialize)]
struct AuditFileEvidence {
    status: String,
    file: String,
    evidence: Option<FileEvidence>,
    error: Option<String>,
}

fn output_audit(evidence: &AuditEvidence, path: Option<String>) -> Result<()> {
    match path {
        Some(path) => {
            let mut file = File::create(path)?;
            serde_json::to_writer_pretty(&mut file, evidence)?;
            file.write_all(b"\n")?;
        }
        None => {
            let stdout = io::stdout();
            let mut output = stdout.lock();
            serde_json::to_writer_pretty(&mut output, evidence)?;
            output.write_all(b"\n")?;
        }
    }
    return Ok(());
}

fn parquet_files(root: &Path) -> Result<Vec<PathBuf>> {
    let metadata = fs::symlink_metadata(root)?;
    if metadata.file_type().is_symlink() {
        return Err("audit input must not be a symbolic link".into());
    }
    if metadata.is_file() {
        if root.extension().and_then(|value| value.to_str()) != Some("parquet") {
            return Err("audit input file must have a .parquet suffix".into());
        }
        return Ok(vec![root.to_path_buf()]);
    }
    if !metadata.is_dir() {
        return Err("audit input is not a file or directory".into());
    }
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(
                    format!("audit input contains symbolic link {}", path.display()).into(),
                );
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file()
                && path.extension().and_then(|value| value.to_str()) == Some("parquet")
            {
                files.push(path);
            }
        }
    }
    files.sort_by(|left, right| audit_name(root, left).cmp(&audit_name(root, right)));
    if files.is_empty() {
        return Err("audit input contains no .parquet files".into());
    }
    return Ok(files);
}

fn audit_name(root: &Path, file: &Path) -> String {
    if root.is_file() {
        return file
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("")
            .to_owned();
    }
    return file
        .strip_prefix(root)
        .unwrap_or(file)
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
}

fn audit(args: &[String]) -> Result<AuditEvidence> {
    validate_options(args, &["--input", "--evidence"])?;
    let root = PathBuf::from(required(args, "--input")?);
    let files = parquet_files(&root)?;
    let mut results = Vec::with_capacity(files.len());
    let mut supported = 0_usize;
    let mut unsupported = 0_usize;
    let panic_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    for file in files {
        let name = audit_name(&root, &file);
        let inspected = panic::catch_unwind(AssertUnwindSafe(|| inspect_file(&file, &name)));
        match inspected {
            Ok(Ok(evidence)) => {
                results.push(AuditFileEvidence {
                    status: "supported".to_owned(),
                    file: name,
                    evidence: Some(evidence),
                    error: None,
                });
                supported += 1;
            }
            Ok(Err(error)) => {
                let path = file.to_string_lossy();
                let message = error.to_string().replace(path.as_ref(), "<file>");
                results.push(AuditFileEvidence {
                    status: "unsupported".to_owned(),
                    file: name,
                    evidence: None,
                    error: Some(message),
                });
                unsupported += 1;
            }
            Err(payload) => {
                let message = if let Some(value) = payload.downcast_ref::<String>() {
                    value.clone()
                } else if let Some(value) = payload.downcast_ref::<&str>() {
                    (*value).to_owned()
                } else {
                    "non-string panic".to_owned()
                };
                results.push(AuditFileEvidence {
                    status: "unsupported".to_owned(),
                    file: name,
                    evidence: None,
                    error: Some(format!("panic: {message}")),
                });
                unsupported += 1;
            }
        }
    }
    panic::set_hook(panic_hook);
    return Ok(AuditEvidence {
        evidence_version: 1,
        oracle: ToolEvidence::current()?,
        action: "audit".to_owned(),
        file_count: results.len(),
        supported_count: supported,
        unsupported_count: unsupported,
        files: results,
    });
}

fn generate(args: &[String]) -> Result<OracleEvidence> {
    validate_options(args, &["--output", "--evidence"])?;
    let directory = PathBuf::from(required(args, "--output")?);
    let files = generate_all(&directory)?;
    let mut evidence = Vec::with_capacity(files.len());
    for (case, version, path) in files {
        let item = inspect_file(&path, case.id())?;
        verify_owned(case, version, &item)?;
        evidence.push(item);
    }
    return OracleEvidence::new("generate", evidence);
}

fn inspect(args: &[String]) -> Result<OracleEvidence> {
    validate_options(args, &["--input", "--case-id", "--evidence"])?;
    let path = PathBuf::from(required(args, "--input")?);
    let case_id = required(args, "--case-id")?;
    let evidence = inspect_file(&path, &case_id)?;
    return OracleEvidence::new("inspect", vec![evidence]);
}

fn diagnose_rule3_near_neighbor(args: &[String]) -> Result<OracleEvidence> {
    validate_options(args, &["--output", "--evidence"])?;
    let directory = PathBuf::from(required(args, "--output")?);
    let files = generate_list_rule3_near_neighbor(&directory)?;
    let mut evidence = Vec::with_capacity(files.len());
    for (version, path) in files {
        let item = inspect_file(&path, LIST_RULE3_NEAR_NEIGHBOR)?;
        verify_list_rule3_near_neighbor(version, &item)?;
        evidence.push(item);
    }
    return OracleEvidence::new("diagnose-rule3-near-neighbor", evidence);
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ExpectedEvidence {
    File(FileEvidence),
    Report(OracleEvidence),
}

fn read_expected(path: &Path, actual: &FileEvidence) -> Result<FileEvidence> {
    let file = File::open(path)?;
    let expected: ExpectedEvidence = serde_json::from_reader(file)?;
    return match expected {
        ExpectedEvidence::File(file) => Ok(file),
        ExpectedEvidence::Report(report) => report
            .files
            .into_iter()
            .find(|file| file.case_id == actual.case_id && file.file_name == actual.file_name)
            .ok_or_else(|| "expected report has no matching file evidence".into()),
    };
}

fn verify(args: &[String]) -> Result<OracleEvidence> {
    validate_options(args, &["--input", "--expected", "--case-id", "--evidence"])?;
    let path = PathBuf::from(required(args, "--input")?);
    let expected_path = PathBuf::from(required(args, "--expected")?);
    let case_id = required(args, "--case-id")?;
    let actual = inspect_file(&path, &case_id)?;
    let expected = read_expected(&expected_path, &actual)?;
    if actual != expected {
        return Err(format!("oracle evidence differs from {}", expected_path.display()).into());
    }
    return OracleEvidence::new("verify", vec![actual]);
}

fn run() -> Result<()> {
    let arguments: Vec<String> = env::args().skip(1).collect();
    let (command, args) = arguments
        .split_first()
        .ok_or_else(|| -> DynError { usage().into() })?;
    if command == "audit" {
        let evidence = audit(args)?;
        output_audit(&evidence, option(args, "--evidence")?)?;
        return Ok(());
    }
    let evidence = match command.as_str() {
        "generate" => generate(args)?,
        "diagnose-rule3-near-neighbor" => diagnose_rule3_near_neighbor(args)?,
        "inspect" => inspect(args)?,
        "verify" => verify(args)?,
        "versions" => {
            validate_options(args, &["--evidence"])?;
            OracleEvidence::new("versions", vec![])?
        }
        _ => return Err(usage().into()),
    };
    output_evidence(&evidence, option(args, "--evidence")?)?;
    return Ok(());
}

fn main() {
    if let Err(error) = run() {
        eprintln!("arrow-rs-oracle: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::io::ErrorKind;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::evidence::inspect_file;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Result<Self> {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            for _ in 0..16 {
                let id = NEXT.fetch_add(1, Ordering::Relaxed);
                let path = std::env::temp_dir().join(format!(
                    "parquet-jl-n5-arrow-rs-{}-{id}",
                    std::process::id()
                ));
                match std::fs::create_dir(&path) {
                    Ok(()) => return Ok(Self(path)),
                    Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error.into()),
                }
            }
            return Err("cannot create a unique test directory".into());
        }

        fn path(&self) -> &Path {
            return &self.0;
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn owned_fixtures_are_deterministic_and_self_verifying() -> Result<()> {
        let first = TestDirectory::new()?;
        let first_files = generate_all(first.path())?;
        assert_eq!(first_files.len(), 6);
        let mut first_hashes = BTreeSet::new();
        for (case, version, path) in &first_files {
            let file = inspect_file(path, case.id())?;
            verify_owned(*case, *version, &file)?;
            first_hashes.insert((file.file_name, file.sha256));
        }
        assert_eq!(first_hashes.len(), 6);
        let mut first_near_neighbor_hashes = BTreeSet::new();
        for (version, path) in generate_list_rule3_near_neighbor(first.path())? {
            let file = inspect_file(&path, LIST_RULE3_NEAR_NEIGHBOR)?;
            verify_list_rule3_near_neighbor(version, &file)?;
            first_near_neighbor_hashes.insert((file.file_name, file.sha256));
        }
        assert_eq!(first_near_neighbor_hashes.len(), 2);

        let second = TestDirectory::new()?;
        let second_files = generate_all(second.path())?;
        let mut second_hashes = BTreeSet::new();
        for (case, version, path) in &second_files {
            let file = inspect_file(path, case.id())?;
            verify_owned(*case, *version, &file)?;
            second_hashes.insert((file.file_name, file.sha256));
        }
        assert_eq!(first_hashes, second_hashes);
        let mut second_near_neighbor_hashes = BTreeSet::new();
        for (version, path) in generate_list_rule3_near_neighbor(second.path())? {
            let file = inspect_file(&path, LIST_RULE3_NEAR_NEIGHBOR)?;
            verify_list_rule3_near_neighbor(version, &file)?;
            second_near_neighbor_hashes.insert((file.file_name, file.sha256));
        }
        assert_eq!(first_near_neighbor_hashes, second_near_neighbor_hashes);
        return Ok(());
    }

    #[test]
    fn audit_records_supported_and_unsupported_files() -> Result<()> {
        let directory = TestDirectory::new()?;
        let generated = generate_all(directory.path())?;
        let retained = generated
            .first()
            .ok_or_else(|| -> DynError { "generated fixture list is empty".into() })?
            .2
            .clone();
        for (_, _, path) in generated.iter().skip(1) {
            fs::remove_file(path)?;
        }
        let invalid = directory.path().join("invalid.parquet");
        fs::write(&invalid, [0_u8, 1, 2, 3])?;

        let report = audit(&["--input".to_owned(), directory.path().display().to_string()])?;
        assert_eq!(report.file_count, 2);
        assert_eq!(report.supported_count, 1);
        assert_eq!(report.unsupported_count, 1);
        let invalid_result = report
            .files
            .iter()
            .find(|file| file.file == "invalid.parquet")
            .ok_or_else(|| -> DynError { "invalid audit result is absent".into() })?;
        assert_eq!(invalid_result.status, "unsupported");
        assert!(invalid_result.evidence.is_none());
        assert!(invalid_result.error.is_some());
        let retained_name = retained.file_name().unwrap().to_string_lossy();
        let valid_result = report
            .files
            .iter()
            .find(|file| file.file == retained_name)
            .ok_or_else(|| -> DynError { "valid audit result is absent".into() })?;
        assert_eq!(valid_result.status, "supported");
        assert!(valid_result.evidence.is_some());
        assert!(valid_result.error.is_none());
        return Ok(());
    }
}
