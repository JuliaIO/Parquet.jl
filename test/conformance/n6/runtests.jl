using SHA
using Test
using TOML

const N6_DIR = @__DIR__
const REPO_DIR = normpath(joinpath(N6_DIR, "..", "..", ".."))
const SHA256_PATTERN = r"^[0-9a-f]{64}$"
const GIT_PATTERN = r"^[0-9a-f]{40}$"
const CASE_PATTERN = r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$"
const CPYTHON_312_DISTRIBUTION_URL = "https://github.com/astral-sh/" *
    "python-build-standalone/releases/download/20250115/" *
    "cpython-3.12.8%2B20250115-aarch64-apple-darwin-" *
    "install_only_stripped.tar.gz"
const CPYTHON_CLEAN_TREE_POLICY = "extract-strip-site-packages-bytecode-v1"
const CPYTHON_314_DISTRIBUTION_URL = "https://github.com/astral-sh/" *
    "python-build-standalone/releases/download/20260127/" *
    "cpython-3.14.2%2B20260127-aarch64-apple-darwin-" *
    "install_only_stripped.tar.gz"
const VALIDATOR_CLEAN_TREE_POLICY =
    "extract-strip-site-packages-bytecode-v1"
const CPYTHON_DISTRIBUTION_ARCHIVE_LIMIT = 128 * 1024 * 1024
const VALIDATOR_DISTRIBUTION_ARCHIVE_LIMIT = 64 * 1024 * 1024
const CONTROL_FILE_LIMIT = 64 * 1024 * 1024
const RAW_JAVA_DOWNLOAD_LIMIT = 256 * 1024 * 1024
const RAW_JAVA_DOWNLOAD_TOTAL_LIMIT = 256 * 1024 * 1024
const VALIDATOR_TREE_ENTRY_LIMIT = 5_000
const VALIDATOR_TREE_FILE_LIMIT = 64 * 1024 * 1024
const VALIDATOR_TREE_TOTAL_LIMIT = 128 * 1024 * 1024
const JDK_TREE_ENTRY_LIMIT = 2_000
const JDK_TREE_FILE_LIMIT = 256 * 1024 * 1024
const JDK_TREE_TOTAL_LIMIT = 512 * 1024 * 1024
const GATE_EXECUTABLE_FILES = Set([
    "test/conformance/n6/oracles/arrow-rs/build/" *
        "parquet-jl-n6-arrow-rs-metadata",
])

struct FileSnapshot
    path::String
    payload::Vector{UInt8}
    sha256::String
end

function file_sha256(file::AbstractString)
    return bytes2hex(open(SHA.sha256, file))
end

function require_gate(condition::Bool, message::AbstractString)
    condition || error(message)
    return
end

function read_bounded_regular_bytes(file::AbstractString, limit::Int)
    limit > 0 || throw(ArgumentError("file byte limit must be positive"))
    io = open(file, "r")
    try
        before = stat(io)
        0 < before.size <= limit || error("input has an invalid byte size: $file")
        payload = read(io, limit + 1)
        length(payload) <= limit || error("input exceeds its byte limit: $file")
        length(payload) == before.size || error("input changed size while read: $file")
        after = stat(io)
        identity(value) = (value.device, value.inode, value.size, value.mtime,
            value.ctime)
        identity(before) == identity(after) ||
            error("input changed while read: $file")
        return payload
    finally
        close(io)
    end
end

function file_snapshot(relative::AbstractString)
    safe_relative(relative) || error("unsafe control path: $relative")
    path = checked_file(REPO_DIR, relative)
    payload = read_bounded_regular_bytes(path, CONTROL_FILE_LIMIT)
    return FileSnapshot(path, payload, bytes2hex(SHA.sha256(payload)))
end

function parse_toml_snapshot(snapshot::FileSnapshot)
    return TOML.parse(String(copy(snapshot.payload)))
end

function control_snapshot(snapshots::Dict{String,FileSnapshot},
        relative::AbstractString)
    haskey(snapshots, relative) || error("control snapshot is absent: $relative")
    return snapshots[relative]
end

function load_control_snapshots(manifest)
    paths = Set{String}([
        manifest["plan_file"],
        manifest["capabilities_file"],
        manifest["fixture_manifest_file"],
        manifest["corpus_manifest_file"],
        manifest["evidence_schema_file"],
        manifest["artifact_manifest_file"],
        manifest["model_producer_descriptor_file"],
        manifest["parquet_jl_producer_descriptor_file"],
        "thrift/parquet.thrift",
        "test/conformance/n6/model/cases.toml",
        "test/conformance/n6/toolchains/pyarrow.toml",
        "test/conformance/n6/toolchains/duckdb.toml",
        "test/conformance/n6/oracles/arrow-rs/toolchain.toml",
        "test/conformance/n6/oracles/parquet-java/toolchain.toml",
    ])
    snapshots = Dict{String,FileSnapshot}()
    for relative in paths
        relative isa String || error("control path is not a string")
        snapshots[relative] = file_snapshot(relative)
    end
    require_gate(control_snapshot(snapshots,
        "thrift/parquet.thrift").sha256 == toolchain_artifact(manifest,
            "raw-java", "parquet-2.13-idl"),
        "local raw scanner IDL hash differs")
    artifact_manifest = control_snapshot(snapshots,
        manifest["artifact_manifest_file"])
    for (relative, digest) in artifact_entries(artifact_manifest.payload)
        repository_relative = "test/conformance/n6/" * relative
        snapshot = get!(() -> file_snapshot(repository_relative), snapshots,
            repository_relative)
        require_gate(snapshot.sha256 == digest,
            "artifact hash differs: $repository_relative")
    end
    producer_descriptor = parse_toml_snapshot(control_snapshot(snapshots,
        manifest["parquet_jl_producer_descriptor_file"]))
    for item in producer_descriptor["file"]
        relative = item["path"]
        snapshot = get!(() -> file_snapshot(relative), snapshots, relative)
        require_gate(snapshot.sha256 == item["sha256"],
            "Parquet.jl producer file hash differs: $relative")
    end
    producer_manifest = producer_descriptor["manifest_file"]
    snapshots["Manifest.toml"] = control_snapshot(snapshots, producer_manifest)
    return snapshots
end

function load_generated_snapshots!(snapshots, fixtures)
    for generated in fixtures["generated_case"]
        relative = "test/conformance/n6/" * generated["output_file"]
        snapshot = file_snapshot(relative)
        require_gate(snapshot.sha256 == generated["output_sha256"],
            "generated fixture hash differs: " * generated["id"])
        snapshots[relative] = snapshot
    end
    return
end

function load_evidence_snapshots!(snapshots, manifest)
    for evidence in vcat(manifest["frozen_evidence"],
            manifest["planned_evidence"])
        relative = evidence["file"]
        if !ispath(joinpath(REPO_DIR, relative))
            evidence["storage"] == "gate-generated" ||
                error("evidence file is absent: $relative")
            continue
        end
        snapshot = file_snapshot(relative)
        if evidence["status"] == "verified" &&
                evidence["storage"] == "checked-in"
            require_gate(snapshot.sha256 == evidence["sha256"],
                "frozen evidence hash differs: " * evidence["id"])
        end
        snapshots[relative] = snapshot
    end
    return
end

function load_oracle_build_snapshots!(snapshots, manifest)
    validate_oracle_build_root(joinpath(N6_DIR, "oracles", "arrow-rs"),
        "Arrow Rust"; reject_nested_symlinks=true)
    validate_oracle_build_root(joinpath(N6_DIR, "oracles", "parquet-java"),
        "Parquet Java"; reject_nested_symlinks=true)
    files = [
        ("test/conformance/n6/oracles/arrow-rs/build/" *
            "parquet-jl-n6-arrow-rs-metadata", "rust-arrow-rs",
            "parquet-jl-n6-arrow-rs-metadata"),
        ("test/conformance/n6/oracles/parquet-java/build/artifacts/" *
            "hadoop-client-api-3.3.0.jar", "parquet-java-interop",
            "hadoop-client-api-3.3.0.jar"),
        ("test/conformance/n6/oracles/parquet-java/build/artifacts/" *
            "hadoop-client-runtime-3.3.0.jar", "parquet-java-interop",
            "hadoop-client-runtime-3.3.0.jar"),
        ("test/conformance/n6/oracles/parquet-java/build/artifacts/" *
            "parquet-cli-1.17.1-runtime.jar", "parquet-java-interop",
            "parquet-cli-1.17.1-runtime.jar"),
        ("test/conformance/n6/oracles/parquet-java/build/artifacts/" *
            "parquet-java-n6-harness.jar", "parquet-java-interop",
            "parquet-java-n6-harness.jar"),
    ]
    for (relative, toolchain, artifact) in files
        snapshot = file_snapshot(relative)
        require_gate(snapshot.sha256 == toolchain_artifact(manifest,
            toolchain, artifact), "oracle build artifact differs: $artifact")
        snapshots[relative] = snapshot
    end
    return
end

function set_repository_snapshot_modes!(root::AbstractString, locked::Bool)
    directories = String[]
    for (directory, children, files) in walkdir(root; follow_symlinks=false)
        push!(directories, directory)
        for name in files
            file = joinpath(directory, name)
            islink(file) && error("gate snapshot file is a symbolic link")
            chmod(file, locked ? 0o400 : 0o600)
        end
        for name in children
            islink(joinpath(directory, name)) &&
                error("gate snapshot directory is a symbolic link")
        end
    end
    for directory in reverse(directories)
        chmod(directory, locked ? 0o500 : 0o700)
    end
    return
end

function with_gate_repository(f, snapshots;
        writable_directories::Vector{String}=String[])
    return mktempdir() do directory
        root = joinpath(directory, "repository")
        mkdir(root)
        root = realpath(root)
        for (relative, snapshot) in snapshots
            safe_relative(relative) || error("unsafe gate snapshot path: $relative")
            target = joinpath(root, relative)
            mkpath(dirname(target))
            open(target, "w") do io
                write(io, snapshot.payload)
                return
            end
        end
        set_repository_snapshot_modes!(root, true)
        for relative in GATE_EXECUTABLE_FILES
            haskey(snapshots, relative) || continue
            chmod(gate_file(root, relative), 0o500)
        end
        writable_inventory = Dict{String,Vector{String}}()
        for relative in writable_directories
            safe_relative(relative) ||
                error("unsafe writable snapshot path: $relative")
            haskey(writable_inventory, relative) &&
                error("duplicate writable snapshot path: $relative")
            target = checked_directory(root, relative)
            writable_inventory[relative] = sort(readdir(target))
            chmod(target, 0o700)
        end
        try
            result = f(root)
            for (relative, inventory) in writable_inventory
                target = checked_directory(root, relative)
                require_gate(sort(readdir(target)) == inventory,
                    "writable gate snapshot inventory changed: $relative")
            end
            for (relative, snapshot) in snapshots
                require_gate(file_sha256(gate_file(root, relative)) ==
                    snapshot.sha256, "gate snapshot changed: $relative")
                require_gate(file_sha256(snapshot.path) == snapshot.sha256,
                    "canonical input changed: $relative")
            end
            return result
        finally
            set_repository_snapshot_modes!(root, false)
        end
    end
end

function gate_file(root::AbstractString, relative::AbstractString)
    return checked_file(root, relative)
end

function require_keys(value::AbstractDict, allowed, required=allowed)
    actual = Set(keys(value))
    @test isempty(setdiff(actual, Set(allowed)))
    @test isempty(setdiff(Set(required), actual))
    return
end

function safe_relative(file::AbstractString)
    isempty(file) && return false
    isabspath(file) && return false
    occursin('\\', file) && return false
    all(character -> isascii(character) &&
        (isletter(character) || isdigit(character) ||
        character in ('_', '.', '+', '@', '=', '-', '/')), file) || return false
    parts = split(file, '/')
    any(part -> isempty(part) || part in (".", ".."), parts) && return false
    any(character -> character < ' ', file) && return false
    return normpath(file) == file
end

function safe_evidence_file(file::AbstractString)
    prefix = "test/conformance/n6/evidence/"
    return safe_relative(file) && startswith(file, prefix) &&
        length(file) > length(prefix) && endswith(basename(file), ".jsonl") &&
        basename(file) != ".jsonl"
end

function safe_generated_output(file::AbstractString)
    prefix = "generated/"
    return safe_relative(file) && startswith(file, prefix) &&
        length(file) > length(prefix) && endswith(basename(file), ".parquet") &&
        basename(file) != ".parquet"
end

function checked_file(root::AbstractString, relative::AbstractString)
    safe_relative(relative) || error("unsafe relative path: $relative")
    root_path = realpath(root)
    candidate = joinpath(root_path, relative)
    isfile(candidate) || error("missing file: $relative")
    islink(candidate) && error("pinned file is a symbolic link: $relative")
    resolved = realpath(candidate)
    startswith(resolved, root_path * Base.Filesystem.path_separator) ||
        error("path escapes its source root: $relative")
    return resolved
end

function checked_directory_files(root::AbstractString, expected::Vector{String})
    require_gate(sort(readdir(root)) == sort(expected),
        "directory contents differ from the pinned inventory: $root")
    return [checked_file(root, name) for name in expected]
end

function artifact_entries(payload::Vector{UInt8})
    entries = Pair{String,String}[]
    for (line_number, line) in enumerate(eachline(IOBuffer(payload)))
        match_result = match(r"^([0-9a-f]{64})  ([A-Za-z0-9._+@=/\-]+)$", line)
        match_result === nothing && error("invalid artifact line $line_number")
        digest, relative = match_result.captures
        safe_relative(relative) || error("unsafe artifact path: $relative")
        push!(entries, relative => digest)
    end
    paths = first.(entries)
    paths == sort(paths) || error("artifact paths are not sorted")
    length(paths) == length(unique(paths)) || error("duplicate artifact path")
    return entries
end

function validate_oracle_build_root(oracle_dir::AbstractString,
        oracle_name::AbstractString; reject_nested_symlinks::Bool=false)
    oracle_root = realpath(oracle_dir)
    build = joinpath(oracle_root, "build")
    (ispath(build) || islink(build)) || return build
    islink(build) && error("$oracle_name build root is a symbolic link")
    isdir(build) || error("$oracle_name build root is not a directory")
    resolved = realpath(build)
    startswith(resolved, oracle_root * Base.Filesystem.path_separator) ||
        error("$oracle_name build root escapes its oracle directory")
    if reject_nested_symlinks
        for (directory, directories, files) in walkdir(resolved;
                follow_symlinks=false)
            for name in vcat(directories, files)
                nested = joinpath(directory, name)
                islink(nested) || continue
                error("$oracle_name build cache contains a symbolic link: " *
                    relpath(nested, resolved))
            end
        end
    end
    return resolved
end

function validate_raw_build_root(raw_dir::AbstractString)
    return validate_oracle_build_root(raw_dir, "raw Java")
end

function intended_artifacts(root::AbstractString=N6_DIR;
        mutable_files::Set{String}=Set{String}())
    paths = String[]
    ignored_builds = Set([
        validate_oracle_build_root(joinpath(root, "oracles", "arrow-rs"),
            "Arrow Rust"; reject_nested_symlinks=true),
        validate_oracle_build_root(joinpath(root, "oracles", "raw-java"),
            "raw Java"),
        validate_oracle_build_root(joinpath(root, "oracles", "parquet-java"),
            "Parquet Java"; reject_nested_symlinks=true),
    ])
    for (directory, directories, files) in walkdir(root; follow_symlinks=false)
        for name in directories
            absolute = joinpath(directory, name)
            normpath(absolute) in ignored_builds && continue
            islink(absolute) && error("unlisted N6 directory symbolic link: " *
                relpath(absolute, root))
        end
        filter!(name -> normpath(joinpath(directory, name)) ∉ ignored_builds,
            directories)
        for name in files
            absolute = joinpath(directory, name)
            islink(absolute) && error("unlisted N6 file symbolic link: " *
                relpath(absolute, root))
            relative = relpath(absolute, root)
            relative in ("artifacts.sha256", "manifest.toml") && continue
            portable = replace(relative, Base.Filesystem.path_separator => '/')
            portable in mutable_files && continue
            push!(paths, portable)
        end
    end
    sort!(paths)
    return paths
end

function tree_sha256(root::AbstractString; excluded::Union{Nothing,String}=nothing)
    root_path = realpath(root)
    entries = String[]
    for (directory, directories, files) in walkdir(root_path; follow_symlinks=false)
        for name in vcat(directories, files)
            absolute = joinpath(directory, name)
            (isfile(absolute) || islink(absolute)) || continue
            relative = replace(relpath(absolute, root_path), Base.Filesystem.path_separator => '/')
            if excluded !== nothing &&
                    (relative == excluded || startswith(relative, excluded * "/"))
                continue
            end
            push!(entries, relative)
        end
    end
    sort!(entries)
    buffer = IOBuffer()
    for relative in entries
        absolute = joinpath(root_path, relative)
        if islink(absolute)
            write(buffer, "L\0", relative, "\0", readlink(absolute), '\n')
        else
            write(buffer, "F\0", relative, "\0", file_sha256(absolute), '\n')
        end
    end
    return bytes2hex(SHA.sha256(take!(buffer)))
end

function validate_bounded_tree(root::AbstractString; max_entries::Int,
        max_file_bytes::Int, max_total_bytes::Int)
    max_entries > 0 || throw(ArgumentError("tree entry limit must be positive"))
    max_file_bytes > 0 ||
        throw(ArgumentError("tree file byte limit must be positive"))
    max_total_bytes >= max_file_bytes ||
        throw(ArgumentError("tree total byte limit is too small"))
    islink(root) && error("tree root is a symbolic link: $root")
    isdir(root) || error("tree root is not a directory: $root")
    root_path = realpath(root)
    entry_count = 0
    total_bytes = 0
    for (directory, directories, files) in walkdir(root_path;
            follow_symlinks=false)
        for name in vcat(directories, files)
            absolute = joinpath(directory, name)
            relative = replace(relpath(absolute, root_path),
                Base.Filesystem.path_separator => '/')
            entry_count < max_entries ||
                error("tree exceeds its entry limit: $root_path")
            entry_count += 1
            metadata = lstat(absolute)
            kind = metadata.mode & Base.Filesystem.S_IFMT
            if kind == Base.Filesystem.S_IFLNK
                target = readlink(absolute)
                isabspath(target) &&
                    error("tree has an absolute symbolic link: $relative")
                target_path = normpath(joinpath(dirname(absolute), target))
                startswith(target_path,
                    root_path * Base.Filesystem.path_separator) ||
                    error("tree symbolic link escapes its root: $relative")
                ispath(absolute) ||
                    error("tree has a broken symbolic link: $relative")
                resolved = realpath(absolute)
                startswith(resolved,
                    root_path * Base.Filesystem.path_separator) ||
                    error("tree symbolic link resolves outside its root: $relative")
            elseif kind == Base.Filesystem.S_IFDIR
                continue
            elseif kind == Base.Filesystem.S_IFREG
                metadata.size <= max_file_bytes ||
                    error("tree file exceeds its byte limit: $relative")
                metadata.size <= max_total_bytes - total_bytes ||
                    error("tree exceeds its total byte limit: $root_path")
                total_bytes += metadata.size
            else
                error("tree contains a special file: $relative")
            end
        end
    end
    return (entries=entry_count, bytes=total_bytes,
        sha256=tree_sha256(root_path))
end

function toolchain_artifact(manifest, toolchain_id::AbstractString,
        artifact_name::AbstractString)
    toolchain = only(filter(item -> item["id"] == toolchain_id,
        manifest["toolchain"]))
    artifact = only(filter(item -> item["name"] == artifact_name,
        toolchain["artifacts"]))
    return artifact["sha256"]
end

function validate_manifest_header(manifest)
    require_keys(manifest, [
        "manifest_version", "gate", "status", "plan_file", "plan_sha256",
        "capabilities_file", "capabilities_sha256", "fixture_manifest_file",
        "fixture_manifest_sha256", "corpus_manifest_file",
        "corpus_manifest_sha256", "evidence_schema_file",
        "evidence_schema_sha256", "artifact_manifest_file",
        "artifact_manifest_sha256", "model_producer_descriptor_file",
        "model_producer_descriptor_sha256",
        "parquet_jl_producer_descriptor_file",
        "parquet_jl_producer_descriptor_sha256",
        "parquet_jl_source_composite_sha256", "supported_platforms",
        "publication_authorized", "oracle_lock_authorized", "evidence_limits",
        "source", "toolchain", "frozen_model", "frozen_evidence",
        "planned_evidence",
    ])
    @test manifest["manifest_version"] == 1
    @test manifest["gate"] == "n6-a-preproduction"
    @test manifest["status"] == "preproduction"
    @test manifest["supported_platforms"] == ["macos-15-arm64"]
    @test manifest["publication_authorized"] === false
    @test manifest["oracle_lock_authorized"] === false
    @test all(field -> manifest[field] isa String, (
        "gate", "status", "plan_file", "plan_sha256", "capabilities_file",
        "capabilities_sha256", "fixture_manifest_file",
        "fixture_manifest_sha256", "corpus_manifest_file",
        "corpus_manifest_sha256", "evidence_schema_file",
        "evidence_schema_sha256", "artifact_manifest_file",
        "artifact_manifest_sha256", "model_producer_descriptor_file",
        "model_producer_descriptor_sha256",
        "parquet_jl_producer_descriptor_file",
        "parquet_jl_producer_descriptor_sha256",
        "parquet_jl_source_composite_sha256"))
    @test safe_relative(manifest["model_producer_descriptor_file"])
    @test safe_relative(manifest["parquet_jl_producer_descriptor_file"])
    @test all(value -> value isa String, manifest["supported_platforms"])
    limits = manifest["evidence_limits"]
    require_keys(limits, ["max_inputs", "max_file_bytes", "max_total_bytes",
        "max_line_bytes", "max_records_per_input", "max_records_total"])
    @test all(value -> value isa Int64, values(limits))
    @test limits["max_inputs"] > 0
    @test limits["max_line_bytes"] > 0
    @test limits["max_file_bytes"] >= limits["max_line_bytes"]
    @test limits["max_total_bytes"] >= limits["max_file_bytes"]
    @test limits["max_records_per_input"] > 0
    @test limits["max_records_total"] >= limits["max_records_per_input"]
    for key in keys(manifest)
        endswith(key, "_sha256") || continue
        @test occursin(SHA256_PATTERN, manifest[key])
    end
    return
end

function validate_manifest_sources(sources)
    for source in sources
        require_keys(source, ["id", "url", "version", "tag", "tag_revision",
            "revision", "status", "root_env", "files"])
        @test all(field -> source[field] isa String, (
            "id", "url", "version", "tag", "tag_revision", "revision",
            "status", "root_env"))
        @test occursin(CASE_PATTERN, source["id"])
        @test source["status"] in ("verified", "planned")
        @test startswith(source["url"], "https://github.com/")
        @test occursin(GIT_PATTERN, source["revision"])
        @test isempty(source["tag_revision"]) ||
            occursin(GIT_PATTERN, source["tag_revision"])
        for pinned in source["files"]
            require_keys(pinned, ["file", "sha256"])
            @test pinned["file"] isa String
            @test pinned["sha256"] isa String
            @test safe_relative(pinned["file"])
            @test occursin(SHA256_PATTERN, pinned["sha256"])
        end
    end
    @test length(sources) == length(unique(item["id"] for item in sources))
    return
end

function validate_manifest_toolchains(toolchains)
    required = ["id", "status", "version", "platform", "scope", "artifacts"]
    allowed = vcat(required, ["distribution_url", "tree_policy"])
    for toolchain in toolchains
        require_keys(toolchain, allowed, required)
        @test all(field -> toolchain[field] isa String,
            ("id", "status", "version", "platform", "scope"))
        @test toolchain["status"] in ("verified", "planned")
        @test !isempty(toolchain["artifacts"])
        names = String[]
        for artifact in toolchain["artifacts"]
            require_keys(artifact, ["name", "sha256"])
            @test artifact["name"] isa String
            @test artifact["sha256"] isa String
            @test occursin(SHA256_PATTERN, artifact["sha256"])
            push!(names, artifact["name"])
        end
        @test length(names) == length(unique(names))
    end
    @test length(toolchains) == length(unique(item["id"] for item in toolchains))
    validator = only(filter(item -> item["id"] == "jsonschema-validator",
        toolchains))
    @test validator["distribution_url"] == CPYTHON_314_DISTRIBUTION_URL
    @test validator["tree_policy"] == VALIDATOR_CLEAN_TREE_POLICY
    return
end

function validate_manifest_models(model_files)
    for model_file in model_files
        require_keys(model_file, ["file", "sha256"])
        @test model_file["file"] isa String
        @test model_file["sha256"] isa String
        @test safe_relative(model_file["file"])
        @test occursin(SHA256_PATTERN, model_file["sha256"])
    end
    return
end

function validate_frozen_evidence(evidence)
    allowed = ["id", "status", "authority",
        "toolchain_sha256", "file", "format", "storage", "schema_file",
        "schema_sha256", "fixture_manifest_file", "case_count",
        "record_count", "sha256", "upstream_evidence", "scope"]
    require_keys(evidence, allowed, setdiff(allowed, ["upstream_evidence"]))
    @test all(field -> evidence[field] isa String, (
        "id", "status", "authority", "toolchain_sha256", "file",
        "format", "storage", "schema_file", "schema_sha256",
        "fixture_manifest_file", "sha256", "scope"))
    @test occursin(CASE_PATTERN, evidence["id"])
    @test evidence["status"] == "verified"
    @test occursin(SHA256_PATTERN, evidence["toolchain_sha256"])
    @test safe_evidence_file(evidence["file"])
    @test evidence["format"] in ("raw-jsonl", "normalized-jsonl")
    @test evidence["storage"] in ("checked-in", "gate-generated")
    @test safe_relative(evidence["schema_file"])
    @test safe_relative(evidence["fixture_manifest_file"])
    @test occursin(SHA256_PATTERN, evidence["schema_sha256"])
    @test occursin(SHA256_PATTERN, evidence["sha256"])
    @test evidence["case_count"] isa Int64
    @test evidence["record_count"] isa Int64
    @test evidence["case_count"] > 0
    @test evidence["record_count"] >= evidence["case_count"]
    @test !isempty(evidence["scope"])
    upstream = get(evidence, "upstream_evidence", String[])
    @test all(item -> item isa String && occursin(CASE_PATTERN, item), upstream)
    @test length(upstream) == length(unique(upstream))
    @test evidence["id"] ∉ upstream
    return evidence["id"], evidence["file"]
end

function validate_planned_evidence(evidence, manifest)
    allowed = ["id", "status", "authority", "file", "format",
        "schema_file", "fixture_manifest_file", "upstream_evidence", "scope"]
    require_keys(evidence, allowed, setdiff(allowed, ["upstream_evidence"]))
    @test all(field -> evidence[field] isa String, (
        "id", "status", "authority", "file", "format", "schema_file",
        "fixture_manifest_file", "scope"))
    @test occursin(CASE_PATTERN, evidence["id"])
    @test evidence["status"] == "planned"
    @test safe_evidence_file(evidence["file"])
    @test evidence["format"] == "normalized-jsonl"
    @test safe_relative(evidence["schema_file"])
    @test safe_relative(evidence["fixture_manifest_file"])
    @test evidence["schema_file"] == manifest["evidence_schema_file"]
    @test evidence["fixture_manifest_file"] == manifest["fixture_manifest_file"]
    @test !isempty(evidence["scope"])
    upstream = get(evidence, "upstream_evidence", String[])
    @test all(item -> item isa String && occursin(CASE_PATTERN, item), upstream)
    @test length(upstream) == length(unique(upstream))
    @test evidence["id"] ∉ upstream
    return evidence["id"], evidence["file"]
end

function validate_manifest_evidence(manifest)
    evidence_ids = String[]
    evidence_files = String[]
    for evidence in manifest["frozen_evidence"]
        id, file = validate_frozen_evidence(evidence)
        push!(evidence_ids, id)
        push!(evidence_files, file)
    end
    for evidence in manifest["planned_evidence"]
        id, file = validate_planned_evidence(evidence, manifest)
        push!(evidence_ids, id)
        push!(evidence_files, file)
    end
    @test length(evidence_ids) == length(unique(evidence_ids))
    @test length(evidence_files) == length(unique(evidence_files))
    known = Set(evidence_ids)
    for evidence in vcat(manifest["frozen_evidence"],
            manifest["planned_evidence"])
        @test all(item -> item in known,
            get(evidence, "upstream_evidence", String[]))
    end
    return
end

function validate_manifest(manifest)
    validate_manifest_header(manifest)
    validate_manifest_sources(manifest["source"])
    validate_manifest_toolchains(manifest["toolchain"])
    validate_manifest_models(manifest["frozen_model"])
    validate_manifest_evidence(manifest)
    return
end

function validate_capability_catalog(capabilities, manifest)
    require_keys(capabilities, ["matrix_version", "plan_sha256",
        "fixture_manifest", "unsupported_is_pass", "statuses", "capability",
        "authority"])
    @test capabilities["matrix_version"] == 2
    @test capabilities["unsupported_is_pass"] === false
    @test capabilities["plan_sha256"] isa String
    @test capabilities["plan_sha256"] == manifest["plan_sha256"]
    @test capabilities["fixture_manifest"] isa String
    @test capabilities["fixture_manifest"] ==
        basename(manifest["fixture_manifest_file"])
    @test capabilities["statuses"] ==
        ["verified", "planned", "unsupported", "not_assessed"]
    @test all(value -> value isa String, capabilities["statuses"])
    capability_ids = Set{String}()
    for capability in capabilities["capability"]
        require_keys(capability, ["id", "kind"])
        @test capability["id"] isa String
        @test capability["kind"] isa String
        @test occursin(CASE_PATTERN, capability["id"])
        @test capability["kind"] in ("wire", "semantic", "runtime", "compatibility")
        @test capability["id"] ∉ capability_ids
        push!(capability_ids, capability["id"])
    end
    return capability_ids
end

function capability_case_context(fixtures, model_cases)
    case_capabilities = Dict(item["id"] => Set(item["capabilities"])
        for item in vcat(fixtures["fixture"], fixtures["generated_case"]))
    for item in model_cases["case_groups"]
        @test !haskey(case_capabilities, item["id"])
        case_capabilities[item["id"]] = Set(item["capabilities"])
    end
    return case_capabilities, Set(keys(case_capabilities))
end

function validate_authority_claim(claim, capabilities, capability_ids, known_cases,
        case_capabilities, seen, claim_pairs)
    require_keys(claim, ["capability", "status", "cases", "scope"])
    @test all(field -> claim[field] isa String,
        ("capability", "status", "scope"))
    @test all(case_id -> case_id isa String, claim["cases"])
    @test claim["capability"] in capability_ids
    @test claim["status"] in capabilities["statuses"]
    @test !isempty(claim["cases"])
    @test !isempty(claim["scope"])
    @test length(claim["cases"]) == length(unique(claim["cases"]))
    @test all(case_id -> case_id in known_cases, claim["cases"])
    for case_id in claim["cases"]
        @test claim["capability"] in case_capabilities[case_id]
        key = (claim["capability"], case_id)
        @test !haskey(seen, key)
        seen[key] = claim["status"]
        push!(claim_pairs, (case_id, claim["capability"]))
    end
    return
end

function validate_authority(authority, capabilities, capability_ids, known_cases,
        case_capabilities, artifact_hashes, revisions,
        authority_ids, claim_pairs)
    require_keys(authority, ["id", "kind", "version", "revision", "platforms",
        "toolchain_sha256", "claim"])
    @test all(field -> authority[field] isa String,
        ("id", "kind", "version", "revision"))
    @test all(value -> value isa String, authority["platforms"])
    @test all(value -> value isa String, authority["toolchain_sha256"])
    @test authority["id"] ∉ authority_ids
    push!(authority_ids, authority["id"])
    @test !isempty(authority["kind"])
    @test !isempty(authority["version"])
    @test !isempty(authority["revision"])
    @test authority["revision"] in revisions
    @test !isempty(authority["platforms"])
    @test length(authority["toolchain_sha256"]) ==
        length(unique(authority["toolchain_sha256"]))
    @test all(digest -> occursin(SHA256_PATTERN, digest),
        authority["toolchain_sha256"])
    @test all(digest -> digest in artifact_hashes,
        authority["toolchain_sha256"])
    seen = Dict{Tuple{String,String},String}()
    for claim in authority["claim"]
        validate_authority_claim(claim, capabilities, capability_ids, known_cases,
            case_capabilities, seen, claim_pairs)
    end
    return
end

function validate_authorities(capabilities, fixtures, model_cases, manifest,
        capability_ids)
    case_capabilities, known_cases =
        capability_case_context(fixtures, model_cases)
    authority_ids = Set{String}()
    claim_pairs = Set{Tuple{String,String}}()
    artifact_hashes = Set(artifact["sha256"] for toolchain in manifest["toolchain"]
        for artifact in toolchain["artifacts"])
    revisions = Set(source["revision"] for source in manifest["source"])
    union!(revisions, Set(model["sha256"] for model in manifest["frozen_model"]))
    push!(revisions, manifest["model_producer_descriptor_sha256"])
    push!(revisions, manifest["parquet_jl_source_composite_sha256"])
    push!(revisions, "preproduction")
    for authority in capabilities["authority"]
        validate_authority(authority, capabilities, capability_ids, known_cases,
            case_capabilities, artifact_hashes, revisions,
            authority_ids, claim_pairs)
    end
    @test length(authority_ids) == length(capabilities["authority"])
    return authority_ids, claim_pairs
end

function validate_evidence_authorities(capabilities, fixtures, manifest,
        authority_ids)
    for evidence in manifest["frozen_evidence"]
        @test evidence["authority"] in authority_ids
        authority = only(filter(item -> item["id"] == evidence["authority"],
            capabilities["authority"]))
        @test evidence["toolchain_sha256"] in authority["toolchain_sha256"]
        owning_toolchains = filter(toolchain -> any(artifact ->
                artifact["sha256"] == evidence["toolchain_sha256"],
                toolchain["artifacts"]), manifest["toolchain"])
        @test length(owning_toolchains) == 1
        @test only(owning_toolchains)["status"] == "verified"
        @test evidence["case_count"] <=
            length(fixtures["fixture"]) + length(fixtures["generated_case"])
        @test evidence["record_count"] <=
            manifest["evidence_limits"]["max_records_per_input"]
        if evidence["format"] == "normalized-jsonl"
            @test evidence["schema_file"] == manifest["evidence_schema_file"]
            @test evidence["schema_sha256"] == manifest["evidence_schema_sha256"]
        end
    end
    raw_evidence = only(filter(item -> item["id"] == "raw-java-apache-corpus",
        manifest["frozen_evidence"]))
    @test raw_evidence["case_count"] == length(fixtures["fixture"])
    @test all(evidence -> evidence["authority"] in authority_ids,
        manifest["planned_evidence"])
    return
end

function validate_fixture_claim_coverage(fixtures, claim_pairs)
    for case in vcat(fixtures["fixture"], fixtures["generated_case"])
        @test all(capability -> (case["id"], capability) in claim_pairs,
            case["capabilities"])
    end
    return
end

function validate_model_claim_coverage(model_cases, claim_pairs)
    for case in model_cases["case_groups"]
        @test all(capability -> (case["id"], capability) in claim_pairs,
            case["capabilities"])
    end
    return
end

function validate_raw_claim_coverage(capabilities, fixtures)
    raw_authority = only(filter(item -> item["id"] == "n6-raw-java",
        capabilities["authority"]))
    raw_claim_pairs = Set((case_id, claim["capability"])
        for claim in raw_authority["claim"] for case_id in claim["cases"])
    for fixture in fixtures["fixture"]
        for capability in fixture["capabilities"]
            startswith(capability, "wire.") || continue
            @test (fixture["id"], capability) in raw_claim_pairs
        end
    end
    for case_id in ("julia-writer-type-order", "julia-writer-ieee-order",
            "julia-writer-nested-row-groups")
        generated = only(filter(item -> item["id"] == case_id,
            fixtures["generated_case"]))
        @test "wire.statistics.exactness" in generated["capabilities"]
        @test (case_id, "wire.statistics.exactness") in raw_claim_pairs
    end
    return
end

function validate_capabilities(capabilities, fixtures, model_cases, manifest)
    capability_ids = validate_capability_catalog(capabilities, manifest)
    authority_ids, claim_pairs = validate_authorities(capabilities, fixtures,
        model_cases, manifest, capability_ids)
    validate_evidence_authorities(capabilities, fixtures, manifest, authority_ids)
    validate_fixture_claim_coverage(fixtures, claim_pairs)
    validate_model_claim_coverage(model_cases, claim_pairs)
    validate_raw_claim_coverage(capabilities, fixtures)
    return capability_ids
end

function validate_fixture_manifest_header(fixtures, manifest)
    require_keys(fixtures, ["manifest_version", "authority", "source_revision",
        "checksum_manifest", "status_values", "output_identity_statuses",
        "generation_contract_version", "default_digest_contract",
        "digest_contracts", "fixture", "generated_case"])
    @test fixtures["manifest_version"] == 1
    @test all(field -> fixtures[field] isa String, (
        "authority", "source_revision", "checksum_manifest",
        "default_digest_contract"))
    testing_source = only(filter(item -> item["id"] == "parquet-testing",
        manifest["source"]))
    @test fixtures["authority"] == "parquet-testing"
    @test fixtures["source_revision"] == testing_source["revision"]
    @test fixtures["checksum_manifest"] ==
        basename(manifest["corpus_manifest_file"])
    @test fixtures["status_values"] == ["verified", "planned", "unsupported"]
    @test fixtures["output_identity_statuses"] == ["planned", "verified"]
    @test all(value -> value isa String, fixtures["status_values"])
    @test all(value -> value isa String, fixtures["output_identity_statuses"])
    @test fixtures["generation_contract_version"] == 1
    @test fixtures["default_digest_contract"] ==
        "n6-capability-result-sha256-v1"
    @test fixtures["digest_contracts"] == ["n6-capability-result-sha256-v1",
        "n6-no-pruning-trace-sha256-v1"]
    @test all(value -> value isa String, fixtures["digest_contracts"])
    return
end

function validate_apache_fixture(fixture, fixtures, capability_ids, ids, files)
    require_keys(fixture, ["id", "status", "source_kind", "authority", "file",
        "sha256", "size", "source_revision", "row_group_count", "leaf_count",
        "normalized_record_count", "capabilities", "expected_unsupported"])
    @test all(field -> fixture[field] isa String, (
        "id", "status", "source_kind", "authority", "file", "sha256",
        "source_revision"))
    @test all(value -> value isa String, fixture["capabilities"])
    @test all(value -> value isa String, fixture["expected_unsupported"])
    @test occursin(CASE_PATTERN, fixture["id"])
    @test fixture["id"] ∉ ids
    push!(ids, fixture["id"])
    @test fixture["status"] in fixtures["status_values"]
    @test fixture["source_kind"] == "apache-corpus"
    @test fixture["authority"] == fixtures["authority"]
    @test fixture["source_revision"] == fixtures["source_revision"]
    @test safe_relative(fixture["file"])
    @test fixture["file"] ∉ keys(files)
    files[fixture["file"]] = fixture["sha256"]
    @test all(field -> fixture[field] isa Int64,
        ("size", "row_group_count", "leaf_count", "normalized_record_count"))
    @test fixture["size"] >= 12
    @test fixture["row_group_count"] >= 0
    @test fixture["leaf_count"] >= 0
    @test fixture["normalized_record_count"] ==
        1 + fixture["row_group_count"] * fixture["leaf_count"]
    @test !isempty(fixture["capabilities"])
    @test length(fixture["capabilities"]) == length(unique(fixture["capabilities"]))
    @test all(capability -> capability in capability_ids, fixture["capabilities"])
    @test all(capability -> capability in fixture["capabilities"],
        fixture["expected_unsupported"])
    return
end

function validate_apache_fixtures(fixtures, capability_ids)
    ids = Set{String}()
    files = Dict{String,String}()
    for fixture in fixtures["fixture"]
        validate_apache_fixture(fixture, fixtures, capability_ids, ids, files)
    end
    @test length(ids) == 20
    return ids, files
end

function validate_generated_output(generated, fixtures, ids, output_files)
    allowed = ["id", "status", "source_kind", "authority", "output_file",
        "output_identity_status", "output_sha256", "output_size",
        "generator_profile", "generator_seed", "variant_id", "mutation",
        "comparison_group", "digest_contract", "row_group_count", "leaf_count",
        "normalized_record_count", "capabilities", "expected_unsupported",
        "description"]
    required = setdiff(allowed, ["output_sha256", "output_size"])
    require_keys(generated, allowed, required)
    @test all(field -> generated[field] isa String, (
        "id", "status", "source_kind", "authority", "output_file",
        "output_identity_status", "generator_profile", "variant_id",
        "comparison_group", "digest_contract", "description"))
    @test all(value -> value isa String, generated["capabilities"])
    @test all(value -> value isa String, generated["expected_unsupported"])
    @test occursin(CASE_PATTERN, generated["id"])
    @test generated["id"] ∉ ids
    push!(ids, generated["id"])
    @test generated["status"] == "verified"
    @test generated["source_kind"] in
        ("julia-writer-generated", "metadata-mutation-generated")
    @test generated["authority"] == "parquet-jl"
    @test safe_generated_output(generated["output_file"])
    @test generated["output_file"] ∉ output_files
    push!(output_files, generated["output_file"])
    identity_status = generated["output_identity_status"]
    @test identity_status in fixtures["output_identity_statuses"]
    if identity_status == "planned"
        @test !haskey(generated, "output_sha256")
        @test !haskey(generated, "output_size")
    else
        @test generated["output_sha256"] isa String
        @test generated["output_size"] isa Int64
        @test occursin(SHA256_PATTERN, generated["output_sha256"])
        @test generated["output_size"] >= 12
    end
    return
end

function validate_generated_profile(generated, fixtures, generation_identities)
    @test occursin(CASE_PATTERN, generated["generator_profile"])
    @test all(field -> generated[field] isa Int64,
        ("generator_seed", "row_group_count", "leaf_count",
            "normalized_record_count"))
    @test generated["generator_seed"] >= 0
    @test occursin(CASE_PATTERN, generated["variant_id"])
    identity = (generated["generator_profile"],
        generated["generator_seed"], generated["variant_id"])
    @test identity ∉ generation_identities
    push!(generation_identities, identity)
    @test generated["comparison_group"] == "" ||
        occursin(CASE_PATTERN, generated["comparison_group"])
    @test generated["digest_contract"] in fixtures["digest_contracts"]
    return
end

function validate_statistics_state_mutation(mutation)
    state = mutation["statistics_state"]
    @test state isa String
    if state == "absent"
        require_keys(mutation, ["kind", "statistics_state"])
    elseif state in ("trusted", "producer-untrusted")
        require_keys(mutation, ["kind", "statistics_state", "created_by"])
        @test mutation["created_by"] isa String
        @test !isempty(mutation["created_by"])
    elseif state == "oversized"
        require_keys(mutation, ["kind", "statistics_state", "bound_bytes"])
        @test mutation["bound_bytes"] isa Int64
        @test mutation["bound_bytes"] == 4097
    elseif state == "semantically-unusable"
        require_keys(mutation, ["kind", "statistics_state", "field", "value_hex"])
        @test mutation["field"] isa String
        @test mutation["value_hex"] isa String
        @test mutation["field"] == "min_value"
        @test mutation["value_hex"] == "000000"
    else
        @test false
    end
    return
end

function validate_generated_mutation(mutation)
    kind = mutation["kind"]
    @test kind isa String
    if kind == "none"
        require_keys(mutation, ["kind"])
    elseif kind == "created-by"
        require_keys(mutation, ["kind", "created_by"])
        @test mutation["created_by"] isa String
        @test !isempty(mutation["created_by"])
    elseif kind == "statistics-state"
        validate_statistics_state_mutation(mutation)
    else
        @test false
    end
    return
end

function validate_generated_topology(generated, capability_ids)
    @test generated["row_group_count"] >= 0
    @test generated["leaf_count"] >= 0
    @test generated["normalized_record_count"] ==
        1 + generated["row_group_count"] * generated["leaf_count"]
    @test !isempty(generated["capabilities"])
    @test length(generated["capabilities"]) ==
        length(unique(generated["capabilities"]))
    @test all(capability -> capability in capability_ids,
        generated["capabilities"])
    @test all(capability -> capability in generated["capabilities"],
        generated["expected_unsupported"])
    return
end

function validate_generated_fixtures(fixtures, capability_ids, ids, files)
    output_files = Set(keys(files))
    generation_identities = Set{Tuple{String,Int64,String}}()
    for generated in fixtures["generated_case"]
        validate_generated_output(generated, fixtures, ids, output_files)
        validate_generated_profile(generated, fixtures, generation_identities)
        validate_generated_mutation(generated["mutation"])
        validate_generated_topology(generated, capability_ids)
    end
    @test length(ids) == 32
    return
end

function validate_no_pruning_fixtures(fixtures)
    no_pruning = filter(item ->
        item["comparison_group"] == "julia-reader-no-pruning-v1",
        fixtures["generated_case"])
    @test Set(item["variant_id"] for item in no_pruning) ==
        Set(["absent", "trusted", "producer-untrusted", "oversized",
            "semantically-unusable"])
    @test all(item -> item["generator_profile"] == "reader-no-pruning-v1" &&
        item["generator_seed"] == 6008 && item["digest_contract"] ==
        "n6-no-pruning-trace-sha256-v1", no_pruning)
    return
end

function validate_fixture_evidence_limits(fixtures, model_cases, manifest)
    maximum_records = 1 + sum(item["normalized_record_count"] +
        length(item["capabilities"]) for item in
        vcat(fixtures["fixture"], fixtures["generated_case"]))
    maximum_records += sum(length(item["capabilities"])
        for item in model_cases["case_groups"])
    @test manifest["evidence_limits"]["max_records_per_input"] == maximum_records
    @test manifest["evidence_limits"]["max_records_total"] ==
        maximum_records * manifest["evidence_limits"]["max_inputs"]
    return
end

function validate_corpus_manifest(corpus_lines, files)
    corpus = Dict{String,String}()
    corpus_paths = String[]
    for (line_number, line) in enumerate(corpus_lines)
        match_result = match(r"^([0-9a-f]{64})  ([A-Za-z0-9._+@=/\-]+)$", line)
        match_result === nothing && error("invalid corpus line $line_number")
        digest, relative = match_result.captures
        safe_relative(relative) || error("unsafe corpus path: $relative")
        haskey(corpus, relative) && error("duplicate corpus path: $relative")
        corpus[relative] = digest
        push!(corpus_paths, relative)
    end
    @test corpus_paths == sort(corpus_paths)
    @test corpus == files
    return
end

function validate_fixtures(fixtures, model_cases, corpus_lines, capability_ids,
        manifest)
    validate_fixture_manifest_header(fixtures, manifest)
    ids, files = validate_apache_fixtures(fixtures, capability_ids)
    validate_generated_fixtures(fixtures, capability_ids, ids, files)
    validate_no_pruning_fixtures(fixtures)
    validate_fixture_evidence_limits(fixtures, model_cases, manifest)
    validate_corpus_manifest(corpus_lines, files)
    return
end

function validate_python_descriptors(manifest, capabilities, snapshots)
    python_source = only(filter(item -> item["id"] == "cpython-3.12",
        manifest["source"]))
    python_toolchain = only(filter(item -> item["id"] == "python-interop",
        manifest["toolchain"]))
    artifact_map = Dict(item["name"] => item["sha256"]
        for item in python_toolchain["artifacts"])
    descriptors = Dict(
        "toolchains/pyarrow.toml" => (
            "pyarrow", "pyarrow-cp312-macos-15-arm64",
            "pyarrow-25.0.1-cp312-cp312-macosx_12_0_arm64.whl"),
        "toolchains/duckdb.toml" => (
            "duckdb", "duckdb-cp312-macos-15-arm64",
            "duckdb-1.5.5-cp312-cp312-macosx_11_0_arm64.whl"))
    for (relative, (authority_id, descriptor_id, wheel_name)) in descriptors
        repository_relative = "test/conformance/n6/" * relative
        snapshot = control_snapshot(snapshots, repository_relative)
        @test snapshot.sha256 == artifact_map[relative]
        descriptor = parse_toml_snapshot(snapshot)
        allowed = ["descriptor_version", "id", "status", "authority", "platform",
            "python_version", "python_source_revision",
            "python_distribution_url", "python_distribution_sha256",
            "python_tree_policy", "python_executable_sha256",
            "python_tree_sha256", "wheels",
            "harness_status", "harness_file", "harness_sha256",
            "support_files", "test_file", "test_sha256"]
        require_keys(descriptor, allowed)
        @test all(field -> descriptor[field] isa String, (
            "id", "status", "authority", "platform", "python_version",
            "python_source_revision", "python_distribution_url",
            "python_distribution_sha256", "python_tree_policy",
            "python_executable_sha256", "python_tree_sha256", "harness_status",
            "harness_file", "harness_sha256", "test_file", "test_sha256"))
        @test descriptor["descriptor_version"] == 1
        @test descriptor["id"] == descriptor_id
        @test descriptor["status"] == python_toolchain["status"]
        @test descriptor["authority"] == authority_id
        @test descriptor["platform"] == "macos-15-arm64"
        @test descriptor["python_version"] == python_source["version"]
        @test descriptor["python_source_revision"] == python_source["revision"]
        @test descriptor["python_distribution_url"] ==
            CPYTHON_312_DISTRIBUTION_URL
        @test descriptor["python_distribution_sha256"] ==
            artifact_map["cpython-distribution-archive"]
        @test descriptor["python_tree_policy"] == CPYTHON_CLEAN_TREE_POLICY
        @test descriptor["python_executable_sha256"] ==
            artifact_map["cpython-executable"]
        @test descriptor["python_tree_sha256"] ==
            artifact_map["cpython-clean-tree-sha256-v1"]
        @test descriptor["wheels"] ==
            [Dict("name" => wheel_name, "sha256" => artifact_map[wheel_name])]
        authority = only(filter(item -> item["id"] == authority_id,
            capabilities["authority"]))
        @test authority["toolchain_sha256"] == [artifact_map[relative]]
        @test safe_relative(descriptor["harness_file"])
        @test descriptor["harness_status"] == descriptor["status"]
        @test occursin(SHA256_PATTERN, descriptor["harness_sha256"])
        @test control_snapshot(snapshots,
            descriptor["harness_file"]).sha256 == descriptor["harness_sha256"]
        @test safe_relative(descriptor["test_file"])
        @test occursin(SHA256_PATTERN, descriptor["test_sha256"])
        @test control_snapshot(snapshots,
            descriptor["test_file"]).sha256 == descriptor["test_sha256"]
        @test descriptor["support_files"] isa Vector
        @test !isempty(descriptor["support_files"])
        support_paths = String[]
        for support in descriptor["support_files"]
            require_keys(support, ["file", "sha256"])
            @test support["file"] isa String
            @test support["sha256"] isa String
            @test safe_relative(support["file"])
            @test occursin(SHA256_PATTERN, support["sha256"])
            @test control_snapshot(snapshots,
                support["file"]).sha256 == support["sha256"]
            push!(support_paths, support["file"])
        end
        @test length(support_paths) == length(unique(support_paths))
    end
    return
end

function validate_arrow_descriptor(manifest, capabilities, snapshots)
    relative = "oracles/arrow-rs/toolchain.toml"
    snapshot = control_snapshot(snapshots, "test/conformance/n6/" * relative)
    toolchain = only(filter(item -> item["id"] == "rust-arrow-rs",
        manifest["toolchain"]))
    artifact = only(filter(item -> item["name"] == relative,
        toolchain["artifacts"]))
    @test snapshot.sha256 == artifact["sha256"]
    descriptor = parse_toml_snapshot(snapshot)
    require_keys(descriptor, ["descriptor_version", "status", "producer",
        "producer_version", "source_revision", "image_reference", "image_id",
        "image_platform", "binary_path", "binary_sha256", "platform",
        "metadata_binary_file", "metadata_binary_sha256",
        "metadata_binary_size",
        "python_version", "python_distribution_url",
        "python_distribution_sha256", "python_tree_policy",
        "python_executable_sha256", "python_tree_sha256",
        "rust_toolchain", "rustc", "cargo", "source", "wrapper"])
    @test descriptor["descriptor_version"] == 1
    @test descriptor["status"] == toolchain["status"]
    @test descriptor["producer"] == "arrow-rs"
    authority = only(filter(item -> item["id"] == "arrow-rs",
        capabilities["authority"]))
    source = only(filter(item -> item["id"] == "arrow-rs", manifest["source"]))
    @test descriptor["producer_version"] == authority["version"] ==
        source["version"]
    @test descriptor["source_revision"] == authority["revision"] ==
        source["revision"]
    @test descriptor["platform"] == "macos-15-arm64"
    @test descriptor["image_platform"] == "linux/amd64"
    @test startswith(descriptor["image_id"], "sha256:")
    @test occursin(SHA256_PATTERN, descriptor["image_id"][8:end])
    @test startswith(descriptor["binary_path"], "/")
    @test occursin(SHA256_PATTERN, descriptor["binary_sha256"])
    @test descriptor["metadata_binary_file"] ==
        "test/conformance/n6/oracles/arrow-rs/build/" *
        "parquet-jl-n6-arrow-rs-metadata"
    @test descriptor["metadata_binary_sha256"] ==
        toolchain_artifact(manifest, "rust-arrow-rs",
            "parquet-jl-n6-arrow-rs-metadata")
    @test descriptor["metadata_binary_size"] == 710064
    python_source = only(filter(item -> item["id"] == "cpython-3.12",
        manifest["source"]))
    python_toolchain = only(filter(item -> item["id"] == "python-interop",
        manifest["toolchain"]))
    python_artifacts = Dict(item["name"] => item["sha256"]
        for item in python_toolchain["artifacts"])
    @test descriptor["python_version"] == python_source["version"]
    @test descriptor["python_distribution_url"] == CPYTHON_312_DISTRIBUTION_URL
    @test descriptor["python_distribution_sha256"] ==
        python_artifacts["cpython-distribution-archive"]
    @test descriptor["python_tree_policy"] == CPYTHON_CLEAN_TREE_POLICY
    @test descriptor["python_executable_sha256"] ==
        python_artifacts["cpython-executable"]
    @test descriptor["python_tree_sha256"] ==
        python_artifacts["cpython-clean-tree-sha256-v1"]
    source_paths = String[]
    for entry in descriptor["source"]
        require_keys(entry, ["path", "sha256"])
        @test entry["path"] isa String
        @test entry["sha256"] isa String
        @test startswith(entry["path"], "/opt/bootstrap/arrow-rs/")
        @test occursin(SHA256_PATTERN, entry["sha256"])
        push!(source_paths, entry["path"])
    end
    @test length(source_paths) == 12
    @test length(source_paths) == length(unique(source_paths))
    wrapper_paths = String[]
    for entry in descriptor["wrapper"]
        require_keys(entry, ["path", "sha256"])
        @test entry["path"] isa String
        @test entry["sha256"] isa String
        @test safe_relative(entry["path"])
        @test occursin(SHA256_PATTERN, entry["sha256"])
        @test control_snapshot(snapshots,
            entry["path"]).sha256 == entry["sha256"]
        push!(wrapper_paths, entry["path"])
    end
    @test Set(wrapper_paths) == Set([
        "test/conformance/n6/harnesses/common.py",
        "test/conformance/n6/oracles/arrow-rs/build.sh",
        "test/conformance/n6/oracles/arrow-rs/run.py",
        "test/conformance/n6/oracles/arrow-rs/run.sh",
        "test/conformance/n6/oracles/arrow-rs/runtests.py",
        "test/conformance/n6/oracles/arrow-rs/check.sh",
        "test/conformance/n6/oracles/arrow-rs/metadata/Cargo.toml",
        "test/conformance/n6/oracles/arrow-rs/metadata/src/main.rs",
    ])
    @test authority["toolchain_sha256"] == [artifact["sha256"]]
    return
end

function validate_parquet_java_descriptor(manifest, capabilities, snapshots)
    relative = "oracles/parquet-java/toolchain.toml"
    snapshot = control_snapshot(snapshots, "test/conformance/n6/" * relative)
    toolchain = only(filter(item -> item["id"] == "parquet-java-interop",
        manifest["toolchain"]))
    artifact_map = Dict(item["name"] => item["sha256"]
        for item in toolchain["artifacts"])
    @test snapshot.sha256 == artifact_map[relative]
    descriptor = parse_toml_snapshot(snapshot)
    require_keys(descriptor, [
        "descriptor_version", "status", "producer", "producer_version",
        "source_revision", "platform", "python_version",
        "python_distribution_url", "python_distribution_sha256",
        "python_tree_policy", "python_executable_sha256", "python_tree_sha256",
        "java_vendor",
        "java_version", "java_executable_sha256", "javac_executable_sha256",
        "java_release_sha256", "jdk_tree_sha256", "hadoop_version",
        "harness_main", "harness_jar_file", "harness_jar_sha256",
        "harness_jar_size", "artifact", "source", "wrapper",
    ])
    @test descriptor["descriptor_version"] == 1
    @test descriptor["status"] == toolchain["status"]
    @test descriptor["producer"] == "parquet-java"
    authority = only(filter(item -> item["id"] == "parquet-java",
        capabilities["authority"]))
    source = only(filter(item -> item["id"] == "parquet-java",
        manifest["source"]))
    @test descriptor["producer_version"] == authority["version"] ==
        source["version"]
    @test descriptor["source_revision"] == authority["revision"] ==
        source["revision"]
    @test descriptor["platform"] == toolchain["platform"] ==
        "macos-15-arm64"
    python_source = only(filter(item -> item["id"] == "cpython-3.12",
        manifest["source"]))
    @test descriptor["python_version"] == python_source["version"]
    @test descriptor["python_distribution_url"] == CPYTHON_312_DISTRIBUTION_URL
    @test descriptor["python_distribution_sha256"] ==
        artifact_map["cpython-distribution-archive"]
    @test descriptor["python_tree_policy"] == CPYTHON_CLEAN_TREE_POLICY
    @test descriptor["python_executable_sha256"] ==
        artifact_map["cpython-executable"]
    @test descriptor["python_tree_sha256"] ==
        artifact_map["cpython-clean-tree-sha256-v1"]
    @test descriptor["java_vendor"] == "Eclipse-Adoptium-Temurin"
    @test descriptor["java_version"] == "21.0.8+9"
    @test descriptor["java_executable_sha256"] ==
        artifact_map["temurin-java"]
    @test descriptor["javac_executable_sha256"] ==
        artifact_map["temurin-javac"]
    @test descriptor["java_release_sha256"] ==
        artifact_map["temurin-release"]
    @test descriptor["jdk_tree_sha256"] ==
        artifact_map["temurin-tree-sha256-v1"]
    @test descriptor["hadoop_version"] == "3.3.0"
    @test descriptor["harness_main"] ==
        "org.julialang.parquet.n6.java.AuditMain"
    @test descriptor["harness_jar_file"] ==
        "test/conformance/n6/oracles/parquet-java/build/artifacts/" *
        "parquet-java-n6-harness.jar"
    @test descriptor["harness_jar_sha256"] ==
        artifact_map["parquet-java-n6-harness.jar"]
    @test descriptor["harness_jar_size"] == 9408
    expected_artifacts = Dict(
        "parquet-cli-runtime" => (
            "parquet-cli-1.17.1-runtime.jar", 50072122),
        "hadoop-client-api" => ("hadoop-client-api-3.3.0.jar", 19207034),
        "hadoop-client-runtime" => (
            "hadoop-client-runtime-3.3.0.jar", 27255121),
    )
    @test Set(item["id"] for item in descriptor["artifact"]) ==
        Set(keys(expected_artifacts))
    for item in descriptor["artifact"]
        require_keys(item, ["id", "file", "url", "sha256", "size"])
        name, size = expected_artifacts[item["id"]]
        @test item["file"] ==
            "test/conformance/n6/oracles/parquet-java/build/artifacts/$name"
        @test item["url"] ==
            "https://repo.maven.apache.org/maven2/" *
            (item["id"] == "parquet-cli-runtime" ?
                "org/apache/parquet/parquet-cli/1.17.1/$name" :
                "org/apache/hadoop/" *
                replace(name, r"-[0-9].*" => "") * "/3.3.0/$name")
        @test item["sha256"] == artifact_map[name]
        @test item["size"] == size
    end
    expected_sources = Set([
        "parquet-common/src/main/java/org/apache/parquet/VersionParser.java",
        "parquet-common/src/main/java/org/apache/parquet/SemanticVersion.java",
        "parquet-common/src/main/java/org/apache/parquet/io/LocalInputFile.java",
        "parquet-column/src/main/java/org/apache/parquet/CorruptStatistics.java",
        "parquet-column/src/main/java/org/apache/parquet/example/data/Group.java",
        "parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetReader.java",
        "parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java",
        "parquet-hadoop/src/main/java/org/apache/parquet/hadoop/example/GroupReadSupport.java",
        "parquet-hadoop/src/main/java/org/apache/parquet/format/converter/ParquetMetadataConverter.java",
    ])
    source_map = Dict{String,String}()
    for item in descriptor["source"]
        require_keys(item, ["file", "sha256"])
        @test safe_relative(item["file"])
        @test occursin(SHA256_PATTERN, item["sha256"])
        @test !haskey(source_map, item["file"])
        source_map[item["file"]] = item["sha256"]
    end
    @test Set(keys(source_map)) == expected_sources
    for item in source["files"]
        @test get(source_map, item["file"], nothing) == item["sha256"]
    end
    expected_wrappers = Set([
        "test/conformance/n6/harnesses/common.py",
        "test/conformance/n6/oracles/parquet-java/build.sh",
        "test/conformance/n6/oracles/parquet-java/check.sh",
        "test/conformance/n6/oracles/parquet-java/run.py",
        "test/conformance/n6/oracles/parquet-java/run.sh",
        "test/conformance/n6/oracles/parquet-java/runtests.py",
        "test/conformance/n6/oracles/parquet-java/src/org/julialang/parquet/n6/java/AuditMain.java",
    ])
    wrapper_paths = Set{String}()
    for item in descriptor["wrapper"]
        require_keys(item, ["path", "sha256"])
        @test safe_relative(item["path"])
        @test occursin(SHA256_PATTERN, item["sha256"])
        @test item["path"] ∉ wrapper_paths
        @test control_snapshot(snapshots,
            item["path"]).sha256 == item["sha256"]
        push!(wrapper_paths, item["path"])
    end
    @test wrapper_paths == expected_wrappers
    @test authority["toolchain_sha256"] == [artifact_map[relative]]
    return
end

function validate_static_files(manifest, capabilities, fixtures, snapshots)
    for (file_key, hash_key) in (
            ("plan_file", "plan_sha256"),
            ("capabilities_file", "capabilities_sha256"),
            ("fixture_manifest_file", "fixture_manifest_sha256"),
            ("corpus_manifest_file", "corpus_manifest_sha256"),
            ("evidence_schema_file", "evidence_schema_sha256"),
            ("artifact_manifest_file", "artifact_manifest_sha256"),
            ("model_producer_descriptor_file",
                "model_producer_descriptor_sha256"),
            ("parquet_jl_producer_descriptor_file",
                "parquet_jl_producer_descriptor_sha256"))
        snapshot = control_snapshot(snapshots, manifest[file_key])
        @test snapshot.sha256 == manifest[hash_key]
    end
    for model_file in manifest["frozen_model"]
        @test control_snapshot(snapshots,
            model_file["file"]).sha256 == model_file["sha256"]
    end
    descriptor = parse_toml_snapshot(control_snapshot(snapshots,
        manifest["model_producer_descriptor_file"]))
    require_keys(descriptor, ["descriptor_version", "producer", "file"])
    @test descriptor["descriptor_version"] == 1
    @test descriptor["producer"] == "n6-independent-model"
    expected_producer_files = Set([
        "test/conformance/n6/model/N6StatisticsModel.jl",
        "test/conformance/n6/model/README.md",
        "test/conformance/n6/model/cases.toml",
        "test/conformance/n6/model/runtests.jl",
        "test/conformance/n6/normalizers/common.py",
        "test/conformance/n6/normalizers/model_bridge.jl",
        "test/conformance/n6/normalizers/normalize_model.py",
        "test/conformance/n6/normalizers/normalize_raw.py",
    ])
    producer_paths = Set{String}()
    for item in descriptor["file"]
        require_keys(item, ["path", "sha256"])
        @test item["path"] isa String
        @test item["sha256"] isa String
        @test safe_relative(item["path"])
        @test occursin(SHA256_PATTERN, item["sha256"])
        @test item["path"] ∉ producer_paths
        @test control_snapshot(snapshots,
            item["path"]).sha256 == item["sha256"]
        push!(producer_paths, item["path"])
    end
    @test producer_paths == expected_producer_files
    authority = only(filter(item -> item["id"] == "n6-independent-model",
        capabilities["authority"]))
    @test authority["revision"] == manifest[
        "model_producer_descriptor_sha256"]
    for evidence in manifest["frozen_evidence"]
        @test control_snapshot(snapshots,
            evidence["schema_file"]).sha256 == evidence["schema_sha256"]
        @test normpath(evidence["fixture_manifest_file"]) ==
            normpath(manifest["fixture_manifest_file"])
        if evidence["storage"] == "checked-in"
            @test file_sha256(checked_file(REPO_DIR, evidence["file"])) ==
                evidence["sha256"]
        else
            @test !ispath(joinpath(REPO_DIR, evidence["file"]))
        end
    end
    validate_python_descriptors(manifest, capabilities, snapshots)
    validate_arrow_descriptor(manifest, capabilities, snapshots)
    validate_parquet_java_descriptor(manifest, capabilities, snapshots)
    validate_parquet_jl_descriptor(manifest, capabilities, snapshots)
    normalized_schema = String(copy(control_snapshot(snapshots,
        manifest["evidence_schema_file"]).payload))
    @test !occursin("\"manifest_sha256\"", normalized_schema)
    @test !occursin("\"artifact_manifest_sha256\"", normalized_schema)
    for field in ("plan_sha256", "capabilities_sha256",
            "fixture_manifest_sha256", "corpus_manifest_sha256",
            "evidence_schema_sha256", "toolchain_sha256")
        @test occursin("\"$field\"", normalized_schema)
    end
    entries = artifact_entries(control_snapshot(snapshots,
        manifest["artifact_manifest_file"]).payload)
    mutable_files = Set{String}()
    all_evidence = vcat(manifest["frozen_evidence"], manifest["planned_evidence"])
    @test length(unique(item["file"] for item in all_evidence)) ==
        length(all_evidence)
    for evidence in all_evidence
        safe_evidence_file(evidence["file"]) ||
            error("unsafe evidence exclusion: " * evidence["file"])
        relative = relpath(joinpath(REPO_DIR, evidence["file"]), N6_DIR)
        push!(mutable_files,
            replace(relative, Base.Filesystem.path_separator => '/'))
    end
    generated_files = Set{String}()
    for item in fixtures["generated_case"]
        safe_generated_output(item["output_file"]) ||
            error("unsafe generated output exclusion: " * item["output_file"])
        push!(generated_files, item["output_file"])
    end
    @test length(generated_files) == length(fixtures["generated_case"])
    union!(mutable_files, generated_files)
    @test isempty(intersect(Set(first.(entries)), mutable_files))
    @test "artifacts.sha256" ∉ first.(entries)
    @test "manifest.toml" ∉ first.(entries)
    @test first.(entries) == intended_artifacts(; mutable_files)
    for (relative, digest) in entries
        @test control_snapshot(snapshots,
            "test/conformance/n6/" * relative).sha256 == digest
    end
    for relative in (
            "test/conformance/n6/oracles/raw-java/check.sh",
            "test/conformance/n6/oracles/raw-java/run.sh",
            "test/conformance/n6/oracles/raw-java/scripts/build.sh",
            "test/conformance/n6/oracles/raw-java/scripts/generate.sh")
        script = String(copy(control_snapshot(snapshots, relative).payload))
        for line in eachline(IOBuffer(script))
            occursin(r"\"\$root/[^\"]+\.sh\"", line) || continue
            invocation = strip(line)
            startswith(invocation, "#") && continue
            @test startswith(invocation, "/bin/bash ") ||
                startswith(invocation, "source ")
        end
    end
    return
end

function validate_source_roots(manifest, fixtures)
    for source in manifest["source"]
        environment_name = source["root_env"]
        haskey(ENV, environment_name) || error("gate requires $environment_name")
        root = realpath(ENV[environment_name])
        require_gate(strip(read(isolated_command(
            `/usr/bin/git -C $root rev-parse HEAD`), String)) ==
            source["revision"], "source revision differs: " * source["id"])
        require_gate(isempty(strip(read(isolated_command(
            `/usr/bin/git -C $root status --porcelain`), String))),
            "source worktree is dirty: " * source["id"])
        require_gate(strip(read(isolated_command(
            `/usr/bin/git -C $root remote get-url origin`), String)) ==
            source["url"], "source origin differs: " * source["id"])
        if !isempty(source["tag"])
            tag_reference = "refs/tags/" * source["tag"]
            peeled_reference = tag_reference * "^{}"
            require_gate(strip(read(isolated_command(
                `/usr/bin/git -C $root rev-parse $tag_reference`), String)) ==
                source["tag_revision"], "source tag differs: " * source["id"])
            require_gate(strip(read(isolated_command(
                `/usr/bin/git -C $root rev-parse $peeled_reference`),
                String)) ==
                source["revision"],
                "source peeled tag differs: " * source["id"])
        end
        for pinned in source["files"]
            require_gate(file_sha256(checked_file(root, pinned["file"])) ==
                pinned["sha256"], "source file hash differs: " *
                source["id"] * ":" * pinned["file"])
        end
    end
    testing_source = only(filter(item -> item["id"] == "parquet-testing",
        manifest["source"]))
    testing_root = ENV[testing_source["root_env"]]
    for fixture in fixtures["fixture"]
        file = checked_file(testing_root, fixture["file"])
        require_gate(filesize(file) == fixture["size"],
            "fixture size differs: " * fixture["id"])
        require_gate(file_sha256(file) == fixture["sha256"],
            "fixture hash differs: " * fixture["id"])
    end
    return
end

function parse_shell_assignments(file::AbstractString)
    assignments = Dict{String,String}()
    for (line_number, line) in enumerate(eachline(file))
        match_result = match(r"^([A-Z][A-Z0-9_]*)=(\S+)$", line)
        match_result === nothing &&
            error("invalid shell assignment at line $line_number")
        name, value = match_result.captures
        haskey(assignments, name) &&
            error("duplicate shell assignment: $name")
        assignments[name] = value
    end
    return assignments
end

function raw_java_download_snapshots(manifest, gate_root)
    cache_input = get(ENV, "PARQUET_N6_RAW_JAVA_DOWNLOAD_CACHE", "")
    isempty(cache_input) &&
        error("gate requires PARQUET_N6_RAW_JAVA_DOWNLOAD_CACHE")
    islink(cache_input) &&
        error("raw Java download cache is a symbolic link")
    isdir(cache_input) || error("raw Java download cache is absent")
    downloads = realpath(cache_input)
    pins = parse_shell_assignments(gate_file(gate_root,
        "test/conformance/n6/oracles/raw-java/toolchain.env"))
    expected = [
        ("OpenJDK21U-jdk_aarch64_mac_hotspot_21.0.8_9.tar.gz",
            "RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_SHA256"),
        ("homebrew-thrift-bd296b14f19462baf03d5d96920209087ca99fa0.rb",
            "RAW_JAVA_HOMEBREW_FORMULA_SHA256"),
        ("libthrift-0.23.0.jar", "RAW_JAVA_LIBTHRIFT_SHA256"),
        ("libthrift-0.23.0.pom", "RAW_JAVA_LIBTHRIFT_POM_SHA256"),
        ("slf4j-api-1.7.36.jar", "RAW_JAVA_SLF4J_API_SHA256"),
        ("slf4j-nop-1.7.36.jar", "RAW_JAVA_SLF4J_NOP_SHA256"),
        ("thrift-0.23.0-dd6ed015e1b7a980c3dfa2b0dd1c01d563a8cf73bdb7f3de87d0cc1656fc1e1b.bottle.tar.gz",
            "RAW_JAVA_BOTTLE_DARWIN_ARM64_SEQUOIA_SHA256"),
        ("thrift-0.23.0.tar.gz", "RAW_JAVA_THRIFT_SOURCE_SHA256"),
    ]
    files = checked_directory_files(downloads, first.(expected))
    snapshots = Dict{String,FileSnapshot}()
    total_bytes = 0
    for ((name, pin), file) in zip(expected, files)
        haskey(pins, pin) || error("raw Java pin is absent: $pin")
        occursin(SHA256_PATTERN, pins[pin]) ||
            error("raw Java pin is invalid: $pin")
        payload = read_bounded_regular_bytes(file, RAW_JAVA_DOWNLOAD_LIMIT)
        length(payload) <= RAW_JAVA_DOWNLOAD_TOTAL_LIMIT - total_bytes ||
            error("raw Java downloads exceed their total byte limit")
        total_bytes += length(payload)
        digest = bytes2hex(SHA.sha256(payload))
        require_gate(digest == pins[pin],
            "raw Java cached download hash differs: $name")
        snapshots[name] = FileSnapshot(file, payload, digest)
    end
    require_gate(pins["RAW_JAVA_JDK_DARWIN_ARM64_SEQUOIA_SHA256"] ==
        toolchain_artifact(manifest, "raw-java", "temurin-jdk-archive"),
        "raw Java JDK archive pin differs from the manifest")
    for (pin, artifact) in (
            ("RAW_JAVA_LIBTHRIFT_SHA256", "libthrift-0.23.0.jar"),
            ("RAW_JAVA_LIBTHRIFT_POM_SHA256", "libthrift-0.23.0.pom"),
            ("RAW_JAVA_SLF4J_API_SHA256", "slf4j-api-1.7.36.jar"),
            ("RAW_JAVA_SLF4J_NOP_SHA256", "slf4j-nop-1.7.36.jar"))
        require_gate(pins[pin] == toolchain_artifact(manifest, "raw-java",
            artifact), "raw Java cached download pin differs: $artifact")
    end
    return snapshots
end

function with_raw_java_build(f, manifest, gate_root)
    snapshots = raw_java_download_snapshots(manifest, gate_root)
    return mktempdir() do directory
        build = joinpath(directory, "build")
        downloads = joinpath(build, "downloads")
        mkpath(downloads)
        private_files = Dict{String,String}()
        for (name, snapshot) in snapshots
            file = joinpath(downloads, name)
            open(file, "w") do io
                write(io, snapshot.payload)
                return
            end
            chmod(file, 0o400)
            private_files[name] = file
        end
        chmod(downloads, 0o500)
        try
            result = f(build)
            for (name, snapshot) in snapshots
                require_gate(file_sha256(private_files[name]) == snapshot.sha256,
                    "private raw Java download changed: $name")
                require_gate(file_sha256(snapshot.path) == snapshot.sha256,
                    "canonical raw Java download changed: $name")
            end
            return result
        finally
            chmod(downloads, 0o700)
            for file in values(private_files)
                chmod(file, 0o600)
            end
        end
    end
end

function validate_raw_java_snapshot(manifest, fixtures, python, isolated,
        gate_root, raw_build, julia_runtime)
    evidence = only(filter(item -> item["id"] == "raw-java-apache-corpus",
        manifest["frozen_evidence"]))
    evidence_schema = gate_file(gate_root, evidence["schema_file"])
    fixture_manifest = gate_file(gate_root, evidence["fixture_manifest_file"])
    raw_dir = joinpath(gate_root, "test", "conformance", "n6", "oracles",
        "raw-java")
    raw_environment = "PARQUET_N6_RAW_JAVA_BUILD_DIR" => raw_build
    run(isolated(`/bin/bash $(joinpath(raw_dir, "check.sh"))`,
        raw_environment))
    testing_source = only(filter(item -> item["id"] == "parquet-testing",
        manifest["source"]))
    testing_root = ENV[testing_source["root_env"]]
    selected = sort(fixtures["fixture"]; by=item -> item["file"])
    inputs = [checked_file(testing_root, item["file"]) for item in selected]
    labels = basename.(inputs)
    require_gate(length(labels) == length(unique(labels)),
        "raw corpus labels are ambiguous")
    validation = raw"""
import json
import pathlib
import sys
import tomllib
import jsonschema

schema_path, fixtures_path, evidence_path, expected_count, max_file_bytes, max_line_bytes = sys.argv[1:]
schema = json.loads(pathlib.Path(schema_path).read_text())
validator_class = jsonschema.validators.validator_for(schema)
validator_class.check_schema(schema)
validator = validator_class(schema)
fixtures = tomllib.loads(pathlib.Path(fixtures_path).read_text())
expected = {
    pathlib.Path(item["file"]).name: (item["sha256"], item["size"])
    for item in fixtures["fixture"]
}
seen = set()
evidence = pathlib.Path(evidence_path)
if evidence.stat().st_size > int(max_file_bytes):
    raise ValueError("raw evidence exceeds its byte limit")
with open(evidence, "rb") as stream:
    line_number = 0
    while True:
        line = stream.readline(int(max_line_bytes) + 1)
        if not line:
            break
        line_number += 1
        if len(line) > int(max_line_bytes):
            raise ValueError(f"line {line_number} exceeds its byte limit")
        if not line.endswith(b"\n"):
            raise ValueError(f"line {line_number} lacks its final newline")
        record = json.loads(line.decode("utf-8"))
        validator.validate(record)
        label = record["file"]
        if label not in expected or label in seen:
            raise ValueError(f"unexpected or duplicate raw label: {label}")
        if (record["file_sha256"], record["file_size"]) != expected[label]:
            raise ValueError(f"raw file identity mismatch: {label}")
        seen.add(label)
if len(seen) != int(expected_count) or seen != set(expected):
    raise ValueError("raw corpus evidence is incomplete")
"""
    mktempdir() do output_dir
        outputs = [joinpath(output_dir, "raw-$index.jsonl") for index in 1:2]
        for output in outputs
            arguments = String["/bin/bash", joinpath(raw_dir, "run.sh"), "scan"]
            for input in inputs
                append!(arguments, ["--input", input])
            end
            append!(arguments, ["--output", output])
            run(isolated(Cmd(arguments), raw_environment))
            output_size = filesize(output)
            0 < output_size <= manifest["evidence_limits"]["max_file_bytes"] ||
                error("raw evidence has an invalid byte size")
            require_gate(file_sha256(output) == evidence["sha256"],
                "raw evidence hash differs")
            command = Cmd(String[python, "-I", "-c", validation, evidence_schema,
                fixture_manifest, output, string(evidence["record_count"]),
                string(manifest["evidence_limits"]["max_file_bytes"]),
                string(manifest["evidence_limits"]["max_line_bytes"])])
            run(isolated(command))
        end
        require_gate(read(outputs[1]) == read(outputs[2]),
            "raw scanner output is not deterministic")
        normalized = joinpath(gate_root, "test", "conformance", "n6", "evidence",
            "raw-java-apache-corpus.normalized.jsonl")
        model = joinpath(gate_root, "test", "conformance", "n6", "evidence",
            "independent-model.normalized.jsonl")
        normalizers = joinpath(gate_root, "test", "conformance", "n6",
            "normalizers")
        run(isolated(Cmd(String[
            python, "-I", "-B", joinpath(normalizers, "normalize_raw.py"),
            "--input", outputs[1], "--output", normalized, "--check",
        ])))
        julia_executable = checked_file(julia_runtime, "bin/julia")
        run(isolated(Cmd(String[
            python, "-I", "-B", joinpath(normalizers, "normalize_model.py"),
            "--input", normalized, "--raw-input", outputs[1],
            "--output", model, "--julia-executable", julia_executable, "--check",
        ])))
        return
    end
    generated = sort(fixtures["generated_case"];
        by=item -> item["output_file"])
    generated_inputs = [gate_file(gate_root,
        "test/conformance/n6/" * item["output_file"]) for item in generated]
    mktempdir() do output_dir
        outputs = [joinpath(output_dir, "generated-$index.jsonl")
            for index in 1:2]
        for output in outputs
            arguments = String["/bin/bash", joinpath(raw_dir, "run.sh"),
                "scan"]
            for input in generated_inputs
                append!(arguments, ["--input", input])
            end
            append!(arguments, ["--output", output])
            run(isolated(Cmd(arguments), raw_environment))
            output_size = filesize(output)
            0 < output_size <= manifest["evidence_limits"]["max_file_bytes"] ||
                error("generated raw evidence has an invalid byte size")
        end
        require_gate(read(outputs[1]) == read(outputs[2]),
            "generated raw scanner output is not deterministic")
        generated_evidence = evidence_entry(manifest,
            "normalized-raw-java-generated")
        normalized = gate_file(gate_root, generated_evidence["file"])
        normalizer = gate_file(gate_root,
            "test/conformance/n6/normalizers/normalize_raw.py")
        run(isolated(Cmd(String[
            python, "-I", "-B", normalizer,
            "--fixture-set", "generated", "--input", outputs[1],
            "--output", normalized, "--check",
        ])))
        return
    end
    return
end

function validate_raw_java_gate(manifest, fixtures, python, isolated, gate_root,
        julia_runtime)
    with_raw_java_build(manifest, gate_root) do raw_build
        validate_raw_java_snapshot(manifest, fixtures, python, isolated,
            gate_root, raw_build, julia_runtime)
        return
    end
    return
end

function validator_runtime_inputs(manifest)
    archive_input = get(ENV, "PARQUET_N6_VALIDATOR_PYTHON_ARCHIVE", "")
    isempty(archive_input) &&
        error("gate requires PARQUET_N6_VALIDATOR_PYTHON_ARCHIVE")
    islink(archive_input) &&
        error("validator Python archive is a symbolic link")
    isfile(archive_input) || error("validator Python archive is absent")
    archive = realpath(archive_input)
    archive_payload = read_bounded_regular_bytes(archive,
        VALIDATOR_DISTRIBUTION_ARCHIVE_LIMIT)
    require_gate(bytes2hex(SHA.sha256(archive_payload)) ==
        toolchain_artifact(manifest, "jsonschema-validator",
            "cpython-distribution-archive"),
        "validator Python archive hash differs")
    validator_wheels = get(ENV, "PARQUET_N6_VALIDATOR_WHEEL_DIR", "")
    isempty(validator_wheels) && error("gate requires PARQUET_N6_VALIDATOR_WHEEL_DIR")
    islink(validator_wheels) && error("validator wheel root is a symbolic link")
    isdir(validator_wheels) || error("validator wheel root is absent")
    wheel_root = realpath(validator_wheels)
    validator_names = [
        "jsonschema-4.26.0-py3-none-any.whl",
        "attrs-25.4.0-py3-none-any.whl",
        "jsonschema_specifications-2025.9.1-py3-none-any.whl",
        "referencing-0.37.0-py3-none-any.whl",
        "rpds_py-0.30.0-cp314-cp314-macosx_11_0_arm64.whl",
    ]
    validator_files = checked_directory_files(wheel_root, validator_names)
    wheel_payloads = Dict{String,Vector{UInt8}}()
    for (artifact_name, file) in zip(validator_names, validator_files)
        payload = read_bounded_regular_bytes(file, 64 * 1024 * 1024)
        require_gate(bytes2hex(SHA.sha256(payload)) ==
            toolchain_artifact(manifest, "jsonschema-validator", artifact_name),
            "validator wheel hash differs: $artifact_name")
        wheel_payloads[artifact_name] = payload
    end
    return archive_payload, validator_names, wheel_payloads
end

function checked_runtime_directory(root::AbstractString, parts::String...)
    runtime_root = realpath(root)
    current = runtime_root
    for part in parts
        candidate = joinpath(current, part)
        (ispath(candidate) || islink(candidate)) ||
            error("Python distribution lacks " * join(parts, '/'))
        islink(candidate) && error("Python runtime path is a symbolic link: " *
            relpath(candidate, runtime_root))
        isdir(candidate) || error("Python runtime path is not a directory: " *
            relpath(candidate, runtime_root))
        resolved = realpath(candidate)
        startswith(resolved, runtime_root * Base.Filesystem.path_separator) ||
            error("Python runtime path escapes its root")
        current = resolved
    end
    return current
end

function prune_python_runtime!(root::AbstractString,
        version_directory::AbstractString)
    runtime_root = realpath(root)
    site_packages = checked_runtime_directory(runtime_root, "lib",
        version_directory,
        "site-packages")
    rm(site_packages; recursive=true)
    for (directory, directories, files) in walkdir(runtime_root;
            topdown=false, follow_symlinks=false)
        for name in files
            endswith(name, ".pyc") || continue
            file = joinpath(directory, name)
            islink(file) && error("Python bytecode cache is a symbolic link")
            rm(file)
        end
        for name in directories
            name == "__pycache__" || continue
            cache = joinpath(directory, name)
            islink(cache) && error("Python cache directory is a symbolic link")
            rm(cache; recursive=true)
        end
    end
    return
end

function prune_interop_python!(root::AbstractString)
    prune_python_runtime!(root, "python3.12")
    return
end

function lock_python_runtime!(root::AbstractString,
        executable_relative::AbstractString)
    runtime_root = realpath(root)
    for (directory, directories, files) in walkdir(runtime_root;
            follow_symlinks=false)
        for name in files
            file = joinpath(directory, name)
            islink(file) && continue
            chmod(file, 0o400)
        end
        for name in directories
            child = joinpath(directory, name)
            islink(child) && continue
            chmod(child, 0o500)
        end
    end
    chmod(checked_file(runtime_root, executable_relative), 0o500)
    chmod(runtime_root, 0o500)
    return
end

function lock_interop_python!(root::AbstractString)
    lock_python_runtime!(root, "bin/python3.12")
    return
end

function unlock_interop_python!(root::AbstractString)
    runtime_root = realpath(root)
    chmod(runtime_root, 0o700)
    for (directory, directories, files) in walkdir(runtime_root;
            follow_symlinks=false)
        for name in directories
            child = joinpath(directory, name)
            islink(child) && continue
            chmod(child, 0o700)
        end
        for name in files
            file = joinpath(directory, name)
            islink(file) && continue
            chmod(file, 0o600)
        end
    end
    return
end

function with_validator_runtime(f, manifest)
    archive_payload, wheel_names, wheel_payloads =
        validator_runtime_inputs(manifest)
    return mktempdir() do directory
        archive = joinpath(directory, "cpython.tar.gz")
        open(archive, "w") do io
            write(io, archive_payload)
            return
        end
        chmod(archive, 0o400)
        extract_root = joinpath(directory, "extract")
        mkdir(extract_root)
        run(isolated_command(`/usr/bin/tar -xzf $archive -C $extract_root`))
        require_gate(readdir(extract_root) == ["python"],
            "validator Python archive top-level contents differ")
        runtime_root = joinpath(extract_root, "python")
        islink(runtime_root) &&
            error("validator Python runtime root is a symbolic link")
        isdir(runtime_root) ||
            error("validator Python runtime root is absent")
        prune_python_runtime!(runtime_root, "python3.14")
        runtime_inventory = validate_bounded_tree(runtime_root;
            max_entries=VALIDATOR_TREE_ENTRY_LIMIT,
            max_file_bytes=VALIDATOR_TREE_FILE_LIMIT,
            max_total_bytes=VALIDATOR_TREE_TOTAL_LIMIT)
        require_gate(runtime_inventory.sha256 == toolchain_artifact(manifest,
            "jsonschema-validator",
            "cpython-clean-tree-sha256-v1"),
            "validator Python snapshot tree hash differs")
        base_python = checked_file(runtime_root, "bin/python3.14")
        require_gate(file_sha256(base_python) == toolchain_artifact(manifest,
            "jsonschema-validator", "cpython-executable"),
            "validator Python snapshot executable differs")
        wheel_root = joinpath(directory, "wheels")
        mkdir(wheel_root)
        validator_files = String[]
        for name in wheel_names
            file = joinpath(wheel_root, name)
            open(file, "w") do io
                write(io, wheel_payloads[name])
                return
            end
            chmod(file, 0o400)
            push!(validator_files, file)
        end
        chmod(wheel_root, 0o500)
        try
            lock_python_runtime!(runtime_root, "bin/python3.14")
            result = f(base_python, validator_files)
            require_gate(validate_bounded_tree(runtime_root;
                max_entries=VALIDATOR_TREE_ENTRY_LIMIT,
                max_file_bytes=VALIDATOR_TREE_FILE_LIMIT,
                max_total_bytes=VALIDATOR_TREE_TOTAL_LIMIT).sha256 ==
                toolchain_artifact(manifest,
                "jsonschema-validator",
                "cpython-clean-tree-sha256-v1"),
                "validator Python snapshot changed after use")
            for (name, file) in zip(wheel_names, validator_files)
                require_gate(file_sha256(file) == toolchain_artifact(manifest,
                    "jsonschema-validator", name),
                    "validator wheel snapshot changed after use: $name")
            end
            return result
        finally
            unlock_interop_python!(runtime_root)
            chmod(wheel_root, 0o700)
            for file in validator_files
                chmod(file, 0o600)
            end
        end
    end
end

function with_interop_runtime(f, manifest)
    archive_input = get(ENV, "PARQUET_N6_INTEROP_PYTHON_ARCHIVE", "")
    isempty(archive_input) &&
        error("gate requires PARQUET_N6_INTEROP_PYTHON_ARCHIVE")
    isfile(archive_input) || error("Python distribution archive is absent")
    islink(archive_input) &&
        error("Python distribution archive is a symbolic link")
    archive = realpath(archive_input)
    archive_payload = read_bounded_regular_bytes(archive,
        CPYTHON_DISTRIBUTION_ARCHIVE_LIMIT)
    require_gate(bytes2hex(SHA.sha256(archive_payload)) ==
        toolchain_artifact(manifest, "python-interop",
            "cpython-distribution-archive"),
        "Python distribution archive hash differs")
    interop_wheels = get(ENV, "PARQUET_N6_INTEROP_WHEEL_DIR", "")
    isempty(interop_wheels) && error("gate requires PARQUET_N6_INTEROP_WHEEL_DIR")
    interop_names = [
        "pyarrow-25.0.1-cp312-cp312-macosx_12_0_arm64.whl",
        "duckdb-1.5.5-cp312-cp312-macosx_11_0_arm64.whl",
    ]
    interop_files = checked_directory_files(interop_wheels, interop_names)
    for (artifact_name, file) in zip(interop_names, interop_files)
        require_gate(file_sha256(file) ==
            toolchain_artifact(manifest, "python-interop", artifact_name),
            "Python interop wheel hash differs: $artifact_name")
    end
    wheels = Dict(name => file
        for (name, file) in zip(interop_names, interop_files))
    return mktempdir() do directory
        archive_snapshot = joinpath(directory, "cpython.tar.gz")
        open(archive_snapshot, "w") do io
            write(io, archive_payload)
            return
        end
        chmod(archive_snapshot, 0o400)
        extract_root = joinpath(directory, "extract")
        mkdir(extract_root)
        run(isolated_command(
            `/usr/bin/tar -xzf $archive_snapshot -C $extract_root`))
        require_gate(readdir(extract_root) == ["python"],
            "Python archive top-level contents differ")
        interop_root = joinpath(extract_root, "python")
        islink(interop_root) && error("Python runtime root is a symbolic link")
        isdir(interop_root) || error("Python runtime root is not a directory")
        prune_interop_python!(interop_root)
        require_gate(tree_sha256(interop_root) == toolchain_artifact(manifest,
            "python-interop", "cpython-clean-tree-sha256-v1"),
            "clean Python runtime tree hash differs")
        interop_python = checked_file(interop_root, "bin/python3.12")
        require_gate(file_sha256(interop_python) == toolchain_artifact(manifest,
            "python-interop", "cpython-executable"),
            "interop Python executable hash differs")
        require_gate(!ispath(joinpath(interop_root, "lib", "python3.12",
            "site-packages")), "Python site-packages survived pruning")
        try
            lock_interop_python!(interop_root)
            return f(interop_python, wheels)
        finally
            unlock_interop_python!(interop_root)
        end
    end
end

function isolated_command(command, extra::Pair...)
    environment = Dict{String,String}(
        "GIT_CONFIG_GLOBAL" => "/dev/null",
        "GIT_CONFIG_NOSYSTEM" => "1",
        "HOME" => "/var/empty",
        "LANG" => "C",
        "LC_ALL" => "C",
        "NO_COLOR" => "1",
        "PATH" => "/usr/bin:/bin:/usr/sbin:/sbin",
        "TERM" => "dumb",
        "TMPDIR" => tempdir(),
        "TZ" => "UTC",
    )
    for (name, value) in extra
        environment[String(name)] = String(value)
    end
    return setenv(command, environment)
end

function isolated_python_command(command, extra::Pair...)
    return isolated_command(command,
        "PIP_CONFIG_FILE" => "/dev/null",
        "PIP_DISABLE_PIP_VERSION_CHECK" => "1",
        "PIP_NO_INDEX" => "1",
        "PYTHONHASHSEED" => "0",
        "PYTHONNOUSERSITE" => "1",
        "PYTHONPATH" => "",
        extra...)
end

function bootstrap_validator_environment(environment, base_python, validator_files)
    flags = strip(read(isolated_python_command(
        `$base_python -I -B -S -c "import sys; print(f'{sys.flags.isolated}|{sys.flags.no_site}|{sys.flags.dont_write_bytecode}')"`),
        String))
    require_gate(flags == "1|1|1", "validator Python isolation flags differ")
    run(isolated_python_command(`$base_python -I -B -S -m venv $environment`))
    python = joinpath(environment, "bin", "python")
    install = Cmd(vcat([python, "-I", "-m", "pip", "install", "--no-cache-dir",
        "--no-deps", "--no-index"], validator_files))
    run(isolated_python_command(install))
    versions = strip(read(isolated_python_command(
        `$python -I -c "import importlib.metadata as m; print('|'.join(m.version(x) for x in ('jsonschema','attrs','jsonschema-specifications','referencing','rpds-py')))"`),
        String))
    require_gate(versions == "4.26.0|25.4.0|2025.9.1|0.37.0|0.30.0",
        "validator package versions differ")
    return python
end

function validator_arguments(python, gate_root)
    return String[
        python,
        "-I",
        gate_file(gate_root, "test/conformance/n6/validate_evidence.py"),
        "--schema", gate_file(gate_root,
            "test/conformance/n6/evidence.schema.json"),
        "--manifest", gate_file(gate_root,
            "test/conformance/n6/manifest.toml"),
        "--capabilities", gate_file(gate_root,
            "test/conformance/n6/capabilities.toml"),
        "--fixtures", gate_file(gate_root,
            "test/conformance/n6/fixtures.toml"),
    ]
end

function normalized_gate_command(arguments::Vector{String},
        normalized::Vector{String}, gate_root::AbstractString)
    all(safe_evidence_file, normalized) ||
        error("gate evidence arguments must be canonical repository-relative paths")
    return Cmd(Cmd(vcat(arguments, String["--gate"], normalized)); dir=gate_root)
end

function validate_normalized_gate(manifest, python, gate_root)
    arguments = validator_arguments(python, gate_root)
    run(isolated_python_command(Cmd(vcat(arguments, ["--self-test"]))))
    normalized = sort!(String[item["file"]
        for item in manifest["frozen_evidence"]
        if item["format"] == "normalized-jsonl"])
    gate = normalized_gate_command(arguments, normalized, gate_root)
    if isempty(normalized)
        mktemp() do _, output
            closed = pipeline(ignorestatus(isolated_python_command(gate));
                stdout=output, stderr=output)
            process = run(closed)
            flush(output)
            seekstart(output)
            message = read(output, String)
            require_gate(!success(process),
                "empty normalized evidence unexpectedly passed")
            require_gate(startswith(message,
                "N6 evidence validation failed: " *
                "missing passing capability evidence:"),
                "empty normalized evidence failed for the wrong reason")
            return
        end
    else
        run(isolated_python_command(gate))
    end
    return
end

function validate_required_raw_gate(manifest, fixtures, python, gate_root,
        julia_runtime)
    validate_raw_java_gate(manifest, fixtures, python, isolated_python_command,
        gate_root, julia_runtime)
    return
end

function interop_harness_arguments(manifest, gate_root)
    testing_source = only(filter(item -> item["id"] == "parquet-testing",
        manifest["source"]))
    testing_root = ENV[testing_source["root_env"]]
    arguments = String[
        "--repository", gate_root,
        "--manifest", gate_file(gate_root,
            "test/conformance/n6/manifest.toml"),
        "--capabilities", gate_file(gate_root,
            "test/conformance/n6/capabilities.toml"),
        "--fixtures", gate_file(gate_root,
            "test/conformance/n6/fixtures.toml"),
        "--raw-evidence", gate_file(gate_root,
            "test/conformance/n6/evidence/" *
            "raw-java-apache-corpus.normalized.jsonl"),
        "--corpus-root", testing_root,
    ]
    return arguments
end

function evidence_entry(manifest, id::AbstractString)
    matches = filter(item -> item["id"] == id,
        vcat(manifest["frozen_evidence"], manifest["planned_evidence"]))
    return only(matches)
end

function draft_argument(evidence)
    return evidence["status"] == "planned" ? ["--draft"] : String[]
end

function validate_python_interop_gate(manifest, python, wheels, gate_root)
    shared = interop_harness_arguments(manifest, gate_root)
    configurations = [
        ("pyarrow", "pyarrow-25.0.1-cp312-cp312-macosx_12_0_arm64.whl"),
        ("duckdb", "duckdb-1.5.5-cp312-cp312-macosx_11_0_arm64.whl"),
    ]
    for (producer, wheel_name) in configurations
        evidence = evidence_entry(manifest, "normalized-$producer")
        arguments = String[
            python, "-I", "-B", "-S",
            gate_file(gate_root,
                "test/conformance/n6/harnesses/$producer.py"),
            shared...,
            "--descriptor", gate_file(gate_root,
                "test/conformance/n6/toolchains/$producer.toml"),
            "--wheel", wheels[wheel_name],
            "--output", gate_file(gate_root, evidence["file"]),
            "--check",
        ]
        append!(arguments, draft_argument(evidence))
        run(isolated_python_command(Cmd(arguments)))
    end
    return
end

function arrow_docker_executable()
    input = get(ENV, "PARQUET_N6_DOCKER", "")
    isempty(input) && error("gate requires PARQUET_N6_DOCKER")
    isabspath(input) || error("Arrow Rust Docker executable is not absolute")
    islink(input) && error("Arrow Rust Docker executable is a symbolic link")
    isfile(input) || error("Arrow Rust Docker executable is absent")
    executable = realpath(input)
    stat(executable).mode & 0o111 != 0 ||
        error("Arrow Rust Docker executable is not executable")
    return executable
end

function validate_arrow_interop_gate(manifest, python, gate_root)
    evidence = evidence_entry(manifest, "normalized-arrow-rs")
    arguments = String[
        python, "-I", "-B", "-S",
        gate_file(gate_root,
            "test/conformance/n6/oracles/arrow-rs/run.py"),
        interop_harness_arguments(manifest, gate_root)...,
        "--descriptor", gate_file(gate_root,
            "test/conformance/n6/oracles/arrow-rs/toolchain.toml"),
        "--output", gate_file(gate_root, evidence["file"]),
        "--docker", arrow_docker_executable(),
        "--check",
    ]
    append!(arguments, draft_argument(evidence))
    run(isolated_python_command(Cmd(arguments)))
    return
end

function parquet_java_jdk_input()
    jdk_input = get(ENV, "PARQUET_N6_JAVA_JDK_ROOT", "")
    isempty(jdk_input) && error("gate requires PARQUET_N6_JAVA_JDK_ROOT")
    islink(jdk_input) && error("Parquet Java JDK root is a symbolic link")
    isdir(jdk_input) || error("Parquet Java JDK root is absent")
    root = realpath(jdk_input)
    inventory = validate_bounded_tree(root;
        max_entries=JDK_TREE_ENTRY_LIMIT,
        max_file_bytes=JDK_TREE_FILE_LIMIT,
        max_total_bytes=JDK_TREE_TOTAL_LIMIT)
    return root, inventory
end

function validate_parquet_java_jdk_snapshot(manifest, jdk_root)
    inventory = validate_bounded_tree(jdk_root;
        max_entries=JDK_TREE_ENTRY_LIMIT,
        max_file_bytes=JDK_TREE_FILE_LIMIT,
        max_total_bytes=JDK_TREE_TOTAL_LIMIT)
    require_gate(inventory.sha256 == toolchain_artifact(manifest,
        "parquet-java-interop", "temurin-tree-sha256-v1"),
        "Parquet Java JDK tree hash differs")
    for (relative, artifact_name) in (
            ("bin/java", "temurin-java"),
            ("bin/javac", "temurin-javac"),
            ("release", "temurin-release"))
        require_gate(file_sha256(checked_file(jdk_root, relative)) ==
            toolchain_artifact(manifest, "parquet-java-interop", artifact_name),
            "Parquet Java JDK artifact differs: $artifact_name")
    end
    return
end

function with_parquet_java_jdk(f, manifest)
    source_root, source_inventory = parquet_java_jdk_input()
    return mktempdir() do directory
        jdk_root = joinpath(directory, "jdk")
        cp(source_root, jdk_root; follow_symlinks=false)
        jdk_root = realpath(jdk_root)
        require_gate(validate_bounded_tree(source_root;
            max_entries=JDK_TREE_ENTRY_LIMIT,
            max_file_bytes=JDK_TREE_FILE_LIMIT,
            max_total_bytes=JDK_TREE_TOTAL_LIMIT) == source_inventory,
            "Parquet Java JDK source tree changed while copied")
        validate_parquet_java_jdk_snapshot(manifest, jdk_root)
        try
            lock_python_runtime!(jdk_root, "bin/java")
            chmod(checked_file(jdk_root, "bin/javac"), 0o500)
            result = f(jdk_root)
            validate_parquet_java_jdk_snapshot(manifest, jdk_root)
            return result
        finally
            unlock_interop_python!(jdk_root)
        end
    end
end

function validate_parquet_java_interop_gate(manifest, python, jdk_root,
        gate_root)
    evidence = evidence_entry(manifest, "normalized-parquet-java")
    java_source = only(filter(item -> item["id"] == "parquet-java",
        manifest["source"]))
    java_root = ENV[java_source["root_env"]]
    arguments = String[
        python, "-I", "-B", "-S",
        gate_file(gate_root,
            "test/conformance/n6/oracles/parquet-java/run.py"),
        interop_harness_arguments(manifest, gate_root)...,
        "--descriptor", gate_file(gate_root,
            "test/conformance/n6/oracles/parquet-java/toolchain.toml"),
        "--java-root", java_root,
        "--jdk-root", jdk_root,
        "--output", gate_file(gate_root, evidence["file"]),
        "--check",
    ]
    append!(arguments, draft_argument(evidence))
    run(isolated_python_command(Cmd(arguments)))
    return
end

function validate_python_gate(manifest, capabilities, fixtures, snapshots)
    with_validator_runtime(manifest) do base_python, validator_files
        with_gate_repository(snapshots) do gate_root
            with_verified_parquet_jl_runtime(manifest, capabilities, snapshots,
                    gate_root) do julia_runtime
                julia_executable = checked_file(julia_runtime, "bin/julia")
                mktempdir() do environment
                    python = bootstrap_validator_environment(environment,
                        base_python, validator_files)
                    run(isolated_python_command(Cmd(String[
                        python, "-I", "-B",
                        gate_file(gate_root,
                            "test/conformance/n6/normalizers/runtests.py"),
                    ]), "PARQUET_N6_TEST_JULIA_EXECUTABLE" => julia_executable))
                    validate_required_raw_gate(manifest, fixtures, python,
                        gate_root, julia_runtime)
                    with_interop_runtime(manifest) do interop_python,
                            interop_wheels
                        run(isolated_python_command(Cmd(String[
                            interop_python, "-I", "-B", "-S",
                            gate_file(gate_root,
                                "test/conformance/n6/harnesses/test_common.py"),
                        ])))
                        validate_python_interop_gate(manifest, interop_python,
                            interop_wheels, gate_root)
                        writable = String["test/conformance/n6/evidence"]
                        with_gate_repository(snapshots;
                                writable_directories=writable) do oracle_root
                            validate_arrow_interop_gate(manifest,
                                interop_python, oracle_root)
                            with_parquet_java_jdk(manifest) do jdk_root
                                validate_parquet_java_interop_gate(manifest,
                                    interop_python, jdk_root, oracle_root)
                                return
                            end
                            return
                        end
                        return
                    end
                    validate_normalized_gate(manifest, python, gate_root)
                    return
                end
                return
            end
            return
        end
        return
    end
    return
end

function validate_running_julia(manifest)
    expected_id = VERSION < v"1.11" ? "julia-1.10" : "julia-1.12"
    expected_name = VERSION < v"1.11" ? "julia-1.10.11-executable" :
        "julia-1.12.6-executable"
    require_gate(file_sha256(joinpath(Sys.BINDIR, "julia")) ==
        toolchain_artifact(manifest, expected_id, expected_name),
        "running Julia executable hash differs")
    runtime_root = normpath(joinpath(Sys.BINDIR, ".."))
    require_gate(tree_sha256(runtime_root) == toolchain_artifact(manifest,
        expected_id, replace(expected_name, "-executable" =>
            "-runtime-tree-sha256-v1")),
        "running Julia runtime tree hash differs")
    return
end

function test_path_safety_helpers()
    oracles = ("arrow-rs", "raw-java", "parquet-java")
    mktempdir() do source_root
        relative = "test/conformance/n6/evidence/example.jsonl"
        source = joinpath(source_root, relative)
        mkpath(dirname(source))
        payload = Vector{UInt8}("evidence\n")
        write(source, payload)
        snapshots = Dict(relative => FileSnapshot(source, payload,
            bytes2hex(SHA.sha256(payload))))
        writable = String[dirname(relative)]
        with_gate_repository(snapshots;
                writable_directories=writable) do gate_root
            output = gate_file(gate_root, relative)
            @test stat(dirname(output)).mode & 0o200 != 0
            @test stat(output).mode & 0o222 == 0
            mktempdir(dirname(output)) do workspace
                @test isdir(workspace)
                return
            end
            @test read(output) == payload
            return
        end
        @test read(source) == payload
        @test_throws ErrorException with_gate_repository(snapshots;
                writable_directories=writable) do gate_root
            mkdir(joinpath(gate_root, dirname(relative), "leftover"))
            return
        end
        return
    end
    mktempdir() do root
        relative = "test/conformance/n6/evidence/example.jsonl"
        command = normalized_gate_command(String["python", "validator.py"],
            String[relative], root)
        @test command.dir == root
        @test command.exec[end-1:end] == ["--gate", relative]
        @test !isabspath(command.exec[end])
        absolute = joinpath(root, relative)
        @test_throws ErrorException normalized_gate_command(
            String["python", "validator.py"], String[absolute], root)
        return
    end
    for oracle in oracles
        mktempdir() do root
            for name in oracles
                mkpath(joinpath(root, "oracles", name))
            end
            oracle_dir = joinpath(root, "oracles", oracle)
            mktempdir() do outside
                symlink(outside, joinpath(oracle_dir, "build"))
                @test_throws ErrorException intended_artifacts(root)
                return
            end
            return
        end
    end
    for oracle in ("arrow-rs", "parquet-java")
        mktempdir() do root
            for name in oracles
                mkpath(joinpath(root, "oracles", name))
            end
            build = joinpath(root, "oracles", oracle, "build")
            mkpath(build)
            mktempdir() do outside
                symlink(outside, joinpath(build, "nested"))
                @test_throws ErrorException intended_artifacts(root)
                return
            end
            return
        end
    end
    mktempdir() do root
        for name in oracles
            mkpath(joinpath(root, "oracles", name))
        end
        mktempdir() do outside
            symlink(outside, joinpath(root, "unlisted"))
            @test_throws ErrorException intended_artifacts(root)
            return
        end
        return
    end
    mktempdir() do root
        site_packages = joinpath(root, "lib", "python3.12", "site-packages")
        cache = joinpath(root, "lib", "python3.12", "encodings",
            "__pycache__")
        mkpath(site_packages)
        mkpath(cache)
        write(joinpath(site_packages, "ambient.py"), "forbidden\n")
        write(joinpath(cache, "aliases.cpython-312.pyc"), "cache\n")
        prune_interop_python!(root)
        @test !ispath(site_packages)
        @test !ispath(cache)
        module_file = joinpath(root, "lib", "python3.12", "module.py")
        write(module_file, "value = 1\n")
        executable = joinpath(root, "bin", "python3.12")
        mkpath(dirname(executable))
        write(executable, "runtime\n")
        lock_interop_python!(root)
        @test (stat(root).mode & 0o222) == 0
        @test (stat(module_file).mode & 0o222) == 0
        unlock_interop_python!(root)
        @test (stat(root).mode & 0o200) != 0
        @test (stat(module_file).mode & 0o200) != 0
        return
    end
    mktempdir() do root
        base = joinpath(root, "lib", "python3.12")
        mkpath(base)
        mktempdir() do outside
            symlink(outside, joinpath(base, "site-packages"))
            @test_throws ErrorException prune_interop_python!(root)
            @test isdir(outside)
            return
        end
        return
    end
    for ancestor in ("lib", joinpath("lib", "python3.12"))
        mktempdir() do root
            mktempdir() do outside
                destination = joinpath(root, ancestor)
                mkpath(dirname(destination))
                mkpath(joinpath(outside, "site-packages"))
                sentinel = joinpath(outside, "site-packages", "sentinel")
                write(sentinel, "preserve\n")
                symlink(outside, destination)
                @test_throws ErrorException prune_interop_python!(root)
                @test read(sentinel, String) == "preserve\n"
                return
            end
            return
        end
    end
    mktempdir() do root
        write(joinpath(root, "payload"), "data")
        write(joinpath(root, "empty"), "")
        symlink("payload", joinpath(root, "alias"))
        inventory = validate_bounded_tree(root; max_entries=3,
            max_file_bytes=4, max_total_bytes=4)
        @test inventory.entries == 3
        @test inventory.bytes == 4
        @test occursin(SHA256_PATTERN, inventory.sha256)
        @test_throws ErrorException validate_bounded_tree(root;
            max_entries=2, max_file_bytes=4, max_total_bytes=4)
        @test_throws ErrorException validate_bounded_tree(root;
            max_entries=3, max_file_bytes=3, max_total_bytes=3)
        return
    end
    mktempdir() do root
        mktempdir() do outside
            write(joinpath(outside, "sentinel"), "preserve\n")
            symlink(joinpath(outside, "sentinel"), joinpath(root, "escape"))
            @test_throws ErrorException validate_bounded_tree(root;
                max_entries=1, max_file_bytes=16, max_total_bytes=16)
            @test read(joinpath(outside, "sentinel"), String) == "preserve\n"
            return
        end
        return
    end
    marker = "n6-test-ambient-secret-do-not-inherit"
    withenv("N6_TEST_AMBIENT_SECRET" => marker) do
        output = read(isolated_command(`/usr/bin/env`), String)
        @test !occursin("N6_TEST_AMBIENT_SECRET", output)
        @test !occursin(marker, output)
        try
            run(isolated_command(`/usr/bin/false`))
            @test false
        catch error
            message = sprint(showerror, error)
            @test !occursin("N6_TEST_AMBIENT_SECRET", message)
            @test !occursin(marker, message)
        end
        return
    end
    return
end

include("julia/producer_gate.jl")

@testset "N6 Parquet.jl producer identity helpers" begin
    test_parquet_jl_identity_helpers()
end

function test_preproduction_evidence()
    manifest_snapshot = file_snapshot("test/conformance/n6/manifest.toml")
    manifest = parse_toml_snapshot(manifest_snapshot)
    snapshots = load_control_snapshots(manifest)
    snapshots["test/conformance/n6/manifest.toml"] = manifest_snapshot
    capabilities = parse_toml_snapshot(control_snapshot(snapshots,
        manifest["capabilities_file"]))
    fixtures = parse_toml_snapshot(control_snapshot(snapshots,
        manifest["fixture_manifest_file"]))
    load_generated_snapshots!(snapshots, fixtures)
    load_evidence_snapshots!(snapshots, manifest)
    model_cases = parse_toml_snapshot(control_snapshot(snapshots,
        "test/conformance/n6/model/cases.toml"))
    corpus_lines = collect(eachline(IOBuffer(control_snapshot(snapshots,
        manifest["corpus_manifest_file"]).payload)))
    capability_ids = Set{String}()
    preflight = @testset "N6 fail-closed preflight" begin
        validate_manifest(manifest)
        capability_ids = validate_capabilities(capabilities, fixtures,
            model_cases, manifest)
        validate_fixtures(fixtures, model_cases,
            corpus_lines, capability_ids, manifest)
        validate_static_files(manifest, capabilities, fixtures, snapshots)
        validate_running_julia(manifest)
    end
    isempty(Test.filter_errors(preflight)) ||
        error("N6 static preflight failed; external execution is disabled")
    if get(ENV, "PARQUET_N6_GATE", "0") == "1"
        require_gate(Sys.isapple(), "N6 gate requires macOS")
        require_gate(Sys.ARCH == :aarch64, "N6 gate requires arm64")
        require_gate(VERSION == v"1.12.6",
            "N6 external gate requires Julia 1.12.6")
        require_gate(startswith(strip(read(isolated_command(
            `/usr/bin/sw_vers -productVersion`), String)), "15."),
            "N6 gate requires macOS 15")
        load_oracle_build_snapshots!(snapshots, manifest)
        validate_source_roots(manifest, fixtures)
        validate_python_gate(manifest, capabilities, fixtures, snapshots)
    end
    return
end

@testset "N6 path safety helpers" begin
    test_path_safety_helpers()
end

@testset "N6 preproduction evidence" begin
    test_preproduction_evidence()
end

include("model/runtests.jl")
