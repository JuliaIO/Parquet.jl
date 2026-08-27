module N6ParquetJLBootstrap

using SHA
using TOML

const DESCRIPTOR_RELATIVE = "test/conformance/n6/julia/parquet-jl-producer.toml"
const DESCRIPTOR_SHA256_ENV = "PARQUET_N6_PRODUCER_DESCRIPTOR_SHA256"
const SOURCE_COMPOSITE_ALGORITHM = "parquet-jl-n6-source-composite-v1"
const SOURCE_COMPOSITE_PREFIX = "parquet-jl-n6-source-composite-v1\0"
const PACKAGE_NAME = "Parquet"
const PACKAGE_UUID = "626c502c-15b0-58ad-a749-f091afb673ae"
const CANONICAL_JULIA_VERSION = v"1.12.6"
const CANONICAL_PLATFORM = "macos-15-arm64"
const MAX_DESCRIPTOR_BYTES = Int64(4 * 1024 * 1024)
const MAX_PINNED_FILE_BYTES = Int64(64 * 1024 * 1024)
const MAX_SOURCE_FILES = 10_000
const MAX_SOURCE_BYTES = Int64(512 * 1024 * 1024)
const MAX_TREE_ENTRIES = 100_000
const MAX_TREE_FILE_BYTES = Int64(1024 * 1024 * 1024)
const MAX_TREE_BYTES = Int64(4) * 1024 * 1024 * 1024
const MAX_RUNTIME_ENTRIES = 10_000
const MAX_RUNTIME_FILE_BYTES = Int64(1024 * 1024 * 1024)
const MAX_RUNTIME_BYTES = Int64(2) * 1024 * 1024 * 1024
const REQUIRED_ENVIRONMENT = Dict(
    "HOME" => "/var/empty",
    "JULIA_LOAD_PATH" => "@:@stdlib",
    "JULIA_PKG_OFFLINE" => "true",
    "JULIA_PKG_SERVER" => "",
    "JULIA_PKG_PRECOMPILE_AUTO" => "0",
)
const ALLOWED_JULIA_ENVIRONMENT = Set([
    "JULIA_DEPOT_PATH",
    "JULIA_LOAD_PATH",
    "JULIA_NUM_THREADS",
    "JULIA_PKG_OFFLINE",
    "JULIA_PKG_PRECOMPILE_AUTO",
    "JULIA_PKG_SERVER",
])
const FORBIDDEN_LINKER_ENVIRONMENT = (
    "DYLD_FALLBACK_LIBRARY_PATH",
    "DYLD_INSERT_LIBRARIES",
    "DYLD_LIBRARY_PATH",
    "LD_LIBRARY_PATH",
    "LD_PRELOAD",
)
const PREFERENCE_FILES = (
    "JuliaLocalPreferences.toml",
    "JuliaPreferences.toml",
    "LocalPreferences.toml",
    "Preferences.toml",
)

struct TreeEntry
    relative::String
    path::String
    metadata::Base.Filesystem.StatStruct
    link_target::Union{Nothing,String}
end

struct BootstrapContext
    root::String
    depot::String
    runtime_root::String
    descriptor_path::String
    descriptor_sha256::String
    descriptor::Dict{String,Any}
    manifest::Dict{String,Any}
    pinned_bytes::Dict{String,Vector{UInt8}}
    dependency_roots::Dict{String,String}
    artifact_roots::Dict{String,String}
end

function _require(condition::Bool, message::AbstractString)
    condition || throw(ArgumentError(message))
    return
end

function _statidentity(value)
    return (value.device, value.inode, value.mode, value.nlink, value.size,
        value.mtime, value.ctime)
end

function _filekind(metadata)
    return metadata.mode & Base.Filesystem.S_IFMT
end

function _regular(metadata)
    return _filekind(metadata) == Base.Filesystem.S_IFREG
end

function _directory(metadata)
    return _filekind(metadata) == Base.Filesystem.S_IFDIR
end

function _symlink(metadata)
    return _filekind(metadata) == Base.Filesystem.S_IFLNK
end

function _readonly(metadata)
    return metadata.mode & 0o222 == 0
end

function _within(path::AbstractString, root::AbstractString)
    path == root && return true
    return startswith(path, root * Base.Filesystem.path_separator)
end

function _portable(path::AbstractString)
    return replace(path, Base.Filesystem.path_separator => '/')
end

function _safe_relative(relative::String, label::String)
    _require(!isempty(relative), "$label path is empty")
    _require(!isabspath(relative), "$label path is absolute: $relative")
    _require(!occursin('\\', relative), "$label path contains a backslash: $relative")
    _require(all(byte -> byte <= 0x7f, codeunits(relative)),
        "$label path is not ASCII: $relative")
    _require(!occursin('\0', relative) && !occursin('\n', relative),
        "$label path contains a control separator: $relative")
    parts = split(relative, '/')
    _require(all(part -> !isempty(part) && part != "." && part != "..", parts),
        "$label path is unsafe: $relative")
    return parts
end

function _safe_path(root::String, relative::String, label::String)
    parts = _safe_relative(relative, label)
    current = root
    for part in parts
        current = joinpath(current, part)
        _require(!islink(current), "$label path contains a symbolic link: $relative")
    end
    absolute = normpath(joinpath(root, parts...))
    _require(_within(absolute, root), "$label path escapes its root: $relative")
    return absolute
end

function _stable_file_bytes(path::String, maximum::Int64, label::String)
    _require(maximum >= 0, "$label byte limit is negative")
    _require(maximum < typemax(Int), "$label byte limit is too large")
    before = lstat(path)
    _require(_regular(before) && !islink(path), "$label is not a regular file")
    _require(0 <= before.size <= maximum, "$label exceeds its byte limit")
    return open(path, "r") do stream
        opened = stat(stream)
        _require(_statidentity(opened) == _statidentity(before),
            "$label changed while it was opened")
        bytes = read(stream, Int(maximum) + 1)
        _require(length(bytes) <= maximum, "$label exceeds its byte limit")
        _require(length(bytes) == opened.size && eof(stream),
            "$label changed size while it was read")
        final = stat(stream)
        current = lstat(path)
        _require(_regular(current) && !islink(path),
            "$label changed type while it was read")
        _require(_statidentity(final) == _statidentity(opened) &&
            _statidentity(current) == _statidentity(opened),
            "$label changed while it was read")
        return bytes
    end
end

function _stable_file_digest(path::String, maximum::Int64, label::String)
    before = lstat(path)
    _require(_regular(before) && !islink(path), "$label is not a regular file")
    _require(0 <= before.size <= maximum, "$label exceeds its byte limit")
    digest = open(path, "r") do stream
        opened = stat(stream)
        _require(_statidentity(opened) == _statidentity(before),
            "$label changed while it was opened")
        value = SHA.sha256(stream)
        _require(eof(stream), "$label was not read to its end")
        final = stat(stream)
        current = lstat(path)
        _require(_regular(current) && !islink(path),
            "$label changed type while it was read")
        _require(_statidentity(final) == _statidentity(opened) &&
            _statidentity(current) == _statidentity(opened),
            "$label changed while it was read")
        return value
    end
    return digest, Int64(before.size)
end

function _hex_sha256(value, label::String)
    _require(value isa String && occursin(r"^[0-9a-f]{64}$", value),
        "$label is not a lowercase SHA-256 digest")
    return value::String
end

function _hex_sha1(value, label::String)
    _require(value isa String && occursin(r"^[0-9a-f]{40}$", value),
        "$label is not a lowercase Git tree SHA-1 digest")
    return value::String
end

function _string_field(table::AbstractDict, key::String, label::String)
    _require(haskey(table, key), "$label lacks $key")
    value = table[key]
    _require(value isa String, "$label $key is not a string")
    return value::String
end

function _integer_field(table::AbstractDict, key::String, label::String)
    _require(haskey(table, key), "$label lacks $key")
    value = table[key]
    _require(value isa Integer && !(value isa Bool),
        "$label $key is not an integer")
    return Int(value)
end

function _table_array(table::AbstractDict, key::String, label::String)
    _require(haskey(table, key), "$label lacks $key")
    value = table[key]
    _require(value isa AbstractVector, "$label $key is not an array")
    _require(all(item -> item isa AbstractDict, value),
        "$label $key contains a non-table value")
    return value
end

function _exact_keys(table::AbstractDict, expected, label::String)
    actual = Set(String(key) for key in keys(table))
    required = Set(String(key) for key in expected)
    _require(actual == required, "$label keys differ; expected $(sort!(collect(required))), " *
        "got $(sort!(collect(actual)))")
    return
end

function _canonical_root(path::String, label::String)
    absolute = normpath(abspath(path))
    _require(isdir(absolute) && !islink(absolute), "$label is not a directory")
    _require(realpath(absolute) == absolute, "$label is not canonical")
    return absolute
end

function _validate_bootstrap_stdlibs()
    _require(VERSION == CANONICAL_JULIA_VERSION,
        "N6 bootstrap has the wrong Julia version")
    runtime_root = _canonical_root(dirname(Sys.BINDIR), "Julia runtime root")
    stdlib_root = joinpath(runtime_root, "share", "julia", "stdlib", "v1.12")
    expected = (
        SHA => joinpath(stdlib_root, "SHA", "src", "SHA.jl"),
        TOML => joinpath(stdlib_root, "TOML", "src", "TOML.jl"),
    )
    for (module_, path) in expected
        _require(pathof(module_) == path,
            "bootstrap stdlib resolves outside the exact Julia runtime: $(nameof(module_))")
    end
    return
end

function _validate_environment(root::String, depot::String)
    for (key, expected) in REQUIRED_ENVIRONMENT
        _require(get(ENV, key, nothing) == expected,
            "$key differs from the isolated N6 value")
    end
    _require(get(ENV, "JULIA_DEPOT_PATH", nothing) == depot,
        "JULIA_DEPOT_PATH is not the canonical private depot")
    threads = get(ENV, "JULIA_NUM_THREADS", nothing)
    _require(threads === nothing || threads == "1",
        "JULIA_NUM_THREADS is not one")
    for key in keys(ENV)
        startswith(key, "JULIA_") || continue
        _require(key in ALLOWED_JULIA_ENVIRONMENT,
            "unexpected Julia environment input: $key")
    end
    for key in FORBIDDEN_LINKER_ENVIRONMENT
        _require(!haskey(ENV, key), "forbidden dynamic-linker environment input: $key")
    end
    _require(Threads.nthreads() == 1, "N6 bootstrap requires one Julia thread")
    _require(Threads.nthreads(:default) == 1 && Threads.nthreads(:interactive) == 0,
        "N6 bootstrap requires one default thread and no interactive threads")
    _require(Base.LOAD_PATH == ["@", "@stdlib"],
        "Julia load path is not isolated")
    _require(Base.DEPOT_PATH == [depot], "Julia depot path is not isolated")
    _require(Base.active_project() == joinpath(root, "Project.toml"),
        "active Julia project is not the read-only gate root")
    options = Base.JLOptions()
    _require(options.startupfile == 2, "Julia startup files are enabled")
    _require(options.historyfile == 0, "Julia history is enabled")
    _require(options.use_compiled_modules == 0, "Julia compiled modules are enabled")
    _require(options.use_pkgimages == 0, "Julia package images are enabled")
    _require(isempty(ARGS), "N6 bootstrap does not accept arguments")
    return
end

function _walk_entries(root::String, maximum_entries::Int, maximum_file_bytes::Int64,
        maximum_total_bytes::Int64, label::String; safe_links::Bool)
    entries = TreeEntry[]
    total = Int64(0)
    for (directory, directories, files) in walkdir(root; follow_symlinks=false)
        sort!(directories)
        sort!(files)
        for name in vcat(directories, files)
            path = joinpath(directory, name)
            metadata = lstat(path)
            if _symlink(metadata)
                target = readlink(path)
                _require(!occursin('\0', target) && !occursin('\n', target),
                    "$label symbolic-link target is invalid")
                if safe_links
                    _require(!isabspath(target),
                        "$label has an absolute symbolic link: $path")
                    normalized = normpath(joinpath(dirname(path), target))
                    _require(_within(normalized, root),
                        "$label symbolic link escapes its root: $path")
                    _require(ispath(path), "$label has a broken symbolic link: $path")
                    _require(_within(realpath(path), root),
                        "$label symbolic link resolves outside its root: $path")
                end
                relative = _portable(relpath(path, root))
                push!(entries, TreeEntry(relative, path, metadata, target))
            elseif _regular(metadata)
                _require(0 <= metadata.size <= maximum_file_bytes,
                    "$label file exceeds its byte limit: $path")
                _require(metadata.size <= maximum_total_bytes - total,
                    "$label exceeds its total byte limit")
                total += metadata.size
                relative = _portable(relpath(path, root))
                push!(entries, TreeEntry(relative, path, metadata, nothing))
            else
                _require(_directory(metadata), "$label contains a special file: $path")
                relative = _portable(relpath(path, root))
                push!(entries, TreeEntry(relative, path, metadata, nothing))
            end
            _require(length(entries) <= maximum_entries,
                "$label exceeds its entry limit")
        end
    end
    sort!(entries; by=entry -> entry.relative)
    _require(length(unique(entry.relative for entry in entries)) == length(entries),
        "$label has duplicate paths")
    return entries, total
end

function _tree_identity(root::String, maximum_entries::Int,
        maximum_file_bytes::Int64, maximum_total_bytes::Int64, label::String;
        safe_links::Bool=true)
    entries, total = _walk_entries(root, maximum_entries, maximum_file_bytes,
        maximum_total_bytes, label; safe_links=safe_links)
    context = SHA.SHA2_256_CTX()
    for entry in entries
        if _directory(entry.metadata)
            continue
        elseif entry.link_target === nothing
            digest, _ = _stable_file_digest(entry.path, maximum_file_bytes,
                "$label file $(entry.relative)")
            SHA.update!(context, codeunits("F\0"))
            SHA.update!(context, codeunits(entry.relative))
            SHA.update!(context, codeunits("\0"))
            SHA.update!(context, codeunits(bytes2hex(digest)))
            SHA.update!(context, codeunits("\n"))
        else
            current = lstat(entry.path)
            _require(_symlink(current) &&
                _statidentity(current) == _statidentity(entry.metadata) &&
                readlink(entry.path) == entry.link_target,
                "$label symbolic link changed while it was hashed")
            SHA.update!(context, codeunits("L\0"))
            SHA.update!(context, codeunits(entry.relative))
            SHA.update!(context, codeunits("\0"))
            SHA.update!(context, codeunits(entry.link_target))
            SHA.update!(context, codeunits("\n"))
        end
    end
    return (sha256=bytes2hex(SHA.digest!(context)),
        entry_count=length(entries), total_bytes=total)
end

function _tree_sha256(root::String, maximum_entries::Int,
        maximum_file_bytes::Int64, maximum_total_bytes::Int64, label::String;
        safe_links::Bool=true)
    identity = _tree_identity(root, maximum_entries, maximum_file_bytes,
        maximum_total_bytes, label; safe_links=safe_links)
    return identity.sha256
end

function _assert_readonly_gate(root::String)
    root_metadata = lstat(root)
    _require(_directory(root_metadata) && _readonly(root_metadata),
        "gate root is not read-only")
    entries, _ = _walk_entries(root, MAX_TREE_ENTRIES, MAX_TREE_FILE_BYTES,
        MAX_TREE_BYTES, "gate root"; safe_links=true)
    for entry in entries
        _require(entry.link_target === nothing,
            "gate root contains a symbolic link: $(entry.relative)")
        _require(_readonly(entry.metadata),
            "gate root entry is writable: $(entry.relative)")
    end
    for relative in PREFERENCE_FILES
        _require(!ispath(joinpath(root, relative)),
            "gate root contains a preference input: $relative")
    end
    return
end

function _assert_readonly_depot(depot::String)
    root_metadata = lstat(depot)
    _require(_directory(root_metadata) && _readonly(root_metadata),
        "private depot root is not read-only")
    entries, _ = _walk_entries(depot, MAX_TREE_ENTRIES, MAX_TREE_FILE_BYTES,
        MAX_TREE_BYTES, "private depot"; safe_links=true)
    for entry in entries
        entry.link_target === nothing || continue
        _require(_readonly(entry.metadata),
            "private depot entry is writable: $(entry.relative)")
    end
    return
end

function _validate_descriptor(descriptor::Dict{String,Any})
    expected = [
        "artifact", "bootstrap_file", "bootstrap_sha256", "dependency",
        "descriptor_version", "file", "harness_file", "harness_sha256",
        "julia_executable_sha256", "julia_runtime_entry_count",
        "julia_runtime_total_bytes", "julia_runtime_tree_sha256", "julia_version",
        "manifest_file", "manifest_sha256", "package_name", "package_uuid",
        "package_version", "platform", "producer", "project_file",
        "project_sha256", "runner_file", "runner_sha256",
        "source_composite_algorithm", "source_composite_sha256",
        "source_file_count", "source_total_bytes", "status",
    ]
    _exact_keys(descriptor, expected, "Parquet.jl producer descriptor")
    _require(_integer_field(descriptor, "descriptor_version", "descriptor") == 1,
        "producer descriptor version is not one")
    _require(_string_field(descriptor, "producer", "descriptor") == "parquet-jl",
        "producer descriptor has the wrong producer")
    _require(_string_field(descriptor, "package_name", "descriptor") == PACKAGE_NAME,
        "producer descriptor has the wrong package name")
    _require(_string_field(descriptor, "package_uuid", "descriptor") == PACKAGE_UUID,
        "producer descriptor has the wrong package UUID")
    _require(_string_field(descriptor, "package_version", "descriptor") ==
        "1.0.0-DEV", "producer descriptor has the wrong package version")
    _require(_string_field(descriptor, "julia_version", "descriptor") ==
        string(CANONICAL_JULIA_VERSION), "producer descriptor has the wrong Julia version")
    _require(_string_field(descriptor, "platform", "descriptor") == CANONICAL_PLATFORM,
        "producer descriptor has the wrong platform")
    _require(_string_field(descriptor, "status", "descriptor") in ("planned", "verified"),
        "producer descriptor has an invalid status")
    _require(_string_field(descriptor, "source_composite_algorithm", "descriptor") ==
        SOURCE_COMPOSITE_ALGORITHM, "producer source composite algorithm differs")
    _hex_sha256(descriptor["source_composite_sha256"],
        "producer source composite")
    _hex_sha256(descriptor["julia_executable_sha256"], "Julia executable")
    _hex_sha256(descriptor["julia_runtime_tree_sha256"], "Julia runtime tree")
    _require(_integer_field(descriptor, "julia_runtime_entry_count",
        "descriptor") > 0, "Julia runtime entry count is not positive")
    _require(_integer_field(descriptor, "julia_runtime_total_bytes",
        "descriptor") > 0, "Julia runtime byte count is not positive")
    _hex_sha256(descriptor["project_sha256"], "project")
    _hex_sha256(descriptor["manifest_sha256"], "manifest")
    _hex_sha256(descriptor["bootstrap_sha256"], "bootstrap")
    _hex_sha256(descriptor["harness_sha256"], "harness")
    _hex_sha256(descriptor["runner_sha256"], "runner")
    _require(_integer_field(descriptor, "source_file_count", "descriptor") > 0,
        "producer source file count is not positive")
    _require(_integer_field(descriptor, "source_total_bytes", "descriptor") > 0,
        "producer source byte count is not positive")
    return
end

function _descriptor_files(descriptor::Dict{String,Any})
    entries = _table_array(descriptor, "file", "descriptor")
    files = Dict{String,String}()
    for (index, entry) in enumerate(entries)
        label = "descriptor file $index"
        _exact_keys(entry, ["path", "sha256"], label)
        relative = _string_field(entry, "path", label)
        _safe_relative(relative, label)
        digest = _hex_sha256(entry["sha256"], "$label digest")
        _require(!haskey(files, relative), "duplicate descriptor file: $relative")
        files[relative] = digest
    end
    _require(!isempty(files), "producer descriptor has no files")
    return files
end

function _source_inventory(root::String)
    source_root = _safe_path(root, "src", "source root")
    _require(isdir(source_root), "source root is absent")
    entries, total = _walk_entries(source_root, MAX_SOURCE_FILES,
        MAX_PINNED_FILE_BYTES, MAX_SOURCE_BYTES, "package source"; safe_links=true)
    _require(all(entry -> entry.link_target === nothing, entries),
        "package source contains a symbolic link")
    paths = String["Project.toml"]
    for entry in entries
        _regular(entry.metadata) || continue
        push!(paths, "src/" * entry.relative)
    end
    sort!(paths)
    return paths, total
end

function _source_composite(paths::Vector{String}, bytes::Dict{String,Vector{UInt8}})
    context = SHA.SHA2_256_CTX()
    SHA.update!(context, codeunits(SOURCE_COMPOSITE_PREFIX))
    total = Int64(0)
    for relative in sort(paths)
        payload = bytes[relative]
        total = Base.checked_add(total, Int64(length(payload)))
        SHA.update!(context, codeunits("F\0"))
        SHA.update!(context, codeunits(relative))
        SHA.update!(context, codeunits("\0"))
        SHA.update!(context, codeunits(string(length(payload))))
        SHA.update!(context, codeunits("\0"))
        SHA.update!(context, SHA.sha256(payload))
    end
    return bytes2hex(SHA.digest!(context)), total
end

function _load_pinned_files(root::String, descriptor::Dict{String,Any})
    expected = _descriptor_files(descriptor)
    source_paths, _ = _source_inventory(root)
    fixed = Dict(
        "project_file" => "Project.toml",
        "manifest_file" => "test/conformance/n6/julia/Manifest.toml",
        "bootstrap_file" => "test/conformance/n6/julia/bootstrap.jl",
        "harness_file" => "test/conformance/n6/julia/N6ParquetJLHarness.jl",
        "runner_file" => "test/conformance/n6/julia/generate.jl",
    )
    required = Set(source_paths)
    for (field, relative) in fixed
        _require(_string_field(descriptor, field, "descriptor") == relative,
            "descriptor $field differs")
        push!(required, relative)
    end
    _require(Set(keys(expected)) == required,
        "producer descriptor file inventory differs from source and bootstrap inputs")
    pinned = Dict{String,Vector{UInt8}}()
    for relative in sort!(collect(required))
        path = _safe_path(root, relative, "descriptor file")
        payload = _stable_file_bytes(path, MAX_PINNED_FILE_BYTES,
            "descriptor file $relative")
        _require(bytes2hex(SHA.sha256(payload)) == expected[relative],
            "descriptor file digest differs: $relative")
        pinned[relative] = payload
    end
    hash_fields = Dict(
        "project_file" => "project_sha256",
        "manifest_file" => "manifest_sha256",
        "bootstrap_file" => "bootstrap_sha256",
        "harness_file" => "harness_sha256",
        "runner_file" => "runner_sha256",
    )
    for (file_field, hash_field) in hash_fields
        relative = descriptor[file_field]
        _require(bytes2hex(SHA.sha256(pinned[relative])) == descriptor[hash_field],
            "descriptor $hash_field differs from its file entry")
    end
    composite, total = _source_composite(source_paths, pinned)
    _require(composite == descriptor["source_composite_sha256"],
        "Parquet.jl source composite differs")
    _require(length(source_paths) == descriptor["source_file_count"],
        "Parquet.jl source file count differs")
    _require(total == descriptor["source_total_bytes"],
        "Parquet.jl source byte count differs")
    return pinned
end

function _validate_project(descriptor::Dict{String,Any}, pinned::Dict{String,Vector{UInt8}})
    project = TOML.parse(String(copy(pinned[descriptor["project_file"]])))
    _require(get(project, "name", nothing) == PACKAGE_NAME,
        "gate project has the wrong package name")
    _require(get(project, "uuid", nothing) == PACKAGE_UUID,
        "gate project has the wrong package UUID")
    _require(get(project, "version", nothing) == descriptor["package_version"],
        "gate project has the wrong package version")
    _require(!haskey(project, "preferences"),
        "gate project contains exported preferences")
    return project
end

function _manifest_entries(manifest::Dict{String,Any})
    _require(get(manifest, "julia_version", nothing) == string(CANONICAL_JULIA_VERSION),
        "producer manifest has the wrong Julia version")
    _require(get(manifest, "manifest_format", nothing) == "2.0",
        "producer manifest has the wrong format")
    dependencies = get(manifest, "deps", nothing)
    _require(dependencies isa AbstractDict, "producer manifest lacks dependencies")
    entries = Dict{String,Dict{String,Any}}()
    for (name_value, records) in dependencies
        name = String(name_value)
        _require(records isa AbstractVector && length(records) == 1 &&
            only(records) isa Dict{String,Any},
            "producer manifest dependency is not singular: $name")
        entries[name] = only(records)
    end
    return entries
end

function _validate_manifest(root::String, descriptor::Dict{String,Any},
        pinned::Dict{String,Vector{UInt8}})
    relative = descriptor["manifest_file"]
    payload = pinned[relative]
    active_path = _safe_path(root, "Manifest.toml", "active manifest")
    active = _stable_file_bytes(active_path, MAX_PINNED_FILE_BYTES,
        "active gate Manifest.toml")
    _require(active == payload,
        "active gate Manifest.toml differs from the pinned producer manifest")
    manifest = TOML.parse(String(copy(payload)))
    entries = _manifest_entries(manifest)
    _require(haskey(entries, PACKAGE_NAME), "producer manifest lacks Parquet")
    parquet = entries[PACKAGE_NAME]
    _require(get(parquet, "uuid", nothing) == PACKAGE_UUID,
        "producer manifest Parquet UUID differs")
    _require(get(parquet, "version", nothing) == descriptor["package_version"],
        "producer manifest Parquet version differs")
    _require(get(parquet, "path", nothing) == ".",
        "producer manifest Parquet path is not the gate root")
    for (name, entry) in entries
        name == PACKAGE_NAME && continue
        _require(!haskey(entry, "path"),
            "producer manifest has an untrusted path dependency: $name")
    end
    return manifest
end

function _dependency_descriptors(descriptor::Dict{String,Any})
    records = Dict{String,Dict{String,Any}}()
    for (index, entry_value) in enumerate(_table_array(descriptor, "dependency",
            "descriptor"))
        entry = entry_value::Dict{String,Any}
        label = "descriptor dependency $index"
        _exact_keys(entry, ["depot_slug", "entry_count", "git_tree_sha1", "name",
            "total_bytes", "tree_sha256", "uuid", "version"], label)
        name = _string_field(entry, "name", label)
        _require(occursin(r"^[A-Za-z][A-Za-z0-9_]*$", name),
            "$label name is invalid")
        _require(!haskey(records, name), "duplicate descriptor dependency: $name")
        _hex_sha1(entry["git_tree_sha1"], "$label Git tree")
        _hex_sha256(entry["tree_sha256"], "$label tree")
        slug = _string_field(entry, "depot_slug", label)
        _require(occursin(r"^[A-Za-z0-9]{5}$", slug),
            "$label depot slug is invalid")
        _require(_integer_field(entry, "entry_count", label) > 0,
            "$label entry count is not positive")
        _require(_integer_field(entry, "total_bytes", label) >= 0,
            "$label total byte count is negative")
        uuid = _string_field(entry, "uuid", label)
        version = _string_field(entry, "version", label)
        try
            Base.UUID(uuid)
            VersionNumber(version)
        catch error
            throw(ArgumentError("$label identity is invalid: $error"))
        end
        records[name] = entry
    end
    _require(!isempty(records), "producer descriptor has no dependency trees")
    return records
end

function _validate_depot_shape(depot::String, dependencies, artifacts)
    top = sort!(readdir(depot))
    _require(top == ["artifacts", "packages"],
        "private depot has unexpected top-level entries")
    package_root = joinpath(depot, "packages")
    _require(isdir(package_root) && !islink(package_root),
        "private depot packages directory is invalid")
    _require(sort!(readdir(package_root)) == sort!(collect(keys(dependencies))),
        "private depot package names differ from the descriptor")
    for (name, entry) in dependencies
        slug = Base.version_slug(Base.UUID(entry["uuid"]),
            Base.SHA1(entry["git_tree_sha1"]))
        _require(slug == entry["depot_slug"],
            "descriptor dependency depot slug is invalid: $name")
        _require(readdir(joinpath(package_root, name)) == [slug],
            "private depot package slug differs: $name")
    end
    artifact_root = joinpath(depot, "artifacts")
    _require(isdir(artifact_root) && !islink(artifact_root),
        "private depot artifacts directory is invalid")
    expected = sort!(String[entry["git_tree_sha1"] for entry in values(artifacts)])
    _require(sort!(readdir(artifact_root)) == expected,
        "private depot artifact trees differ from the descriptor")
    return
end

function _artifact_descriptors(descriptor::Dict{String,Any})
    records = Dict{String,Dict{String,Any}}()
    for (index, entry_value) in enumerate(_table_array(descriptor, "artifact",
            "descriptor"))
        entry = entry_value::Dict{String,Any}
        label = "descriptor artifact $index"
        _exact_keys(entry, ["entry_count", "git_tree_sha1", "name", "package",
            "total_bytes", "tree_sha256"], label)
        package = _string_field(entry, "package", label)
        name = _string_field(entry, "name", label)
        key = package * "\0" * name
        _require(!haskey(records, key), "duplicate descriptor artifact: $package/$name")
        _hex_sha1(entry["git_tree_sha1"], "$label Git tree")
        _hex_sha256(entry["tree_sha256"], "$label tree")
        _require(_integer_field(entry, "entry_count", label) > 0,
            "$label entry count is not positive")
        _require(_integer_field(entry, "total_bytes", label) >= 0,
            "$label total byte count is negative")
        records[key] = entry
    end
    _require(!isempty(records), "producer descriptor has no native artifacts")
    return records
end

function _selected_artifacts(dependency_root::String, package::String)
    path = _safe_path(dependency_root, "Artifacts.toml", "$package artifact declaration")
    payload = _stable_file_bytes(path, MAX_PINNED_FILE_BYTES,
        "$package Artifacts.toml")
    declaration = TOML.parse(String(copy(payload)))
    selected = Dict{String,String}()
    for (name_value, entries) in declaration
        name = String(name_value)
        _require(entries isa AbstractVector,
            "$package artifact declaration is not an array: $name")
        matches = Any[entry for entry in entries if entry isa AbstractDict &&
            get(entry, "arch", nothing) == "aarch64" &&
            get(entry, "os", nothing) == "macos"]
        _require(length(matches) == 1,
            "$package has no singular macOS aarch64 artifact: $name")
        selected[name] = _hex_sha1(only(matches)["git-tree-sha1"],
            "$package selected artifact $name")
    end
    _require(!isempty(selected), "$package has no selected artifacts")
    return selected
end

function _validate_dependencies(depot::String, descriptor::Dict{String,Any},
        manifest::Dict{String,Any})
    dependencies = _dependency_descriptors(descriptor)
    artifacts = _artifact_descriptors(descriptor)
    entries = _manifest_entries(manifest)
    expected_names = Set(name for (name, entry) in entries if
        haskey(entry, "git-tree-sha1"))
    _require(Set(keys(dependencies)) == expected_names,
        "descriptor dependency trees differ from the producer manifest")
    _validate_depot_shape(depot, dependencies, artifacts)
    roots = Dict{String,String}()
    for (name, dependency) in dependencies
        manifest_entry = entries[name]
        for field in ("uuid", "version")
            _require(get(manifest_entry, field, nothing) == dependency[field],
                "descriptor dependency $name $field differs from the manifest")
        end
        _require(get(manifest_entry, "git-tree-sha1", nothing) ==
            dependency["git_tree_sha1"],
            "descriptor dependency $name Git tree differs from the manifest")
        slug = Base.version_slug(Base.UUID(dependency["uuid"]),
            Base.SHA1(dependency["git_tree_sha1"]))
        _require(slug == dependency["depot_slug"],
            "descriptor dependency depot slug differs: $name")
        root = _canonical_root(joinpath(depot, "packages", name, slug),
            "private dependency $name")
        identity = _tree_identity(root, MAX_TREE_ENTRIES, MAX_TREE_FILE_BYTES,
            MAX_TREE_BYTES, "private dependency $name")
        _require(identity.sha256 == dependency["tree_sha256"],
            "private dependency tree digest differs: $name")
        _require(identity.entry_count == dependency["entry_count"],
            "private dependency entry count differs: $name")
        _require(identity.total_bytes == dependency["total_bytes"],
            "private dependency total byte count differs: $name")
        roots[name] = root
    end
    selected = Dict{String,String}()
    jll_names = sort!(String[name for name in keys(dependencies) if
        endswith(name, "_jll")])
    for package in jll_names
        for (name, git_tree) in _selected_artifacts(roots[package], package)
            selected[package * "\0" * name] = git_tree
        end
    end
    _require(Set(keys(artifacts)) == Set(keys(selected)),
        "descriptor native artifacts differ from selected JLL artifacts")
    artifact_roots = Dict{String,String}()
    for (key, artifact) in artifacts
        _require(artifact["git_tree_sha1"] == selected[key],
            "descriptor selected artifact Git tree differs: $key")
        root = _canonical_root(joinpath(depot, "artifacts",
            artifact["git_tree_sha1"]), "private artifact $key")
        identity = _tree_identity(root, MAX_TREE_ENTRIES, MAX_TREE_FILE_BYTES,
            MAX_TREE_BYTES, "private artifact $key")
        _require(identity.sha256 == artifact["tree_sha256"],
            "private artifact tree digest differs: $key")
        _require(identity.entry_count == artifact["entry_count"],
            "private artifact entry count differs: $key")
        _require(identity.total_bytes == artifact["total_bytes"],
            "private artifact total byte count differs: $key")
        artifact_roots[key] = root
    end
    return roots, artifact_roots
end

function _validate_runtime(descriptor::Dict{String,Any})
    _require(VERSION == CANONICAL_JULIA_VERSION,
        "N6 producer requires Julia $CANONICAL_JULIA_VERSION")
    _require(Sys.isapple() && Sys.ARCH === :aarch64,
        "N6 producer requires macOS aarch64")
    command = Base.julia_cmd().exec
    _require(!isempty(command), "Julia command has no executable")
    executable = String(first(command))
    _require(isabspath(executable), "Julia executable is not absolute")
    executable = normpath(executable)
    _require(!islink(executable) && realpath(executable) == executable,
        "Julia executable is not canonical")
    _require(executable == joinpath(Sys.BINDIR, "julia"),
        "Julia executable is outside Sys.BINDIR")
    digest, _ = _stable_file_digest(executable, MAX_PINNED_FILE_BYTES,
        "Julia executable")
    _require(bytes2hex(digest) == descriptor["julia_executable_sha256"],
        "Julia executable digest differs")
    runtime_root = _canonical_root(dirname(Sys.BINDIR), "Julia runtime root")
    identity = _tree_identity(runtime_root, MAX_RUNTIME_ENTRIES,
        MAX_RUNTIME_FILE_BYTES, MAX_RUNTIME_BYTES, "Julia runtime";
        safe_links=false)
    _require(identity.sha256 == descriptor["julia_runtime_tree_sha256"],
        "Julia runtime tree digest differs")
    _require(identity.entry_count == descriptor["julia_runtime_entry_count"],
        "Julia runtime entry count differs")
    _require(identity.total_bytes == descriptor["julia_runtime_total_bytes"],
        "Julia runtime byte count differs")
    return runtime_root
end

function _package_id(name::String, entry::Dict{String,Any})
    return Base.PkgId(Base.UUID(entry["uuid"]), name)
end

function _resolved_path(name::String, entry::Dict{String,Any})
    path = Base.locate_package(_package_id(name, entry))
    _require(path !== nothing, "manifest package cannot be resolved: $name")
    absolute = normpath(String(path))
    _require(isfile(absolute) && !islink(absolute),
        "resolved package entry is not a regular file: $name")
    _require(realpath(absolute) == absolute,
        "resolved package entry is not canonical: $name")
    return absolute
end

function _validate_resolution(context::BootstrapContext)
    entries = _manifest_entries(context.manifest)
    for (name, entry) in entries
        path = _resolved_path(name, entry)
        if name == PACKAGE_NAME
            _require(path == joinpath(context.root, "src", "Parquet.jl"),
                "Parquet resolves outside the read-only gate root")
        elseif haskey(entry, "git-tree-sha1")
            _require(_within(path, context.dependency_roots[name]),
                "dependency resolves outside the private depot: $name")
        else
            _require(_within(path, context.runtime_root),
                "stdlib resolves outside the exact Julia runtime: $name")
        end
    end
    return
end

function _read_descriptor(root::String)
    expected = get(ENV, DESCRIPTOR_SHA256_ENV, nothing)
    _hex_sha256(expected, "trusted producer descriptor")
    path = _safe_path(root, DESCRIPTOR_RELATIVE, "producer descriptor")
    payload = _stable_file_bytes(path, MAX_DESCRIPTOR_BYTES,
        "Parquet.jl producer descriptor")
    digest = bytes2hex(SHA.sha256(payload))
    _require(digest == expected,
        "Parquet.jl producer descriptor digest differs from the trusted parent pin")
    descriptor_value = TOML.parse(String(copy(payload)))
    _require(descriptor_value isa Dict{String,Any},
        "Parquet.jl producer descriptor is not a TOML table")
    descriptor = descriptor_value::Dict{String,Any}
    _validate_descriptor(descriptor)
    return path, digest, descriptor
end

function _verify_preload()
    _validate_bootstrap_stdlibs()
    root = _canonical_root(pwd(), "N6 gate root")
    depot_value = get(ENV, "JULIA_DEPOT_PATH", nothing)
    _require(depot_value isa String && !isempty(depot_value),
        "JULIA_DEPOT_PATH is absent")
    _require(!occursin(':', depot_value),
        "JULIA_DEPOT_PATH contains more than one depot")
    depot = _canonical_root(depot_value, "N6 private depot")
    _require(!_within(depot, root) && !_within(root, depot),
        "gate root and private depot overlap")
    _validate_environment(root, depot)
    expected_program = joinpath(root, "test", "conformance", "n6", "julia",
        "bootstrap.jl")
    _require(!isempty(PROGRAM_FILE) && realpath(PROGRAM_FILE) == expected_program,
        "Julia did not execute the pinned N6 bootstrap file directly")
    descriptor_path, descriptor_sha256, descriptor = _read_descriptor(root)
    _assert_readonly_gate(root)
    _assert_readonly_depot(depot)
    pinned = _load_pinned_files(root, descriptor)
    _validate_project(descriptor, pinned)
    manifest = _validate_manifest(root, descriptor, pinned)
    runtime_root = _validate_runtime(descriptor)
    dependency_roots, artifact_roots = _validate_dependencies(depot, descriptor,
        manifest)
    context = BootstrapContext(root, depot, runtime_root, descriptor_path,
        descriptor_sha256, descriptor, manifest, pinned, dependency_roots,
        artifact_roots)
    _validate_resolution(context)
    _require(bytes2hex(SHA.sha256(_stable_file_bytes(descriptor_path,
        MAX_DESCRIPTOR_BYTES, "Parquet.jl producer descriptor"))) ==
        descriptor_sha256, "producer descriptor changed before package loading")
    return context
end

function _verify_tree_identities(context::BootstrapContext)
    runtime = _tree_identity(context.runtime_root, MAX_RUNTIME_ENTRIES,
        MAX_RUNTIME_FILE_BYTES, MAX_RUNTIME_BYTES, "Julia runtime";
        safe_links=false)
    _require(runtime.sha256 == context.descriptor["julia_runtime_tree_sha256"],
        "Julia runtime changed after package execution")
    _require(runtime.entry_count ==
        context.descriptor["julia_runtime_entry_count"],
        "Julia runtime entry count changed after package execution")
    _require(runtime.total_bytes ==
        context.descriptor["julia_runtime_total_bytes"],
        "Julia runtime byte count changed after package execution")
    dependencies = _dependency_descriptors(context.descriptor)
    for (name, root) in context.dependency_roots
        identity = _tree_identity(root, MAX_TREE_ENTRIES, MAX_TREE_FILE_BYTES,
            MAX_TREE_BYTES, "private dependency $name")
        _require(identity.sha256 == dependencies[name]["tree_sha256"],
            "private dependency changed after package execution: $name")
        _require(identity.entry_count == dependencies[name]["entry_count"],
            "private dependency entry count changed after package execution: $name")
        _require(identity.total_bytes == dependencies[name]["total_bytes"],
            "private dependency byte count changed after package execution: $name")
    end
    artifacts = _artifact_descriptors(context.descriptor)
    for (key, root) in context.artifact_roots
        identity = _tree_identity(root, MAX_TREE_ENTRIES, MAX_TREE_FILE_BYTES,
            MAX_TREE_BYTES, "private artifact $key")
        _require(identity.sha256 == artifacts[key]["tree_sha256"],
            "private artifact changed after package execution: $key")
        _require(identity.entry_count == artifacts[key]["entry_count"],
            "private artifact entry count changed after package execution: $key")
        _require(identity.total_bytes == artifacts[key]["total_bytes"],
            "private artifact byte count changed after package execution: $key")
    end
    return
end

function _verify_postload(context::BootstrapContext)
    _validate_environment(context.root, context.depot)
    _validate_resolution(context)
    _require(pathof(Parquet) == joinpath(context.root, "src", "Parquet.jl"),
        "loaded Parquet module is outside the read-only gate root")
    descriptor_payload = _stable_file_bytes(context.descriptor_path,
        MAX_DESCRIPTOR_BYTES, "Parquet.jl producer descriptor")
    _require(bytes2hex(SHA.sha256(descriptor_payload)) == context.descriptor_sha256,
        "producer descriptor changed after package execution")
    pinned = _load_pinned_files(context.root, context.descriptor)
    _require(pinned == context.pinned_bytes,
        "descriptor-bound input changed after package execution")
    _verify_tree_identities(context)
    _assert_readonly_gate(context.root)
    _assert_readonly_depot(context.depot)
    return
end

const CONTEXT = _verify_preload()

using Parquet

_validate_resolution(CONTEXT)
_require(pathof(Parquet) == joinpath(CONTEXT.root, "src", "Parquet.jl"),
    "loaded Parquet module is outside the read-only gate root")

const HARNESS_RELATIVE = CONTEXT.descriptor["harness_file"]
Base.include_string(@__MODULE__, String(copy(CONTEXT.pinned_bytes[HARNESS_RELATIVE])),
    joinpath(CONTEXT.root, split(HARNESS_RELATIVE, '/')...))
const OUTPUT = N6ParquetJLHarness.runharness(:check)

_verify_postload(CONTEXT)
println("checked ", length(OUTPUT.files),
    " generated N6 files and 96 evidence records in the isolated Parquet.jl gate")

end
