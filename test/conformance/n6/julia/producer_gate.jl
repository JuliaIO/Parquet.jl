using Pkg
using UUIDs

const PARQUET_JL_DESCRIPTOR_RELATIVE =
    "test/conformance/n6/julia/parquet-jl-producer.toml"
const PARQUET_JL_MANIFEST_RELATIVE =
    "test/conformance/n6/julia/Manifest.toml"
const PARQUET_JL_BOOTSTRAP_RELATIVE =
    "test/conformance/n6/julia/bootstrap.jl"
const PARQUET_JL_SOURCE_COMPOSITE_ALGORITHM =
    "parquet-jl-n6-source-composite-v1"
const PARQUET_JL_SOURCE_COMPOSITE_PREFIX =
    UInt8[codeunits(PARQUET_JL_SOURCE_COMPOSITE_ALGORITHM * "\0")...]
const PARQUET_JL_DEPOT_SOURCE_ENV = "PARQUET_N6_JULIA_SOURCE_DEPOT"
const PARQUET_JL_TREE_ENTRY_LIMIT = 10_000
const PARQUET_JL_TREE_FILE_LIMIT = 256 * 1024 * 1024
const PARQUET_JL_TREE_TOTAL_LIMIT = 512 * 1024 * 1024

function parquet_jl_source_paths(root::AbstractString)
    project = joinpath(root, "Project.toml")
    islink(project) && error("Parquet.jl Project.toml is a symbolic link")
    isfile(project) || error("Parquet.jl Project.toml is absent")
    source = joinpath(root, "src")
    islink(source) && error("Parquet.jl source root is a symbolic link")
    isdir(source) || error("Parquet.jl source root is absent")
    paths = String["Project.toml"]
    for (directory, directories, files) in walkdir(source; follow_symlinks=false)
        for name in directories
            path = joinpath(directory, name)
            islink(path) && error("Parquet.jl source directory is a symbolic link: " *
                replace(relpath(path, root), Base.Filesystem.path_separator => '/'))
        end
        for name in files
            path = joinpath(directory, name)
            relative = replace(relpath(path, root),
                Base.Filesystem.path_separator => '/')
            islink(path) && error("Parquet.jl source file is a symbolic link: $relative")
            isfile(path) || error("Parquet.jl source entry is not a file: $relative")
            safe_relative(relative) || error("unsafe Parquet.jl source path: $relative")
            push!(paths, relative)
        end
    end
    sort!(paths)
    return paths
end

function parquet_jl_source_composite(entries, snapshots)
    rows = Pair{String,FileSnapshot}[]
    for item in entries
        path = item["path"]
        snapshot = control_snapshot(snapshots, path)
        snapshot.sha256 == item["sha256"] ||
            error("Parquet.jl source file hash differs: $path")
        push!(rows, path => snapshot)
    end
    sort!(rows; by=first)
    paths = first.(rows)
    length(paths) == length(unique(paths)) ||
        error("Parquet.jl source descriptor has duplicate paths")
    output = IOBuffer()
    write(output, PARQUET_JL_SOURCE_COMPOSITE_PREFIX)
    for (path, snapshot) in rows
        safe_relative(path) || error("unsafe Parquet.jl composite path: $path")
        all(isascii, path) || error("non-ASCII Parquet.jl composite path: $path")
        write(output, "F\0", path, '\0', string(length(snapshot.payload)), '\0')
        write(output, hex2bytes(snapshot.sha256))
    end
    return bytes2hex(SHA.sha256(take!(output)))
end

function parquet_jl_manifest_dependencies(manifest)
    dependencies = Dict{String,Dict{String,Any}}()
    path_packages = String[]
    for (name, records) in manifest["deps"]
        length(records) == 1 || error("Julia manifest has duplicate package $name")
        record = only(records)
        if haskey(record, "path")
            push!(path_packages, name)
            continue
        end
        haskey(record, "git-tree-sha1") || continue
        dependencies[name] = record
    end
    path_packages == ["Parquet"] ||
        error("Julia manifest path package inventory differs")
    only(manifest["deps"]["Parquet"])["path"] == "." ||
        error("Julia manifest Parquet path differs")
    return dependencies
end

function parquet_jl_dependency_map(descriptor)
    dependencies = Dict{String,Dict{String,Any}}()
    for dependency in descriptor["dependency"]
        name = dependency["name"]
        haskey(dependencies, name) &&
            error("duplicate Parquet.jl dependency descriptor: $name")
        dependencies[name] = dependency
    end
    return dependencies
end

function validate_parquet_jl_dependency_descriptor(dependency, manifest_entry)
    require_keys(dependency, ["name", "uuid", "version", "git_tree_sha1",
        "depot_slug", "tree_sha256", "entry_count", "total_bytes"])
    @test all(field -> dependency[field] isa String,
        ("name", "uuid", "version", "git_tree_sha1", "depot_slug",
            "tree_sha256"))
    @test dependency["uuid"] == manifest_entry["uuid"]
    @test dependency["version"] == manifest_entry["version"]
    @test dependency["git_tree_sha1"] == manifest_entry["git-tree-sha1"]
    @test occursin(GIT_PATTERN, dependency["git_tree_sha1"])
    @test occursin(SHA256_PATTERN, dependency["tree_sha256"])
    @test occursin(r"^[A-Za-z0-9]{5}$", dependency["depot_slug"])
    @test dependency["entry_count"] isa Int64
    @test dependency["total_bytes"] isa Int64
    @test 0 < dependency["entry_count"] <= PARQUET_JL_TREE_ENTRY_LIMIT
    @test 0 < dependency["total_bytes"] <= PARQUET_JL_TREE_TOTAL_LIMIT
    uuid = UUID(dependency["uuid"])
    tree = Base.SHA1(hex2bytes(dependency["git_tree_sha1"]))
    @test dependency["depot_slug"] == Base.version_slug(uuid, tree)
    return
end

function validate_parquet_jl_artifact_descriptor(artifact)
    require_keys(artifact, ["package", "name", "git_tree_sha1",
        "tree_sha256", "entry_count", "total_bytes"])
    @test all(field -> artifact[field] isa String,
        ("package", "name", "git_tree_sha1", "tree_sha256"))
    @test occursin(GIT_PATTERN, artifact["git_tree_sha1"])
    @test occursin(SHA256_PATTERN, artifact["tree_sha256"])
    @test artifact["entry_count"] isa Int64
    @test artifact["total_bytes"] isa Int64
    @test 0 < artifact["entry_count"] <= PARQUET_JL_TREE_ENTRY_LIMIT
    @test 0 < artifact["total_bytes"] <= PARQUET_JL_TREE_TOTAL_LIMIT
    return
end

function validate_parquet_jl_descriptor(manifest, capabilities, snapshots)
    descriptor_snapshot = control_snapshot(snapshots,
        manifest["parquet_jl_producer_descriptor_file"])
    @test descriptor_snapshot.sha256 ==
        manifest["parquet_jl_producer_descriptor_sha256"]
    descriptor = parse_toml_snapshot(descriptor_snapshot)
    require_keys(descriptor, ["descriptor_version", "status", "producer",
        "package_name", "package_uuid", "package_version", "julia_version",
        "platform",
        "julia_executable_sha256", "julia_runtime_tree_sha256",
        "julia_runtime_entry_count", "julia_runtime_total_bytes",
        "source_composite_algorithm", "source_composite_sha256",
        "source_file_count", "source_total_bytes", "project_file",
        "project_sha256", "manifest_file", "manifest_sha256",
        "harness_file", "harness_sha256", "runner_file", "runner_sha256",
        "bootstrap_file", "bootstrap_sha256", "artifact", "dependency",
        "file"])
    @test descriptor["descriptor_version"] == 1
    @test descriptor["status"] in ("planned", "verified")
    @test descriptor["producer"] == "parquet-jl"
    @test descriptor["package_name"] == "Parquet"
    @test descriptor["package_uuid"] ==
        "626c502c-15b0-58ad-a749-f091afb673ae"
    @test descriptor["package_version"] == "1.0.0-DEV"
    @test descriptor["julia_version"] == "1.12.6"
    @test descriptor["platform"] == "macos-15-arm64"
    @test descriptor["julia_runtime_entry_count"] == 7180
    @test descriptor["julia_runtime_total_bytes"] == 820750312
    @test descriptor["source_composite_algorithm"] ==
        PARQUET_JL_SOURCE_COMPOSITE_ALGORITHM
    for (file_key, hash_key) in (("project_file", "project_sha256"),
            ("manifest_file", "manifest_sha256"),
            ("harness_file", "harness_sha256"),
            ("runner_file", "runner_sha256"),
            ("bootstrap_file", "bootstrap_sha256"))
        @test safe_relative(descriptor[file_key])
        @test occursin(SHA256_PATTERN, descriptor[hash_key])
        @test control_snapshot(snapshots,
            descriptor[file_key]).sha256 == descriptor[hash_key]
    end
    @test descriptor["project_file"] == "Project.toml"
    @test descriptor["manifest_file"] == PARQUET_JL_MANIFEST_RELATIVE
    @test descriptor["bootstrap_file"] == PARQUET_JL_BOOTSTRAP_RELATIVE
    file_map = Dict{String,Dict{String,Any}}()
    for item in descriptor["file"]
        require_keys(item, ["path", "sha256"])
        @test item["path"] isa String
        @test item["sha256"] isa String
        @test safe_relative(item["path"])
        @test occursin(SHA256_PATTERN, item["sha256"])
        @test !haskey(file_map, item["path"])
        @test control_snapshot(snapshots, item["path"]).sha256 == item["sha256"]
        file_map[item["path"]] = item
    end
    source_paths = parquet_jl_source_paths(REPO_DIR)
    source_entries = Dict{String,Any}[file_map[path] for path in source_paths]
    support_paths = Set([descriptor["manifest_file"], descriptor["harness_file"],
        descriptor["runner_file"], descriptor["bootstrap_file"]])
    @test Set(keys(file_map)) == union(Set(source_paths), support_paths)
    @test descriptor["source_file_count"] == length(source_paths)
    @test descriptor["source_total_bytes"] ==
        sum(length(control_snapshot(snapshots, path).payload) for path in source_paths)
    composite = parquet_jl_source_composite(source_entries, snapshots)
    @test composite == descriptor["source_composite_sha256"]
    @test composite == manifest["parquet_jl_source_composite_sha256"]
    julia_manifest = TOML.parse(String(copy(control_snapshot(snapshots,
        descriptor["manifest_file"]).payload)))
    @test julia_manifest["julia_version"] == descriptor["julia_version"]
    manifest_dependencies = parquet_jl_manifest_dependencies(julia_manifest)
    descriptor_dependencies = parquet_jl_dependency_map(descriptor)
    @test Set(keys(descriptor_dependencies)) == Set(keys(manifest_dependencies))
    for (name, dependency) in descriptor_dependencies
        validate_parquet_jl_dependency_descriptor(dependency,
            manifest_dependencies[name])
    end
    artifact_keys = Set{Tuple{String,String}}()
    for artifact in descriptor["artifact"]
        validate_parquet_jl_artifact_descriptor(artifact)
        key = (artifact["package"], artifact["name"])
        @test key ∉ artifact_keys
        push!(artifact_keys, key)
    end
    @test artifact_keys == Set([
        ("Lz4_jll", "Lz4"),
        ("Zstd_jll", "Zstd"),
        ("brotli_jll", "brotli"),
        ("snappy_jll", "snappy"),
    ])
    authority = only(filter(item -> item["id"] == "parquet-jl",
        capabilities["authority"]))
    @test authority["revision"] == descriptor["source_composite_sha256"]
    @test descriptor_snapshot.sha256 in authority["toolchain_sha256"]
    toolchain = only(filter(item -> item["id"] == "parquet-jl-evidence",
        manifest["toolchain"]))
    @test toolchain["status"] == descriptor["status"]
    @test toolchain_artifact(manifest, "parquet-jl-evidence",
        "parquet-jl-producer.toml") == descriptor_snapshot.sha256
    return descriptor
end

function parquet_jl_tree_identity(root::AbstractString, item)
    inventory = validate_bounded_tree(root;
        max_entries=Int(item["entry_count"]),
        max_file_bytes=min(Int(item["total_bytes"]),
            PARQUET_JL_TREE_FILE_LIMIT),
        max_total_bytes=Int(item["total_bytes"]))
    require_gate(inventory.entries == item["entry_count"],
        "tree entry count differs: $root")
    require_gate(inventory.bytes == item["total_bytes"],
        "tree byte count differs: $root")
    require_gate(inventory.sha256 == item["tree_sha256"],
        "tree SHA-256 differs: $root")
    require_gate(bytes2hex(Pkg.GitTools.tree_hash(root)) ==
        item["git_tree_sha1"], "tree Git identity differs: $root")
    return inventory
end

function parquet_jl_source_depots()
    explicit = get(ENV, PARQUET_JL_DEPOT_SOURCE_ENV, "")
    if !isempty(explicit)
        islink(explicit) && error("Parquet.jl source depot is a symbolic link")
        isdir(explicit) || error("Parquet.jl source depot is absent")
        return String[realpath(explicit)]
    end
    roots = String[]
    for depot in Base.DEPOT_PATH
        isdir(depot) || continue
        root = realpath(depot)
        root in roots || push!(roots, root)
    end
    isempty(roots) && error("no Julia source depot is available")
    return roots
end

function locate_parquet_jl_dependency(item, depots)
    relative = joinpath("packages", item["name"], item["depot_slug"])
    failures = String[]
    for depot in depots
        candidate = joinpath(depot, relative)
        ispath(candidate) || continue
        try
            islink(candidate) && error("dependency root is a symbolic link")
            parquet_jl_tree_identity(candidate, item)
            return realpath(candidate)
        catch error
            push!(failures, "$candidate: $(sprint(showerror, error))")
        end
    end
    detail = isempty(failures) ? "no candidate exists" : join(failures, "; ")
    error("exact Parquet.jl dependency source is unavailable for " *
        item["name"] * ": " * detail)
end

function locate_parquet_jl_artifact(item, depots)
    relative = joinpath("artifacts", item["git_tree_sha1"])
    failures = String[]
    for depot in depots
        candidate = joinpath(depot, relative)
        ispath(candidate) || continue
        try
            islink(candidate) && error("artifact root is a symbolic link")
            parquet_jl_tree_identity(candidate, item)
            return realpath(candidate)
        catch error
            push!(failures, "$candidate: $(sprint(showerror, error))")
        end
    end
    detail = isempty(failures) ? "no candidate exists" : join(failures, "; ")
    error("exact Parquet.jl native artifact is unavailable for " *
        item["package"] * ":" * item["name"] * ": " * detail)
end

function copy_parquet_jl_tree(source::AbstractString,
        destination::AbstractString, item)
    ispath(destination) && error("private Julia depot target already exists")
    mkpath(dirname(destination))
    before = parquet_jl_tree_identity(source, item)
    cp(source, destination; follow_symlinks=false)
    require_gate(parquet_jl_tree_identity(source, item) == before,
        "source tree changed while copied: $source")
    require_gate(parquet_jl_tree_identity(destination, item) == before,
        "private Julia depot copy differs: $destination")
    return
end

function set_parquet_jl_tree_locked!(root::AbstractString, locked::Bool)
    if !locked
        root_mode = stat(root).mode & 0o777
        chmod(root, root_mode | 0o700)
        for (directory, children, files) in walkdir(root; follow_symlinks=false)
            for name in children
                path = joinpath(directory, name)
                islink(path) && continue
                mode = stat(path).mode & 0o777
                chmod(path, mode | 0o700)
            end
            for name in files
                path = joinpath(directory, name)
                islink(path) && continue
                mode = stat(path).mode & 0o777
                chmod(path, mode | 0o200)
            end
        end
        return
    end
    directories = String[]
    for (directory, children, files) in walkdir(root; follow_symlinks=false)
        push!(directories, directory)
        for name in children
            islink(joinpath(directory, name)) && continue
        end
        for name in files
            path = joinpath(directory, name)
            islink(path) && continue
            mode = stat(path).mode & 0o777
            chmod(path, mode & 0o555)
        end
    end
    for directory in reverse(directories)
        mode = stat(directory).mode & 0o777
        chmod(directory, mode & 0o555)
    end
    return
end

function validate_private_parquet_jl_depot(depot::AbstractString, descriptor)
    expected_top = sort(["artifacts", "packages"])
    require_gate(sort(readdir(depot)) == expected_top,
        "private Julia depot top-level inventory differs")
    for dependency in descriptor["dependency"]
        root = checked_directory(depot,
            joinpath("packages", dependency["name"], dependency["depot_slug"]))
        parquet_jl_tree_identity(root, dependency)
    end
    for artifact in descriptor["artifact"]
        root = checked_directory(depot,
            joinpath("artifacts", artifact["git_tree_sha1"]))
        parquet_jl_tree_identity(root, artifact)
    end
    for forbidden in ("compiled", "config", "environments", "logs",
            "prefs", "registries", "scratchspaces")
        require_gate(!ispath(joinpath(depot, forbidden)),
            "private Julia depot contains forbidden state: $forbidden")
    end
    return
end

function checked_directory(root::AbstractString, relative::AbstractString)
    safe_relative(replace(relative, Base.Filesystem.path_separator => '/')) ||
        error("unsafe relative directory: $relative")
    root_path = realpath(root)
    candidate = joinpath(root_path, relative)
    islink(candidate) && error("pinned directory is a symbolic link: $relative")
    isdir(candidate) || error("missing directory: $relative")
    resolved = realpath(candidate)
    startswith(resolved, root_path * Base.Filesystem.path_separator) ||
        error("directory escapes its source root: $relative")
    return resolved
end

function with_private_parquet_jl_depot(f, descriptor)
    depots = parquet_jl_source_depots()
    return mktempdir() do directory
        depot = joinpath(directory, "depot")
        mkdir(depot)
        depot = realpath(depot)
        for dependency in descriptor["dependency"]
            source = locate_parquet_jl_dependency(dependency, depots)
            destination = joinpath(depot, "packages", dependency["name"],
                dependency["depot_slug"])
            copy_parquet_jl_tree(source, destination, dependency)
        end
        for artifact in descriptor["artifact"]
            source = locate_parquet_jl_artifact(artifact, depots)
            destination = joinpath(depot, "artifacts",
                artifact["git_tree_sha1"])
            copy_parquet_jl_tree(source, destination, artifact)
        end
        validate_private_parquet_jl_depot(depot, descriptor)
        set_parquet_jl_tree_locked!(depot, true)
        try
            result = f(depot)
            validate_private_parquet_jl_depot(depot, descriptor)
            return result
        finally
            set_parquet_jl_tree_locked!(depot, false)
        end
    end
end

function parquet_jl_runtime_identity(root::AbstractString, descriptor)
    inventory = validate_bounded_tree(root;
        max_entries=Int(descriptor["julia_runtime_entry_count"]),
        max_file_bytes=min(Int(descriptor["julia_runtime_total_bytes"]),
            1024 * 1024 * 1024),
        max_total_bytes=Int(descriptor["julia_runtime_total_bytes"]))
    require_gate(inventory.entries == descriptor["julia_runtime_entry_count"],
        "Julia runtime entry count differs")
    require_gate(inventory.bytes == descriptor["julia_runtime_total_bytes"],
        "Julia runtime byte count differs")
    require_gate(inventory.sha256 == descriptor["julia_runtime_tree_sha256"],
        "Julia runtime tree hash differs")
    executable = checked_file(root, "bin/julia")
    require_gate(file_sha256(executable) == descriptor["julia_executable_sha256"],
        "Parquet.jl gate Julia executable differs")
    return inventory
end

function with_private_parquet_jl_runtime_source(f, source::AbstractString,
        descriptor)
    source = realpath(source)
    before = parquet_jl_runtime_identity(source, descriptor)
    return mktempdir() do directory
        runtime = joinpath(directory, "runtime")
        cp(source, runtime; follow_symlinks=false)
        runtime = realpath(runtime)
        require_gate(parquet_jl_runtime_identity(source, descriptor) == before,
            "source Julia runtime changed while copied")
        require_gate(parquet_jl_runtime_identity(runtime, descriptor) == before,
            "private Julia runtime copy differs")
        set_parquet_jl_tree_locked!(runtime, true)
        try
            result = f(runtime)
            require_gate(parquet_jl_runtime_identity(runtime, descriptor) == before,
                "private Julia runtime changed while executed")
            require_gate(parquet_jl_runtime_identity(source, descriptor) == before,
                "source Julia runtime changed while gate executed")
            return result
        finally
            set_parquet_jl_tree_locked!(runtime, false)
        end
    end
end

function with_private_parquet_jl_runtime(f, descriptor)
    source = realpath(normpath(joinpath(Sys.BINDIR, "..")))
    return with_private_parquet_jl_runtime_source(f, source, descriptor)
end

function parquet_jl_child_command(gate_root, depot, runtime, descriptor,
        descriptor_sha256::String)
    executable = checked_file(runtime, "bin/julia")
    arguments = String[
        executable,
        "--startup-file=no",
        "--history-file=no",
        "--compiled-modules=no",
        "--pkgimages=no",
        "--threads=1",
        "--project=$gate_root",
        gate_file(gate_root, descriptor["bootstrap_file"]),
    ]
    command = Cmd(Cmd(arguments); dir=gate_root)
    return isolated_command(command,
        "JULIA_DEPOT_PATH" => depot,
        "JULIA_LOAD_PATH" => "@:@stdlib",
        "JULIA_NUM_THREADS" => "1",
        "JULIA_PKG_OFFLINE" => "true",
        "JULIA_PKG_PRECOMPILE_AUTO" => "0",
        "JULIA_PKG_SERVER" => "",
        "PARQUET_N6_PRODUCER_DESCRIPTOR_SHA256" => descriptor_sha256,
        "OPENBLAS_NUM_THREADS" => "1")
end

function with_verified_parquet_jl_runtime(f, manifest, capabilities, snapshots,
        gate_root)
    descriptor = validate_parquet_jl_descriptor(manifest, capabilities, snapshots)
    require_gate(VERSION == v"1.12.6",
        "Parquet.jl evidence gate requires Julia 1.12.6")
    require_gate(!ispath(joinpath(gate_root, "LocalPreferences.toml")),
        "Parquet.jl gate has LocalPreferences.toml")
    with_private_parquet_jl_runtime(descriptor) do runtime
        with_private_parquet_jl_depot(descriptor) do depot
            descriptor_sha256 = control_snapshot(snapshots,
                manifest["parquet_jl_producer_descriptor_file"]).sha256
            run(parquet_jl_child_command(gate_root, depot, runtime, descriptor,
                descriptor_sha256))
            return f(runtime)
        end
    end
end

function validate_parquet_jl_gate(manifest, capabilities, snapshots, gate_root)
    with_verified_parquet_jl_runtime(manifest, capabilities, snapshots,
            gate_root) do _
        return
    end
    return
end

function test_parquet_jl_identity_helpers()
    mktempdir() do root
        mkpath(joinpath(root, "src", "nested"))
        project = joinpath(root, "Project.toml")
        source = joinpath(root, "src", "Parquet.jl")
        nested = joinpath(root, "src", "nested", "value.jl")
        write(project, "name = \"Parquet\"\n")
        write(source, "module Parquet\nend\n")
        write(nested, "const VALUE = 1\n")
        expected = ["Project.toml", "src/Parquet.jl", "src/nested/value.jl"]
        @test parquet_jl_source_paths(root) == expected
        snapshots = Dict{String,FileSnapshot}()
        entries = Dict{String,Any}[]
        for relative in expected
            path = joinpath(root, split(relative, '/')...)
            payload = read(path)
            digest = bytes2hex(SHA.sha256(payload))
            snapshots[relative] = FileSnapshot(path, payload, digest)
            push!(entries, Dict{String,Any}(
                "path" => relative, "sha256" => digest))
        end
        baseline = parquet_jl_source_composite(entries, snapshots)
        @test occursin(SHA256_PATTERN, baseline)
        changed = copy(snapshots)
        payload = UInt8[codeunits("module Parquet\nconst CHANGED = true\nend\n")...]
        changed["src/Parquet.jl"] = FileSnapshot(source, payload,
            bytes2hex(SHA.sha256(payload)))
        @test_throws ErrorException parquet_jl_source_composite(entries, changed)
        extra = joinpath(root, "src", "extra.jl")
        write(extra, "const EXTRA = true\n")
        @test parquet_jl_source_paths(root) != expected
        rm(extra)
        rm(nested)
        @test parquet_jl_source_paths(root) != expected
        write(nested, "const VALUE = 1\n")
        rm(nested)
        symlink(source, nested)
        @test_throws ErrorException parquet_jl_source_paths(root)
        return
    end
    mktempdir() do root
        file = joinpath(root, "value")
        write(file, "exact\n")
        inventory = validate_bounded_tree(root; max_entries=1,
            max_file_bytes=6, max_total_bytes=6)
        item = Dict{String,Any}(
            "entry_count" => Int64(1),
            "total_bytes" => Int64(6),
            "tree_sha256" => inventory.sha256,
            "git_tree_sha1" => bytes2hex(Pkg.GitTools.tree_hash(root)),
        )
        @test parquet_jl_tree_identity(root, item) == inventory
        write(file, "other\n")
        @test_throws ErrorException parquet_jl_tree_identity(root, item)
        write(file, "exact\n")
        write(joinpath(root, "extra"), "x")
        @test_throws ErrorException parquet_jl_tree_identity(root, item)
        return
    end
    mktempdir() do root
        mkpath(joinpath(root, "bin"))
        mkpath(joinpath(root, "lib"))
        executable = joinpath(root, "bin", "julia")
        write(executable, "runtime\n")
        chmod(executable, 0o755)
        write(joinpath(root, "lib", "payload"), "library\n")
        inventory = validate_bounded_tree(root; max_entries=4,
            max_file_bytes=16, max_total_bytes=16)
        descriptor = Dict{String,Any}(
            "julia_runtime_entry_count" => inventory.entries,
            "julia_runtime_total_bytes" => inventory.bytes,
            "julia_runtime_tree_sha256" => inventory.sha256,
            "julia_executable_sha256" => file_sha256(executable),
        )
        @test parquet_jl_runtime_identity(root, descriptor) == inventory
        with_private_parquet_jl_runtime_source(root, descriptor) do runtime
            @test runtime != realpath(root)
            @test parquet_jl_runtime_identity(runtime, descriptor) == inventory
            @test stat(runtime).mode & 0o222 == 0
            @test stat(joinpath(runtime, "bin", "julia")).mode & 0o111 != 0
            return
        end
        wrong = copy(descriptor)
        wrong["julia_runtime_entry_count"] += 1
        @test_throws ErrorException parquet_jl_runtime_identity(root, wrong)
        return
    end
    return
end
