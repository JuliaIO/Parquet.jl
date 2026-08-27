using SHA
using TOML
using Test

const N5_EXTERNAL_ROOT = @__DIR__

const N5_EXTERNAL_PRODUCER_PINS = Dict(
    "parquet-java" => (
        version="1.17.1",
        commit="78a8d3230eb4769db93de5f2f2e18363c04cae81",
    ),
    "arrow-rs" => (
        version="59.2.0",
        commit="782e5a685501a9db6cc8e9a3b7cbff894940c47a",
    ),
)

struct N5ExternalEvidence
    producer::String
    producer_version::String
    producer_commit::String
    scope::String
    file::String
    sha256::String
    fixture_count::Int
end

struct N5ExternalFixture
    producer::String
    producer_version::String
    producer_commit::String
    case_id::String
    role::String
    page_version::Symbol
    file::String
    sha256::String
    rows::Int
    evidence_file::String
    evidence_sha256::String
end

struct N5ExternalManifest
    evidence::Vector{N5ExternalEvidence}
    fixtures::Vector{N5ExternalFixture}
end

function n5externalrequirekeys(value, required::Vector{String}, label::String)
    value isa AbstractDict || throw(ArgumentError("$label must be a TOML table"))
    actual = Set(String(key) for key in keys(value))
    expected = Set(required)
    actual == expected && return value
    missing = sort!(collect(setdiff(expected, actual)))
    extra = sort!(collect(setdiff(actual, expected)))
    throw(ArgumentError("$label keys differ: missing=$missing extra=$extra"))
end

function n5externalstring(value, label::String)
    value isa String || throw(ArgumentError("$label must be a string"))
    isempty(value) && throw(ArgumentError("$label must not be empty"))
    return value
end

function n5externalinteger(value, label::String)
    value isa Integer || throw(ArgumentError("$label must be an integer"))
    0 <= value <= typemax(Int) || throw(ArgumentError("$label is outside the Int range"))
    return Int(value)
end

function n5externalsha256(value, label::String)
    digest = n5externalstring(value, label)
    occursin(r"^[0-9a-f]{64}$", digest) || throw(ArgumentError(
        "$label must be a lowercase SHA-256 digest"))
    return digest
end

function n5externalgitcommit(value, label::String)
    commit = n5externalstring(value, label)
    occursin(r"^[0-9a-f]{40}$", commit) || throw(ArgumentError(
        "$label must be a lowercase 40-hex Git commit"))
    return commit
end

function n5externalrelativepath(value, label::String)
    path = n5externalstring(value, label)
    isabspath(path) && throw(ArgumentError("$label must be relative"))
    occursin('\\', path) && throw(ArgumentError("$label must use forward slashes"))
    parts = split(path, '/')
    any(part -> isempty(part) || part in (".", ".."), parts) &&
        throw(ArgumentError("$label is not a normalized relative path"))
    normpath(path) == path || throw(ArgumentError(
        "$label is not a normalized relative path"))
    return path
end

function n5externalevidence(value, index::Int)
    label = "expected_evidence[$index]"
    table = n5externalrequirekeys(value, [
        "producer",
        "producer_version",
        "producer_commit",
        "scope",
        "file",
        "sha256",
        "fixture_count",
    ], label)
    producer = n5externalstring(table["producer"], "$label.producer")
    pin = get(N5_EXTERNAL_PRODUCER_PINS, producer, nothing)
    pin === nothing && throw(ArgumentError("$label has unknown producer $producer"))
    version = n5externalstring(table["producer_version"],
        "$label.producer_version")
    commit = n5externalgitcommit(table["producer_commit"],
        "$label.producer_commit")
    version == pin.version || throw(ArgumentError(
        "$label producer version differs from the checked pin"))
    commit == pin.commit || throw(ArgumentError(
        "$label producer commit differs from the checked pin"))
    scope = n5externalstring(table["scope"], "$label.scope")
    scope in ("owned", "near-neighbor") || throw(ArgumentError(
        "$label has unsupported scope $scope"))
    file = n5externalrelativepath(table["file"], "$label.file")
    startswith(file, "expected/") || throw(ArgumentError(
        "$label file must be below expected/"))
    digest = n5externalsha256(table["sha256"], "$label.sha256")
    count = n5externalinteger(table["fixture_count"], "$label.fixture_count")
    count > 0 || throw(ArgumentError("$label.fixture_count must be positive"))
    return N5ExternalEvidence(producer, version, commit, scope, file,
        digest, count)
end

function n5externalcase(value, index::Int)
    label = "case[$index]"
    table = n5externalrequirekeys(value, [
        "producer",
        "producer_version",
        "producer_commit",
        "case_id",
        "role",
        "rows",
        "expected_evidence_file",
        "expected_evidence_sha256",
        "files",
    ], label)
    producer = n5externalstring(table["producer"], "$label.producer")
    pin = get(N5_EXTERNAL_PRODUCER_PINS, producer, nothing)
    pin === nothing && throw(ArgumentError("$label has unknown producer $producer"))
    version = n5externalstring(table["producer_version"],
        "$label.producer_version")
    commit = n5externalgitcommit(table["producer_commit"],
        "$label.producer_commit")
    version == pin.version || throw(ArgumentError(
        "$label producer version differs from the checked pin"))
    commit == pin.commit || throw(ArgumentError(
        "$label producer commit differs from the checked pin"))
    case_id = n5externalstring(table["case_id"], "$label.case_id")
    role = n5externalstring(table["role"], "$label.role")
    role in ("binding", "compatibility", "diagnostic") ||
        throw(ArgumentError("$label has unsupported role $role"))
    rows = n5externalinteger(table["rows"], "$label.rows")
    evidence_file = n5externalrelativepath(table["expected_evidence_file"],
        "$label.expected_evidence_file")
    evidence_sha256 = n5externalsha256(
        table["expected_evidence_sha256"],
        "$label.expected_evidence_sha256")
    rawfiles = table["files"]
    rawfiles isa AbstractVector || throw(ArgumentError(
        "$label.files must be an array of inline tables"))
    length(rawfiles) == 2 || throw(ArgumentError(
        "$label.files must contain exactly V1 and V2"))
    fixtures = N5ExternalFixture[]
    sizehint!(fixtures, 2)
    pages = Set{Symbol}()
    for (fileindex, rawfile) in enumerate(rawfiles)
        filelabel = "$label.files[$fileindex]"
        filetable = n5externalrequirekeys(rawfile,
            ["page_version", "file", "sha256"], filelabel)
        pagestring = n5externalstring(filetable["page_version"],
            "$filelabel.page_version")
        pagestring in ("v1", "v2") || throw(ArgumentError(
            "$filelabel has unsupported page version $pagestring"))
        page = Symbol(pagestring)
        page in pages && throw(ArgumentError(
            "$label contains duplicate page version $page"))
        push!(pages, page)
        file = n5externalrelativepath(filetable["file"], "$filelabel.file")
        startswith(file, "golden/$producer/") || throw(ArgumentError(
            "$filelabel is outside the producer fixture directory"))
        endswith(file, ".parquet") || throw(ArgumentError(
            "$filelabel is not a Parquet file"))
        digest = n5externalsha256(filetable["sha256"], "$filelabel.sha256")
        push!(fixtures, N5ExternalFixture(producer, version, commit,
            case_id, role, page, file, digest, rows, evidence_file,
            evidence_sha256))
    end
    pages == Set((:v1, :v2)) || throw(ArgumentError(
        "$label does not bind both page versions"))
    return fixtures
end

function n5externalloadmanifest(path::String)
    raw = TOML.parsefile(path)
    n5externalrequirekeys(raw,
        ["manifest_version", "expected_evidence", "case"], "manifest")
    n5externalinteger(raw["manifest_version"], "manifest_version") == 1 ||
        throw(ArgumentError("unsupported N5 external manifest version"))
    rawevidence = raw["expected_evidence"]
    rawevidence isa AbstractVector || throw(ArgumentError(
        "expected_evidence must be an array of tables"))
    evidence = N5ExternalEvidence[
        n5externalevidence(value, index)
        for (index, value) in enumerate(rawevidence)
    ]
    length(unique(item.file for item in evidence)) == length(evidence) ||
        throw(ArgumentError("expected evidence files are duplicated"))
    rawcases = raw["case"]
    rawcases isa AbstractVector || throw(ArgumentError(
        "case must be an array of tables"))
    fixtures = N5ExternalFixture[]
    for (index, value) in enumerate(rawcases)
        append!(fixtures, n5externalcase(value, index))
    end
    identities = [(fixture.producer, fixture.case_id,
        fixture.page_version) for fixture in fixtures]
    length(unique(identities)) == length(identities) || throw(ArgumentError(
        "external fixture producer/case/page identities are duplicated"))
    length(unique(fixture.file for fixture in fixtures)) == length(fixtures) ||
        throw(ArgumentError("external fixture files are duplicated"))
    for fixture in fixtures
        matches = [item for item in evidence if
            item.file == fixture.evidence_file &&
            item.sha256 == fixture.evidence_sha256]
        length(matches) == 1 || throw(ArgumentError(
            "$(fixture.producer)/$(fixture.case_id) has no unique " *
            "cryptographic evidence reference"))
        item = only(matches)
        item.producer == fixture.producer || throw(ArgumentError(
            "$(fixture.producer)/$(fixture.case_id) evidence producer differs"))
        item.producer_version == fixture.producer_version ||
            throw(ArgumentError("$(fixture.producer)/$(fixture.case_id) evidence version differs"))
        item.producer_commit == fixture.producer_commit ||
            throw(ArgumentError("$(fixture.producer)/$(fixture.case_id) evidence commit differs"))
    end
    for item in evidence
        boundcount = count(fixture ->
            fixture.evidence_file == item.file &&
            fixture.evidence_sha256 == item.sha256, fixtures)
        boundcount == item.fixture_count || throw(ArgumentError(
            "$(item.file) binds $boundcount fixtures, expected $(item.fixture_count)"))
    end
    return N5ExternalManifest(evidence, fixtures)
end

function n5externallist(values...)
    return (:list, Any[values...])
end

function n5externalstruct(values::Pair...)
    output = Pair{String,Any}[]
    sizehint!(output, length(values))
    for value in values
        push!(output, Pair{String,Any}(String(value.first), value.second))
    end
    return (:struct, output)
end

function n5externalmap(values::Pair...)
    output = Pair{Any,Any}[]
    sizehint!(output, length(values))
    for value in values
        push!(output, Pair{Any,Any}(value.first, value.second))
    end
    return (:map, output)
end

function n5externalrows(name::String, values...)
    return [Pair{String,Any}[Pair{String,Any}(name, value)] for value in values]
end

function n5externalexpectedrows()
    required = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(Int32(10)),
        n5externallist(Int32(20), Int32(30)))
    nesteditems = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(n5externallist()),
        n5externallist(n5externallist(Int32(1), Int32(2)),
            n5externallist(), n5externallist(Int32(3))))
    nestedvalues = n5externalrows("values",
        missing,
        n5externallist(),
        n5externallist(n5externallist()),
        n5externallist(n5externallist(Int32(1), Int32(2)),
            n5externallist(), n5externallist(Int32(3))))
    unannotateditems = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(n5externalstruct("element" => n5externallist())),
        n5externallist(
            n5externalstruct("element" => n5externallist(Int32(1), Int32(2))),
            n5externalstruct("element" => n5externallist()),
            n5externalstruct("element" => n5externallist(Int32(3)))))
    unannotatedvalues = n5externalrows("values",
        missing,
        n5externallist(),
        n5externallist(n5externalstruct("element" => n5externallist())),
        n5externallist(
            n5externalstruct("element" => n5externallist(Int32(1), Int32(2))),
            n5externalstruct("element" => n5externallist()),
            n5externalstruct("element" => n5externallist(Int32(3)))))
    standardmap = n5externalrows("map",
        missing,
        n5externalmap(),
        n5externalmap("a" => missing),
        n5externalmap("a" => Int32(1), "a" => Int32(2), "b" => Int32(3)),
        n5externalmap("c" => Int32(4)))
    output = Dict{String,Vector{Vector{Pair{String,Any}}}}()
    output["parquet-java/list_rule1_primitive"] = required
    output["parquet-java/list_rule5_required"] = required
    output["parquet-java/list_rule2_struct"] = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(n5externalstruct(
            "x" => Int32(1), "y" => missing)),
        n5externallist(
            n5externalstruct("x" => Int32(2), "y" => Int32(20)),
            n5externalstruct("x" => Int32(3), "y" => Int32(30))))
    output["parquet-java/list_rule3_nested"] = nesteditems
    output["parquet-java/list_rule3_unannotated_diagnostic"] =
        unannotateditems
    output["parquet-java/list_rule4_array"] = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(n5externalstruct("value" => missing)),
        n5externallist(n5externalstruct("value" => Int32(4)),
            n5externalstruct("value" => missing)))
    output["parquet-java/list_rule4_tuple"] = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(n5externalstruct("value" => Int32(7))),
        n5externallist(n5externalstruct("value" => missing),
            n5externalstruct("value" => Int32(8))))
    output["parquet-java/list_rule5_optional_paired"] = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(missing),
        n5externallist(Int32(4), missing))
    output["parquet-java/list_rule5_optional_extended"] = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(missing),
        n5externallist(Int32(5), missing, Int32(6)))
    output["parquet-java/list_direct_map"] = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(n5externalmap()),
        n5externallist(
            n5externalmap(Int32(1) => Int32(10), Int32(1) => Int32(20)),
            n5externalmap(), n5externalmap(Int32(2) => Int32(30))))
    output["parquet-java/list_direct_map_utf8"] = n5externalrows("items",
        missing,
        n5externallist(),
        n5externallist(n5externalmap()),
        n5externallist(
            n5externalmap("a" => Int32(10), "a" => Int32(20)),
            n5externalmap(), n5externalmap("b" => Int32(30))))
    output["parquet-java/map_standard"] = standardmap
    output["parquet-java/map_standalone_mkv"] = standardmap
    output["parquet-java/map_arbitrary_names"] = n5externalrows("bag",
        missing,
        n5externalmap(),
        n5externalmap("a" => missing),
        n5externalmap("a" => Int32(1), "a" => Int32(2), "b" => Int32(3)),
        n5externalmap("c" => Int32(4)))
    output["parquet-java/map_key_only"] = n5externalrows("map",
        n5externalmap(),
        n5externalmap("k1" => missing),
        n5externalmap("k2" => missing, "k2" => missing))
    output["arrow-rs/arrow-rs-duplicate-keys"] = n5externalrows("entries",
        missing,
        n5externalmap(),
        n5externalmap("a" => missing),
        n5externalmap("a" => Int32(1), "a" => Int32(2), "b" => Int32(3)),
        n5externalmap("c" => Int32(4)))
    output["arrow-rs/arrow-rs-optional-key-present"] = n5externalrows("entries",
        missing,
        n5externalmap(),
        n5externalmap("a" => Int32(1)),
        n5externalmap("b" => Int32(2), "c" => Int32(3)))
    output["arrow-rs/arrow-rs-list-rule3"] = nestedvalues
    output["arrow-rs/arrow-rs-list-rule3-unannotated-near-neighbor"] =
        unannotatedvalues
    return output
end

const N5_EXTERNAL_EXPECTED_ROWS = n5externalexpectedrows()

function n5externalnormalize(value)
    value === missing && return missing
    if value isa Parquet.StructValue
        output = Pair{String,Any}[]
        sizehint!(output, length(value))
        for pair in value
            push!(output, Pair{String,Any}(String(pair.first),
                n5externalnormalize(pair.second)))
        end
        return (:struct, output)
    elseif value isa Parquet.MapValue
        output = Pair{Any,Any}[]
        sizehint!(output, length(value))
        for pair in value
            push!(output, Pair{Any,Any}(n5externalnormalize(pair.first),
                n5externalnormalize(pair.second)))
        end
        return (:map, output)
    elseif value isa Parquet.ListValue
        return (:list, Any[n5externalnormalize(item) for item in value])
    elseif value isa AbstractVector{UInt8}
        return (:bytes, bytes2hex(value))
    elseif value isa AbstractVector
        throw(ArgumentError("unexpected materialized vector $(typeof(value))"))
    end
    return value
end

function n5externalnormalizerows(table::Parquet.Table)
    columns = collect(pairs(table.columns))
    output = Vector{Vector{Pair{String,Any}}}(undef, table.rows)
    for row in 1:table.rows
        values = Pair{String,Any}[]
        sizehint!(values, length(columns))
        for column in columns
            push!(values, Pair{String,Any}(String(column.first),
                n5externalnormalize(column.second[row])))
        end
        output[row] = values
    end
    return output
end

function n5externalpageversions(table::Parquet.Table)
    versions = Symbol[]
    for group in table.metadata.row_groups
        for column in group.columns
            metadata = column.meta_data
            metadata === nothing && throw(ArgumentError(
                "external column chunk has no metadata"))
            firstbyte, stop = Parquet._chunkrange(metadata,
                table.file.footer.offset)
            position = firstbyte
            while position < stop
                frame = Parquet.readpage(table.file.source, position, stop,
                    Parquet.Limits())
                kind = Parquet.pagekind(frame)
                if kind === :data_v1
                    push!(versions, :v1)
                elseif kind === :data_v2
                    push!(versions, :v2)
                else
                    throw(ArgumentError("external fixture has $kind page"))
                end
                position = Parquet.pageend(frame)
            end
            position == stop || throw(ArgumentError(
                "external page scan did not end at the column boundary"))
        end
    end
    isempty(versions) && throw(ArgumentError("external fixture has no data pages"))
    return versions
end

function n5externalfiles(root::String)
    output = String[]
    for (directory, _, files) in walkdir(root)
        for file in files
            path = relpath(joinpath(directory, file), N5_EXTERNAL_ROOT)
            push!(output, replace(path, '\\' => '/'))
        end
    end
    sort!(output)
    return output
end

function n5externalexactset(label::String, declared, actual)
    declaredset = Set(declared)
    actualset = Set(actual)
    declaredset == actualset && return
    missing = sort!(collect(setdiff(declaredset, actualset)))
    extra = sort!(collect(setdiff(actualset, declaredset)))
    throw(ArgumentError("$label differs: missing=$missing extra=$extra"))
end

function n5externalfilehash(path::String)
    return open(path, "r") do input
        return bytes2hex(SHA.sha256(input))
    end
end

function n5externalcheckhash(relative::String, expected::String)
    path = joinpath(N5_EXTERNAL_ROOT, split(relative, '/')...)
    isfile(path) || throw(ArgumentError("required N5 external file is absent: $relative"))
    actual = n5externalfilehash(path)
    actual == expected || throw(ArgumentError(
        "$relative SHA-256 differs: expected $expected, got $actual"))
    return path
end

function n5externalrewrite(table::Parquet.Table, pageversion::Symbol)
    return N5.n5productionencodedbytes(table, pageversion)
end

function n5externaltableerror(bytes::Vector{UInt8})
    table = try
        Parquet.Table(bytes)
    catch error
        return error
    end
    close(table)
    return nothing
end

const N5_EXTERNAL_MANIFEST_PATH = joinpath(N5_EXTERNAL_ROOT, "manifest.toml")
const N5_EXTERNAL_MANIFEST = n5externalloadmanifest(N5_EXTERNAL_MANIFEST_PATH)

@testset "N5 external fixture manifest" begin
    manifest = N5_EXTERNAL_MANIFEST
    @test length(manifest.evidence) == 3
    @test length(manifest.fixtures) == 38
    @test count(fixture -> fixture.producer == "parquet-java",
        manifest.fixtures) == 30
    @test count(fixture -> fixture.producer == "arrow-rs",
        manifest.fixtures) == 8
    manifestcases = Set("$(fixture.producer)/$(fixture.case_id)"
        for fixture in manifest.fixtures)
    n5externalexactset("external semantic case set",
        keys(N5_EXTERNAL_EXPECTED_ROWS), manifestcases)
    n5externalexactset("external fixture file set",
        (fixture.file for fixture in manifest.fixtures),
        n5externalfiles(joinpath(N5_EXTERNAL_ROOT, "golden")))
    n5externalexactset("external expected-evidence file set",
        (item.file for item in manifest.evidence),
        n5externalfiles(joinpath(N5_EXTERNAL_ROOT, "expected")))
    for item in manifest.evidence
        n5externalcheckhash(item.file, item.sha256)
    end
    for fixture in manifest.fixtures
        n5externalcheckhash(fixture.file, fixture.sha256)
    end
    @test_throws ArgumentError n5externalexactset("synthetic missing",
        ["a", "b"], ["a"])
    @test_throws ArgumentError n5externalexactset("synthetic extra",
        ["a"], ["a", "b"])
    with_extra = TOML.parsefile(N5_EXTERNAL_MANIFEST_PATH)
    with_extra["unexpected"] = true
    mktemp() do temporary, output
        TOML.print(output, with_extra)
        flush(output)
        @test_throws ArgumentError begin
            n5externalloadmanifest(temporary)
        end
    end
    with_missing = TOML.parsefile(N5_EXTERNAL_MANIFEST_PATH)
    delete!(first(with_missing["case"]), "rows")
    mktemp() do temporary, output
        TOML.print(output, with_missing)
        flush(output)
        @test_throws ArgumentError begin
            n5externalloadmanifest(temporary)
        end
    end
end

@testset "N5 Julia external fixture read and rewrite" begin
    for fixture in N5_EXTERNAL_MANIFEST.fixtures
        identity = "$(fixture.producer)/$(fixture.case_id)/$(fixture.page_version)"
        @testset "$identity" begin
            path = joinpath(N5_EXTERNAL_ROOT, split(fixture.file, '/')...)
            table = Parquet.Table(path)
            try
                expected = N5_EXTERNAL_EXPECTED_ROWS[
                    "$(fixture.producer)/$(fixture.case_id)"]
                @test table.rows == fixture.rows == length(expected)
                @test table.metadata.num_rows == fixture.rows
                @test all(==(fixture.page_version),
                    n5externalpageversions(table))
                actual = n5externalnormalizerows(table)
                @test isequal(actual, expected)
                rewrites = Dict{Symbol,Vector{UInt8}}()
                for pageversion in (:v1, :v2)
                    output = n5externalrewrite(table, pageversion)
                    @test output.privatefirst == output.privatesecond
                    @test output.publicfirst == output.publicsecond
                    @test output.privatefirst == output.publicfirst
                    rewrites[pageversion] = output.privatefirst
                    decoded = N5.n5decodefile(output.privatefirst)
                    @test decoded.metadata.num_rows == fixture.rows
                    @test N5.n5schemaexact(decoded.metadata.schema,
                        table.metadata.schema)
                    @test all(==(pageversion), decoded.pageversions)
                    rewritten = Parquet.Table(output.privatefirst)
                    try
                        @test rewritten.rows == fixture.rows
                        @test N5.n5schemaexact(rewritten.metadata.schema,
                            table.metadata.schema)
                        @test all(==(pageversion),
                            n5externalpageversions(rewritten))
                        @test isequal(n5externalnormalizerows(rewritten),
                            expected)
                    finally
                        close(rewritten)
                    end
                end
                @test rewrites[:v1] != rewrites[:v2]
            finally
                close(table)
            end
        end
    end
end

@testset "N5 external optional-key actual-null mutation" begin
    fixture = only(item for item in N5_EXTERNAL_MANIFEST.fixtures if
        item.producer == "arrow-rs" &&
        item.case_id == "arrow-rs-optional-key-present" &&
        item.page_version === :v1)
    source = read(joinpath(N5_EXTERNAL_ROOT, split(fixture.file, '/')...))
    decoded = N5.n5decodefile(source)
    @test decoded.metadata.num_rows == fixture.rows
    keyindex = findfirst(leaf -> leaf.path ==
        ["entries", "key_value", "key"], decoded.leaves)
    @test keyindex !== nothing
    if keyindex !== nothing
        key = decoded.streams[keyindex]
        @test key.max_repetition == 1
        @test key.max_definition == 3
        @test key.repetition == UInt64[0, 0, 0, 0, 1]
        @test key.definition == UInt64[0, 1, 3, 3, 3]
        @test key.values == Any["a", "b", "c"]
        definitions = copy(key.definition)
        definitions[3] = UInt64(2)
        streams = copy(decoded.streams)
        streams[keyindex] = N5.N5LeafStream(copy(key.repetition),
            definitions, Any[key.values[2:end]...], key.max_repetition,
            key.max_definition)
        for pageversion in (:v1, :v2)
            mutated = N5.n5emitfile(decoded.metadata.schema, streams,
                fixture.rows; pageversion=pageversion)
            wire = N5.n5decodefile(mutated)
            @test N5.n5schemaexact(wire.metadata.schema,
                decoded.metadata.schema)
            @test wire.streams[keyindex].definition[3] == UInt64(2)
            @test wire.streams[keyindex].values == Any["b", "c"]
            @test n5externaltableerror(mutated) isa Parquet.FormatError
        end
    end
end
