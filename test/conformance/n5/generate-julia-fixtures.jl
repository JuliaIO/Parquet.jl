#!/usr/bin/env julia

using Parquet
using SHA
using TOML

include(joinpath(@__DIR__, "model", "N5ConformanceModel.jl"))

const N5 = N5ConformanceModel
const N5_PAGE_VERSIONS = (:v1, :v2)

struct N5FixtureMapping
    kind::String
    caseid::String
    pageversion::Symbol
    codec::Symbol
    reference::String
    target::String
    referencesha256::String
    targetsha256::String
end

function n5usage()
    println(stderr, "usage: generate-julia-fixtures.jl --repo REPO --output DIR")
    return exit(64)
end

function n5arguments(arguments::Vector{String})
    values = Dict{String,String}()
    index = 1
    while index <= length(arguments)
        index == length(arguments) && n5usage()
        key = arguments[index]
        key in ("--repo", "--output") || n5usage()
        haskey(values, key) && n5usage()
        values[key] = arguments[index + 1]
        index += 2
    end
    Set(keys(values)) == Set(("--repo", "--output")) || n5usage()
    return abspath(values["--repo"]), abspath(values["--output"])
end

function n5relativepath(path::String, label::String)
    isempty(path) && throw(ArgumentError("$label must not be empty"))
    isabspath(path) && throw(ArgumentError("$label must be relative"))
    occursin('\\', path) && throw(ArgumentError(
        "$label must use forward slashes"))
    parts = split(path, '/')
    any(part -> isempty(part) || part in (".", ".."), parts) &&
        throw(ArgumentError("$label is not normalized"))
    normpath(path) == path || throw(ArgumentError(
        "$label is not normalized"))
    return path
end

function n5sha256(bytes::AbstractVector{UInt8})
    return bytes2hex(SHA.sha256(bytes))
end

function n5sha256(path::String)
    return open(path, "r") do input
        return bytes2hex(SHA.sha256(input))
    end
end

function n5writebytes(root::String, relative::String,
        bytes::AbstractVector{UInt8}, seen::Set{String})
    n5relativepath(relative, "generated fixture path")
    relative in seen && throw(ArgumentError(
        "duplicate generated fixture path $relative"))
    push!(seen, relative)
    path = joinpath(root, split(relative, '/')...)
    mkpath(dirname(path))
    open(path, "w") do output
        write(output, bytes)
        return nothing
    end
    return n5sha256(bytes)
end

function n5mapping(kind::String, caseid::String, pageversion::Symbol,
        codec::Symbol, reference::String, target::String,
        referencesha256::String, targetsha256::String)
    pageversion in N5_PAGE_VERSIONS || throw(ArgumentError(
        "unsupported mapping page version $pageversion"))
    n5relativepath(reference, "reference path")
    n5relativepath(target, "target path")
    return N5FixtureMapping(kind, caseid, pageversion, codec, reference,
        target, referencesha256, targetsha256)
end

function n5rewrite(source::Vector{UInt8}, pageversion::Symbol;
        codec::Symbol=:uncompressed)
    table = Parquet.Table(source)
    try
        output = N5.n5productionencodedbytes(table, pageversion; codec=codec)
        output.privatefirst == output.privatesecond == output.publicfirst ==
            output.publicsecond || throw(ArgumentError(
            "production output is not byte deterministic"))
        return output.publicfirst
    finally
        close(table)
    end
end

function n5checkmodel(case, source::Vector{UInt8}, target::Vector{UInt8},
        pageversion::Symbol)
    sourcedecoded = N5.n5decodefile(source)
    targetdecoded = N5.n5decodefile(target)
    N5.n5schemaexact(sourcedecoded.metadata.schema,
        targetdecoded.metadata.schema) || throw(ArgumentError(
        "$(case.name) schema differs after production rewrite"))
    sourcedecoded.metadata.num_rows == targetdecoded.metadata.num_rows ==
        length(case.rows) || throw(ArgumentError(
        "$(case.name) row count differs after production rewrite"))
    sourcedecoded.streams == targetdecoded.streams || throw(ArgumentError(
        "$(case.name) streams differ after production rewrite"))
    all(==(pageversion), targetdecoded.pageversions) || throw(ArgumentError(
        "$(case.name) production page version differs"))
    return nothing
end

function n5checkproperty(case, fixture)
    table = Parquet.Table(fixture.bytes)
    try
        table.rows == length(case.rows) || throw(ArgumentError(
            "property case $(case.id) row count differs"))
        N5.n5schemaexact(table.metadata.schema, fixture.schema) ||
            throw(ArgumentError("property case $(case.id) schema differs"))
        isequal(N5.n5normalizetable(case.node, table), case.rows) ||
            throw(ArgumentError("property case $(case.id) rows differ"))
    finally
        close(table)
    end
    return nothing
end

function n5modelbindings!(output::String, mappings::Vector{N5FixtureMapping},
        seen::Set{String})
    cases = Any[N5.n5bindinggoldens()...]
    push!(cases, N5.n5provenancegolden())
    for case in cases
        kind = case.name == "schema-provenance" ? "provenance" : "binding"
        for pageversion in N5_PAGE_VERSIONS
            reference = "reference/model/$(case.name).$(pageversion).parquet"
            target = "julia/model/$(case.name).$(pageversion).parquet"
            source = N5.n5emitfile(case.schema, case.streams,
                length(case.rows); pageversion=pageversion)
            rewritten = n5rewrite(source, pageversion)
            n5checkmodel(case, source, rewritten, pageversion)
            referencesha = n5writebytes(output, reference, source, seen)
            targetsha = n5writebytes(output, target, rewritten, seen)
            push!(mappings, n5mapping(kind, case.name, pageversion,
                :uncompressed, reference, target, referencesha, targetsha))
        end
    end
    return nothing
end

function n5properties!(output::String, mappings::Vector{N5FixtureMapping},
        seen::Set{String})
    cases, _ = N5.n5propertycases()
    subset = N5.n5propertycodecsubset(cases)
    fixtures = N5.n5propertycodecfixtures(cases)
    byid = Dict(case.id => case for case in subset)
    references = Dict{Int,Tuple{String,String}}()
    for case in subset
        reference = "reference/property/$(case.name)-$(case.pageversion).parquet"
        source = N5.n5emitfile(case.schema, case.streams, length(case.rows);
            pageversion=case.pageversion)
        references[case.id] = (reference,
            n5writebytes(output, reference, source, seen))
    end
    for fixture in fixtures
        case = get(byid, fixture.caseid, nothing)
        case === nothing && throw(ArgumentError(
            "unknown property codec case $(fixture.caseid)"))
        fixture.pageversion == case.pageversion || throw(ArgumentError(
            "property codec page version differs for $(fixture.caseid)"))
        fixture.sha256 == n5sha256(fixture.bytes) || throw(ArgumentError(
            "property codec SHA-256 differs for $(fixture.filename)"))
        target = "julia/property/$(fixture.filename)"
        targetsha = n5writebytes(output, target, fixture.bytes, seen)
        reference, referencesha = references[fixture.caseid]
        n5checkproperty(case, fixture)
        push!(mappings, n5mapping("property", string(fixture.caseid),
            fixture.pageversion, fixture.codec, reference, target,
            referencesha, targetsha))
    end
    return nothing
end

function n5normalizeread(value)
    value === missing && return missing
    if value isa Parquet.StructValue
        return (:struct, Pair{String,Any}[String(pair.first) =>
            n5normalizeread(pair.second) for pair in value])
    elseif value isa Parquet.MapValue
        return (:map, Pair{Any,Any}[n5normalizeread(pair.first) =>
            n5normalizeread(pair.second) for pair in value])
    elseif value isa Parquet.ListValue
        return (:list, Any[n5normalizeread(item) for item in value])
    elseif value isa AbstractVector{UInt8}
        return (:bytes, bytes2hex(value))
    elseif value isa AbstractVector
        throw(ArgumentError("unexpected materialized vector $(typeof(value))"))
    end
    return value
end

function n5normalizetable(table::Parquet.Table)
    columns = collect(pairs(table.columns))
    output = Vector{Vector{Pair{String,Any}}}(undef, table.rows)
    for row in 1:table.rows
        values = Pair{String,Any}[]
        sizehint!(values, length(columns))
        for column in columns
            push!(values, String(column.first) =>
                n5normalizeread(column.second[row]))
        end
        output[row] = values
    end
    return output
end

function n5external!(repo::String, output::String,
        mappings::Vector{N5FixtureMapping}, seen::Set{String})
    root = joinpath(repo, "test", "conformance", "n5")
    manifest = TOML.parsefile(joinpath(root, "manifest.toml"))
    rawcases = get(manifest, "case", nothing)
    rawcases isa AbstractVector || throw(ArgumentError(
        "external manifest case list is absent"))
    fixtures = NamedTuple[]
    for rawcase in rawcases
        producer = String(rawcase["producer"])
        caseid = String(rawcase["case_id"])
        files = rawcase["files"]
        files isa AbstractVector || throw(ArgumentError(
            "external case $caseid has no files"))
        for rawfile in files
            pageversion = Symbol(String(rawfile["page_version"]))
            sourcepath = String(rawfile["file"])
            n5relativepath(sourcepath, "external fixture path")
            push!(fixtures, (; producer, caseid, pageversion, sourcepath))
        end
    end
    sort!(fixtures; by=fixture -> (fixture.producer, fixture.caseid,
        fixture.pageversion))
    for fixture in fixtures
        input = read(joinpath(root, split(fixture.sourcepath, '/')...))
        reference = "reference/external/$(fixture.producer)/" *
            basename(fixture.sourcepath)
        target = "julia/rewrite/$(fixture.producer)/" *
            basename(fixture.sourcepath)
        rewritten = n5rewrite(input, fixture.pageversion)
        sourcetable = Parquet.Table(input)
        targettable = Parquet.Table(rewritten)
        try
            N5.n5schemaexact(sourcetable.metadata.schema,
                targettable.metadata.schema) || throw(ArgumentError(
                "external $(fixture.caseid) schema differs"))
            sourcetable.rows == targettable.rows || throw(ArgumentError(
                "external $(fixture.caseid) rows differ"))
            isequal(n5normalizetable(sourcetable),
                n5normalizetable(targettable)) || throw(ArgumentError(
                "external $(fixture.caseid) values differ"))
        finally
            close(targettable)
            close(sourcetable)
        end
        result = N5.n5decodefile(rewritten)
        all(==(fixture.pageversion), result.pageversions) || throw(
            ArgumentError("external $(fixture.caseid) page version differs"))
        referencesha = n5writebytes(output, reference, input, seen)
        targetsha = n5writebytes(output, target, rewritten, seen)
        push!(mappings, n5mapping("external", fixture.caseid,
            fixture.pageversion, :uncompressed, reference, target,
            referencesha, targetsha))
    end
    return nothing
end

function n5writemappingmanifest(output::String,
        mappings::Vector{N5FixtureMapping}, seen::Set{String})
    sort!(mappings; by=mapping -> (mapping.target, mapping.reference))
    length(Set(mapping.target for mapping in mappings)) == length(mappings) ||
        throw(ArgumentError("generated target paths are not unique"))
    path = "fixture-manifest.tsv"
    path in seen && throw(ArgumentError("duplicate generated manifest path"))
    push!(seen, path)
    open(joinpath(output, path), "w") do io
        println(io, join(("kind", "case_id", "page_version", "codec",
            "reference", "target", "reference_sha256", "target_sha256"),
            '\t'))
        for mapping in mappings
            fields = (mapping.kind, mapping.caseid,
                String(mapping.pageversion), String(mapping.codec),
                mapping.reference, mapping.target, mapping.referencesha256,
                mapping.targetsha256)
            any(field -> occursin('\t', field) || occursin('\n', field),
                fields) && throw(ArgumentError(
                "generated manifest field contains a delimiter"))
            println(io, join(fields, '\t'))
        end
        return nothing
    end
    return nothing
end

function n5walkfiles(root::String)
    files = String[]
    for (directory, _, names) in walkdir(root)
        for name in names
            relative = replace(relpath(joinpath(directory, name), root),
                '\\' => '/')
            push!(files, relative)
        end
    end
    sort!(files)
    return files
end

function n5writefilemanifest(output::String, seen::Set{String})
    actual = n5walkfiles(output)
    sort!(collect(seen)) == actual || throw(ArgumentError(
        "generated file set differs before the file manifest"))
    open(joinpath(output, "files.sha256"), "w") do io
        for relative in actual
            digest = n5sha256(joinpath(output, split(relative, '/')...))
            println(io, "$digest  $relative")
        end
        return nothing
    end
    return nothing
end

function main(arguments::Vector{String})
    repo, output = n5arguments(arguments)
    isfile(joinpath(repo, "Project.toml")) || throw(ArgumentError(
        "--repo does not name the Parquet repository"))
    if ispath(output)
        isdir(output) || throw(ArgumentError("--output is not a directory"))
        isempty(readdir(output)) || throw(ArgumentError(
            "--output must be empty"))
    else
        mkpath(output)
    end
    mappings = N5FixtureMapping[]
    seen = Set{String}()
    n5modelbindings!(output, mappings, seen)
    n5properties!(output, mappings, seen)
    n5external!(repo, output, mappings, seen)
    length(mappings) == 256 || throw(ArgumentError(
        "expected 256 Julia fixture mappings, got $(length(mappings))"))
    n5writemappingmanifest(output, mappings, seen)
    n5writefilemanifest(output, seen)
    println("generated $(length(mappings)) N5 Julia fixture mappings")
    return nothing
end

main(ARGS)
