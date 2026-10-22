#!/usr/bin/env julia

using Parquet
using SHA

include(joinpath(@__DIR__, "model", "N5ConformanceModel.jl"))

const N5 = N5ConformanceModel

function n5usage()
    println(stderr, "usage: generate-codec-candidates.jl --output DIR " *
        "[--codec-ids ID,ID,...]")
    return exit(64)
end

function n5arguments(arguments::Vector{String})
    values = Dict{String,String}()
    index = 1
    while index <= length(arguments)
        index == length(arguments) && n5usage()
        key = arguments[index]
        key in ("--output", "--codec-ids") || n5usage()
        haskey(values, key) && n5usage()
        values[key] = arguments[index + 1]
        index += 2
    end
    haskey(values, "--output") || n5usage()
    ids = if haskey(values, "--codec-ids")
        parsed = parse.(Int, split(values["--codec-ids"], ','))
        length(parsed) == 32 || throw(ArgumentError(
            "--codec-ids must contain exactly 32 IDs"))
        length(Set(parsed)) == 32 || throw(ArgumentError(
            "--codec-ids contains a duplicate"))
        sort(parsed) == parsed || throw(ArgumentError(
            "--codec-ids must be strictly increasing"))
        parsed
    else
        nothing
    end
    return abspath(values["--output"]), ids
end

function n5write(path::String, bytes::Vector{UInt8})
    open(path, "w") do output
        write(output, bytes)
        return nothing
    end
    return bytes2hex(SHA.sha256(bytes))
end

function n5writecandidates(output::String, cases)
    manifest = joinpath(output, "cases.tsv")
    open(manifest, "w") do io
        println(io, join(("id", "page_version", "rows", "payload_bytes",
            "depth", "width", "ast_nodes", "leaves", "level_entries",
            "dense_values", "sha256", "file"), '\t'))
        for case in cases
            length(case.rows) > 0 && case.payloadbytes > 0 || continue
            file = "case-$(lpad(case.id, 4, '0')).parquet"
            bytes = N5.n5emitfile(case.schema, case.streams,
                length(case.rows); pageversion=case.pageversion)
            digest = n5write(joinpath(output, file), bytes)
            println(io, join((case.id, case.pageversion, length(case.rows),
                case.payloadbytes, case.depth, case.width, case.astnodes,
                case.leaves, case.levelentries, case.densevalues, digest,
                file), '\t'))
        end
        return nothing
    end
    println("generated $(count(line -> endswith(line, ".parquet"),
        readdir(output))) codec candidate schemas")
    return nothing
end

function n5writecodecs(output::String, cases, ids::Vector{Int})
    byid = Dict(case.id => case for case in cases)
    manifest = joinpath(output, "codecs.tsv")
    count = 0
    open(manifest, "w") do io
        println(io, join(("id", "page_version", "codec", "sha256",
            "file"), '\t'))
        for id in ids
            case = get(byid, id, nothing)
            case === nothing && throw(ArgumentError(
                "codec candidate ID $id is not accepted"))
            length(case.rows) > 0 && case.payloadbytes > 0 ||
                throw(ArgumentError("codec candidate ID $id has no payload"))
            for codec in N5.N5_PROPERTY_CODECS
                fixture = N5.n5propertycodecfixture(case, codec)
                digest = n5write(joinpath(output, fixture.filename),
                    fixture.bytes)
                digest == fixture.sha256 || throw(ArgumentError(
                    "codec fixture $(fixture.filename) SHA-256 differs"))
                println(io, join((id, case.pageversion, codec, digest,
                    fixture.filename), '\t'))
                count += 1
            end
        end
        return nothing
    end
    count == 192 || throw(ArgumentError(
        "expected 192 codec fixtures, got $count"))
    println("generated $count codec candidate fixtures")
    return nothing
end

function main(arguments::Vector{String})
    output, ids = n5arguments(arguments)
    if ispath(output)
        isdir(output) || throw(ArgumentError("--output is not a directory"))
        isempty(readdir(output)) || throw(ArgumentError(
            "--output must be empty"))
    else
        mkpath(output)
    end
    cases, _ = N5.n5propertycases()
    ids === nothing ? n5writecandidates(output, cases) :
        n5writecodecs(output, cases, ids)
    return nothing
end

main(ARGS)
