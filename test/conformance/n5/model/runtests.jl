using Parquet
using SHA
using Test

include(joinpath(@__DIR__, "N5ConformanceModel.jl"))

const N5 = N5ConformanceModel
const N5_PROPERTY_CASES, N5_PROPERTY_REJECTED = N5.n5propertycases()

function n5frozenwriterreference(expression)
    expression isa Expr && expression.head == :. || return nothing
    length(expression.args) == 2 || return nothing
    expression.args[1] == :Parquet || return nothing
    name = expression.args[2]
    name isa QuoteNode || return nothing
    name.value in (:_encodefile, :write) || return nothing
    return name.value
end

function n5frozenwriterusesoptions(expression::Expr)
    for argument in expression.args
        argument isa Expr && argument.head == :parameters || continue
        for keyword in argument.args
            keyword isa Expr && keyword.head == :... || continue
            length(keyword.args) == 1 && keyword.args[1] == :options &&
                return true
        end
    end
    return false
end

function n5collectfrozenwritercalls!(calls::Vector{Tuple{Symbol,Bool}},
        expression)
    expression isa Expr || return calls
    if expression.head == :call && !isempty(expression.args)
        name = n5frozenwriterreference(expression.args[1])
        if name !== nothing
            push!(calls, (name, n5frozenwriterusesoptions(expression)))
            for argument in expression.args[2:end]
                n5collectfrozenwritercalls!(calls, argument)
            end
            return calls
        end
    end
    name = n5frozenwriterreference(expression)
    name === nothing || push!(calls, (name, false))
    for argument in expression.args
        n5collectfrozenwritercalls!(calls, argument)
    end
    return calls
end

function n5frozenwriterguard()
    root = normpath(joinpath(@__DIR__, ".."))
    integration = normpath(joinpath(@__DIR__, "integration.jl"))
    violations = String[]
    calls = Tuple{Symbol,Bool}[]
    for (directory, _, files) in walkdir(root)
        for file in sort!(files)
            endswith(file, ".jl") || continue
            path = normpath(joinpath(directory, file))
            relative = relpath(path, root)
            first(splitpath(relative)) == "hardening" && continue
            parsed = Meta.parseall(read(path, String))
            found = Tuple{Symbol,Bool}[]
            n5collectfrozenwritercalls!(found, parsed)
            if path == integration
                append!(calls, found)
            else
                append!(violations,
                    ("$relative:$name" for (name, _) in found))
            end
        end
    end
    append!(violations,
        ("model/integration.jl:$name" for (name, usesoptions) in calls
            if !usesoptions))
    return (; integration, calls, violations)
end

@testset "N5 frozen outputs disable writer statistics" begin
    guard = n5frozenwriterguard()
    @test guard.violations == String[]
    @test guard.calls == [(:_encodefile, true), (:_encodefile, true),
        (:write, true), (:write, true)]
    source = read(guard.integration, String)
    helper = match(r"(?ms)^function n5productionencodedbytes\b.*?^end$",
        source)
    @test helper !== nothing
    if helper !== nothing
        options = match(r"(?s)options\s*=\s*\(;(.*?)\)", helper.match)
        @test options !== nothing
        if options !== nothing
            @test occursin(r"\bstatistics\s*=\s*false\b", options.match)
        end
        helpercalls = Tuple{Symbol,Bool}[]
        n5collectfrozenwritercalls!(helpercalls, Meta.parse(helper.match))
        @test helpercalls == guard.calls
    end
end

function n5checkcase(case)
    compiled = N5.n5compile(case.node)
    physical = N5.n5physicalleaves(case.schema)
    @test length(compiled.leaves) == length(case.streams)
    @test length(physical) == length(case.streams)
    @test [leaf.path for leaf in physical] == case.paths
    @test [leaf.max_repetition for leaf in physical] ==
        [stream.max_repetition for stream in case.streams]
    @test [leaf.max_definition for leaf in physical] ==
        [stream.max_definition for stream in case.streams]
    @test [leaf.max_repetition for leaf in compiled.leaves] ==
        [stream.max_repetition for stream in case.streams]
    @test [leaf.max_definition for leaf in compiled.leaves] ==
        [stream.max_definition for stream in case.streams]
    actual = N5.n5shred(compiled, case.rows)
    @test isequal(actual, case.streams)
    @test isequal(N5.n5assemble(compiled, actual, length(case.rows)), case.rows)
    return compiled
end

function n5checkbytes(case, compiled, pageversion::Symbol)
    firstbytes = N5.n5emitfile(case.schema, case.streams, length(case.rows);
        pageversion=pageversion)
    @test firstbytes == N5.n5emitfile(case.schema, case.streams,
        length(case.rows); pageversion=pageversion)
    decoded = N5.n5decodefile(firstbytes)
    @test decoded.metadata.schema == case.schema
    @test decoded.metadata.schema[1].name == "schema"
    @test decoded.metadata.num_rows == length(case.rows)
    @test [leaf.path for leaf in decoded.leaves] == case.paths
    @test all(==(pageversion), decoded.pageversions)
    @test isequal(decoded.streams, case.streams)
    @test isequal(N5.n5assemble(compiled, decoded.streams,
        length(case.rows)), case.rows)
    return firstbytes
end

function n5productioncheck(node, schema, streams, expectedrows,
        pageversion::Symbol)
    compiled = N5.n5compile(node)
    bytes = N5.n5emitfile(schema, streams, length(expectedrows);
        pageversion=pageversion)
    table = Parquet.Table(bytes)
    try
        assembled = N5.n5assemble(compiled, streams, length(expectedrows))
        @test isequal(assembled, expectedrows)
        @test isequal(N5.n5normalizetable(node, table), assembled)
        expectedtree = N5.n5expectedvectortree(node, assembled)
        @test N5.n5comparevectortree(expectedtree, table.columns)
        actual, fields, rows = N5.n5productionstreams(table, compiled)
        @test rows == length(expectedrows)
        @test fields.elements == schema
        @test N5.n5compareproductionstreams(actual, streams, compiled)
    finally
        close(table)
    end
    return bytes
end

function n5productionbytecheck(case, pageversion::Symbol)
    source = N5.n5emitfile(case.schema, case.streams, length(case.rows);
        pageversion=pageversion)
    sourcedecoded = N5.n5decodefile(source)
    table = Parquet.Table(source)
    try
        outputs = N5.n5productionencodedbytes(table, pageversion)
        @test outputs.privatefirst == outputs.privatesecond
        @test outputs.publicfirst == outputs.publicsecond
        @test outputs.privatefirst == outputs.publicfirst
        for bytes in (outputs.privatefirst, outputs.publicfirst)
            decoded = N5.n5decodefile(bytes)
            @test decoded.metadata.schema == case.schema
            @test N5.n5schemaexact(decoded.metadata.schema,
                sourcedecoded.metadata.schema)
            @test decoded.metadata.num_rows == length(case.rows)
            @test [leaf.path for leaf in decoded.leaves] == case.paths
            @test all(==(pageversion), decoded.pageversions)
            @test isequal(decoded.streams, case.streams)
            compiled = N5.n5compile(case.node)
            @test isequal(N5.n5assemble(compiled, decoded.streams,
                length(case.rows)), case.rows)
        end
        return outputs.privatefirst
    finally
        close(table)
    end
end

function n5tableerror(bytes)
    table = try
        Parquet.Table(bytes)
    catch error
        return error
    end
    close(table)
    return nothing
end

function n5replace(value; changes...)
    names = fieldnames(typeof(value))
    fields = map(names) do name
        return haskey(changes, name) ? changes[name] : getfield(value, name)
    end
    return typeof(value)(fields...)
end

function n5headermutator(pageversion::Symbol; changes...)
    return function(header)
        if pageversion === :v1
            data = n5replace(header.data_page_header; changes...)
            return n5replace(header; data_page_header=data)
        end
        data = n5replace(header.data_page_header_v2; changes...)
        return n5replace(header; data_page_header_v2=data)
    end
end

function n5pageheadermutator(; changes...)
    return header -> n5replace(header; changes...)
end

function n5footercolumnmutator(bytes::Vector{UInt8}; changes...)
    metadata, dataend = N5.n5decodefooter(bytes)
    rowgroup = only(metadata.row_groups)
    chunk = only(rowgroup.columns)
    column = n5replace(chunk.meta_data; changes...)
    updatedchunk = n5replace(chunk; meta_data=column)
    updatedgroup = n5replace(rowgroup;
        columns=N5.MD.ColumnChunk[updatedchunk])
    updatedmetadata = n5replace(metadata;
        row_groups=N5.MD.RowGroup[updatedgroup])
    footer = N5.TH.encode(updatedmetadata)
    output = Vector{UInt8}(view(bytes, firstindex(bytes):dataend))
    append!(output, footer)
    N5._n5pushu32!(output, UInt32(length(footer)))
    append!(output, N5.N5_MAGIC)
    return output
end

function n5footerlengthmutator(bytes::Vector{UInt8}, lengthvalue::UInt32)
    output = copy(bytes)
    encoded = UInt8[]
    N5._n5pushu32!(encoded, lengthvalue)
    output[(end - 7):(end - 4)] = encoded
    return output
end

function n5modeldecodeerror(bytes::Vector{UInt8})
    try
        N5.n5decodefile(bytes)
    catch error
        return error
    end
    return nothing
end

function n5propertycodecenum(codec::Symbol)
    codec === :uncompressed && return N5.MD.CompressionCodec.UNCOMPRESSED
    codec === :snappy && return N5.MD.CompressionCodec.SNAPPY
    codec === :gzip && return N5.MD.CompressionCodec.GZIP
    codec === :brotli && return N5.MD.CompressionCodec.BROTLI
    codec === :zstd && return N5.MD.CompressionCodec.ZSTD
    codec === :lz4_raw && return N5.MD.CompressionCodec.LZ4_RAW
    throw(ArgumentError("unsupported N5 property codec $codec"))
end

function n5propertycodecmetadata(table, codec::Symbol)
    expected = n5propertycodecenum(codec)
    for rowgroup in table.metadata.row_groups
        for chunk in rowgroup.columns
            @test chunk.meta_data.codec == expected
        end
    end
    return
end

@testset "N5 independent binding goldens" begin
    goldens = N5.n5bindinggoldens()
    @test length(goldens) == 12
    @test [case.name for case in goldens] == [
        "list-rule-1",
        "list-rule-2",
        "list-rule-3",
        "list-rule-4-array",
        "list-rule-4-tuple",
        "list-rule-5-required",
        "list-rule-5-paired",
        "list-rule-5-extended",
        "direct-list-of-map",
        "map-standard",
        "map-key-only",
        "map-optional-key",
    ]
    compiled = Dict{String,N5.N5Compiled}()
    for case in goldens
        compiled[case.name] = n5checkcase(case)
    end
    rule4 = only(case for case in goldens if case.name == "list-rule-4-array")
    rule5 = only(case for case in goldens if case.name == "list-rule-5-paired")
    @test isequal(rule4.streams, rule5.streams)
    @test !isequal(rule4.rows, rule5.rows)
    standard = only(case for case in goldens if case.name == "map-standard")
    @test N5.n5maplookup(standard.rows[4], "a") == Int32(2)
    @test_throws KeyError N5.n5maplookup(standard.rows[4], "absent")
    optionalkey = only(case for case in goldens if
        case.name == "map-optional-key")
    key = optionalkey.streams[1]
    definitions = copy(key.definition)
    definitions[3] = UInt64(2)
    mutated = copy(optionalkey.streams)
    mutated[1] = N5.N5LeafStream(copy(key.repetition), definitions,
        Any[key.values[2:end]...], key.max_repetition, key.max_definition)
    @test_throws ArgumentError N5.n5assemble(compiled[optionalkey.name],
        mutated, length(optionalkey.rows))
    for pageversion in (:v1, :v2)
        bytes = N5.n5emitfile(optionalkey.schema, mutated,
            length(optionalkey.rows); pageversion=pageversion)
        @test n5tableerror(bytes) isa Parquet.FormatError
    end
end

@testset "N5 independent hybrid decoder" begin
    levels = UInt64[0, 0, 1, 1, 1, 2, 2, 0]
    encoded = N5.n5encodehybrid(levels, 3; length_prefix=false)
    decoded, position = N5.n5decodehybrid(encoded, 1, length(encoded),
        length(levels), 3; length_prefix=false)
    @test decoded == levels
    @test position == length(encoded) + 1
    prefixed = N5.n5encodehybrid(levels, 3; length_prefix=true)
    decoded, position = N5.n5decodehybrid(prefixed, 1, length(prefixed),
        length(levels), 3; length_prefix=true)
    @test decoded == levels
    @test position == length(prefixed) + 1
    bitpacked = UInt8[0x03, 0xe4, 0xe4]
    expected = UInt64[0, 1, 2, 3, 0, 1, 2, 3]
    decoded, position = N5.n5decodehybrid(bitpacked, 1, length(bitpacked),
        8, 3; length_prefix=false)
    @test decoded == expected
    @test position == length(bitpacked) + 1
    bitpackedprefix = UInt8[0x03, 0x00, 0x00, 0x00, bitpacked...]
    decoded, position = N5.n5decodehybrid(bitpackedprefix, 1,
        length(bitpackedprefix), 8, 3; length_prefix=true)
    @test decoded == expected
    @test position == length(bitpackedprefix) + 1
    @test_throws ArgumentError N5.n5decodehybrid(UInt8[0x02], 1, 1,
        1, 1; length_prefix=false)
    @test_throws ArgumentError N5.n5decodehybrid(UInt8[0x02, 0x02], 1, 2,
        1, 1; length_prefix=false)
    @test_throws ArgumentError N5.n5decodehybrid(UInt8[], 1, 0,
        N5.N5_MAX_DECODE_VALUES + 1, 0; length_prefix=false)
    oversizedrun = UInt8[fill(UInt8(0xff), 9)..., UInt8(0x01)]
    error = try
        N5.n5decodehybrid(oversizedrun, 1, length(oversizedrun), 1, 1;
            length_prefix=false)
        nothing
    catch caught
        caught
    end
    @test error isa ArgumentError
    @test error.msg == "N5 hybrid bit-packed value count overflows Int"
    error = try
        N5.n5bitwidth(big(1) << 100)
        nothing
    catch caught
        caught
    end
    @test error isa ArgumentError
    @test error.msg == "N5 level maximum does not fit UInt64"
end

@testset "N5 hostile serialized page-header counts" begin
    case = first(N5.n5bindinggoldens())
    v1changes = (
        (; num_values=Int32(-1)),
        (; num_values=Int32(N5.N5_MAX_DECODE_VALUES + 1)),
        (; repetition_level_encoding=N5.MD.Encoding.BIT_PACKED),
        (; definition_level_encoding=N5.MD.Encoding.BIT_PACKED),
    )
    for changes in v1changes
        bytes = N5.n5emitfile(case.schema, case.streams, length(case.rows);
            pageversion=:v1, headermutator=n5headermutator(:v1; changes...))
        @test_throws ArgumentError N5.n5decodefile(bytes)
    end
    v2changes = (
        (; num_values=Int32(-1)),
        (; num_values=Int32(N5.N5_MAX_DECODE_VALUES + 1)),
        (; num_rows=Int32(-1)),
        (; num_rows=Int32(N5.N5_MAX_DECODE_VALUES + 1)),
        (; num_nulls=Int32(-1)),
        (; num_nulls=Int32(N5.N5_MAX_DECODE_VALUES + 1)),
        (; repetition_levels_byte_length=Int32(-1)),
        (; repetition_levels_byte_length=typemax(Int32)),
        (; definition_levels_byte_length=Int32(-1)),
        (; definition_levels_byte_length=typemax(Int32)),
    )
    for changes in v2changes
        bytes = N5.n5emitfile(case.schema, case.streams, length(case.rows);
            pageversion=:v2, headermutator=n5headermutator(:v2; changes...))
        @test_throws ArgumentError N5.n5decodefile(bytes)
    end
end

@testset "N5 hostile serialized footer and frame ranges" begin
    case = first(N5.n5bindinggoldens())
    source = N5.n5emitfile(case.schema, case.streams, length(case.rows))
    footerchanges = (
        ((; data_page_offset=typemax(Int64), total_compressed_size=Int64(1)),
            "N5 page first byte overflows Int"),
        ((; data_page_offset=Int64(4),
            total_compressed_size=typemax(Int64)),
            "N5 page frame exceeds its bound"),
        ((; data_page_offset=Int64(-1), total_compressed_size=Int64(1)),
            "N5 page offset is negative"),
        ((; data_page_offset=Int64(4), total_compressed_size=Int64(0)),
            "N5 page frame is empty"),
    )
    for (changes, message) in footerchanges
        bytes = n5footercolumnmutator(source; changes...)
        error = n5modeldecodeerror(bytes)
        @test error isa ArgumentError
        @test error.msg == message
    end
    for (lengthvalue, message) in (
            (UInt32(0), "N5 footer is empty"),
            (typemax(UInt32), "N5 footer starts before data"))
        error = n5modeldecodeerror(n5footerlengthmutator(source, lengthvalue))
        @test error isa ArgumentError
        @test error.msg == message
    end
    pagechanges = (
        ((; compressed_page_size=Int32(-1),
            uncompressed_page_size=Int32(-1)),
            "N5 compressed page size is negative"),
        ((; uncompressed_page_size=Int32(-1)),
            "N5 uncompressed page size is negative"),
        ((; compressed_page_size=typemax(Int32),
            uncompressed_page_size=typemax(Int32)),
            "N5 page payload exceeds its bound"),
    )
    for pageversion in (:v1, :v2), (changes, message) in pagechanges
        bytes = N5.n5emitfile(case.schema, case.streams, length(case.rows);
            pageversion=pageversion,
            headermutator=n5pageheadermutator(; changes...))
        error = n5modeldecodeerror(bytes)
        @test error isa ArgumentError
        @test error.msg == message
    end
end

@testset "N5 independent V1 and V2 fixtures" begin
    for case in N5.n5bindinggoldens()
        compiled = N5.n5compile(case.node)
        v1 = n5checkbytes(case, compiled, :v1)
        v2 = n5checkbytes(case, compiled, :v2)
        @test v1 != v2
    end
end

@testset "N5 independently decoded production writes" begin
    for case in N5.n5bindinggoldens()
        v1 = n5productionbytecheck(case, :v1)
        v2 = n5productionbytecheck(case, :v2)
        @test v1 != v2
    end
end

@testset "N5 exact schema-bearing rewrite" begin
    case = N5.n5provenancegolden()
    @test !isempty(case.schema[1].unknown_fields)
    @test !isempty(case.schema[2].unknown_fields)
    @test !isempty(case.schema[2].logicalType.LIST.unknown_fields)
    @test !isempty(case.schema[3].unknown_fields)
    @test !isempty(case.schema[3].logicalType.unknown_fields)
    for pageversion in (:v1, :v2)
        n5productionbytecheck(case, pageversion)
    end
end

@testset "N5 checked golden manifest" begin
    manifest = N5.n5goldenmanifest()
    expected = N5.N5_GOLDEN_FILE_SHA256
    @test length(manifest) == length(expected) == 12
    for (entry, hashes) in zip(manifest, expected)
        @test (entry.name, entry.v1_sha256, entry.v2_sha256) == hashes
    end
    firstbytes = N5.n5encodemanifest(manifest)
    @test firstbytes == N5.n5encodemanifest(N5.n5goldenmanifest())
    @test N5.n5manifestsha256(manifest) ==
        N5.N5_GOLDEN_MANIFEST_SHA256
end

@testset "N5 production boundaries for binding goldens" begin
    goldens = N5.n5bindinggoldens()
    for case in goldens
        for pageversion in (:v1, :v2)
            n5productioncheck(case.node, case.schema, case.streams,
                case.rows, pageversion)
        end
    end
    standard = only(case for case in goldens if case.name == "map-standard")
    for pageversion in (:v1, :v2)
        bytes = N5.n5emitfile(standard.schema, standard.streams,
            length(standard.rows); pageversion=pageversion)
        table = Parquet.Table(bytes)
        try
            duplicated = table.columns.attrs[4]
            @test Parquet.maplookup(duplicated, "a") == Int32(2)
            @test_throws KeyError Parquet.maplookup(duplicated, "absent")
        finally
            close(table)
        end
    end
end

@testset "N5 complete accepted MAP Cartesian product" begin
    cases = N5.n5mapmatrix()
    @test length(cases) == 1152
    @test Set(case.node.marker for case in cases) == Set((:modern, :dual,
        :modern_alias, :conflict, :modern_primitive, :modern_unknown,
        :legacy, :alias))
    @test Set(case.node.entrymarker for case in cases) == Set((:none, :marked,
        :empty, :future, :future_marked, :unknown_converted))
    seen = Set{String}()
    for case in cases
        @test case.name ∉ seen
        push!(seen, case.name)
        compiled = N5.n5compile(case.node)
        schema = N5.n5schema(case.node)
        streams = N5.n5shred(compiled, case.rows)
        @test isequal(N5.n5assemble(compiled, streams, length(case.rows)),
            case.rows)
        for pageversion in (:v1, :v2)
            bytes = N5.n5emitfile(schema, streams, length(case.rows);
                pageversion=pageversion)
            decoded = N5.n5decodefile(bytes)
            @test decoded.metadata.schema == schema
            @test isequal(decoded.streams, streams)
            @test isequal(N5.n5assemble(compiled, decoded.streams,
                length(case.rows)), case.rows)
            n5productioncheck(case.node, schema, streams, case.rows,
                pageversion)
        end
    end
end

@testset "N5 recursive MAP key and value shapes" begin
    cases = N5.n5recursivecases()
    @test length(cases) == 3
    for case in cases
        compiled = N5.n5compile(case.node)
        schema = N5.n5schema(case.node)
        streams = N5.n5shred(compiled, case.rows)
        @test isequal(N5.n5assemble(compiled, streams, length(case.rows)),
            case.rows)
        for pageversion in (:v1, :v2)
            bytes = N5.n5emitfile(schema, streams, length(case.rows);
                pageversion=pageversion)
            decoded = N5.n5decodefile(bytes)
            @test decoded.metadata.schema == schema
            @test isequal(decoded.streams, streams)
            n5productioncheck(case.node, schema, streams, case.rows,
                pageversion)
        end
    end
end

@testset "N5 annotation precedence controls" begin
    for case in N5.n5annotationcontrols()
        compiled = N5.n5compile(case.node)
        schema = N5.n5schema(case.node)
        streams = N5.n5shred(compiled, case.rows)
        @test isequal(N5.n5assemble(compiled, streams, length(case.rows)),
            case.rows)
        for pageversion in (:v1, :v2)
            bytes = N5.n5emitfile(schema, streams, length(case.rows);
                pageversion=pageversion)
            decoded = N5.n5decodefile(bytes)
            @test decoded.metadata.schema == schema
            @test isequal(decoded.streams, streams)
            n5productioncheck(case.node, schema, streams, case.rows,
                pageversion)
        end
    end
end

@testset "N5 serialized LIST precedence rows" begin
    controls = N5.n5listbindingcontrols()
    @test [control.name for control in controls] == [
        "list-rule-2-multifield-array",
        "list-rule-3-repeated-array",
        "list-rule-3-unannotated-group",
        "list-rule-5-Array",
        "list-rule-5-ARRAY",
        "list-rule-5-case-mismatched-tuple",
        "list-unknown-blocks-legacy",
    ]
    for control in controls
        @test control.node !== nothing
        node = control.node
        compiled = N5.n5compile(node)
        for pageversion in (:v1, :v2)
            bytes = N5.n5emitfile(control.schema, control.streams,
                length(control.rows); pageversion=pageversion)
            decoded = N5.n5decodefile(bytes)
            @test decoded.metadata.schema == control.schema
            @test isequal(decoded.streams, control.streams)
            table = Parquet.Table(bytes)
            try
                column = first(values(table.columns))
                if control.expected === :list
                    @test column isa Parquet.ListVector
                else
                    @test column isa Parquet.StructVector
                end
                @test isequal(N5.n5normalizetable(node, table), control.rows)
                expectedtree = N5.n5expectedvectortree(node, control.rows)
                @test N5.n5comparevectortree(expectedtree, table.columns)
                actual, fields, rows = N5.n5productionstreams(table,
                    compiled)
                @test rows == length(control.rows)
                @test fields.elements == control.schema
                @test N5.n5compareproductionstreams(actual,
                    control.streams, compiled)
            finally
                close(table)
            end
        end
    end
end

@testset "N5 complete MAP annotation binding rows" begin
    controls = N5.n5mapbindingcontrols()
    @test [control.name for control in controls] == [
        "outer-modern-map",
        "outer-modern-map-matching",
        "outer-modern-map-alias",
        "outer-modern-map-conflict",
        "outer-modern-map-primitive",
        "outer-modern-map-unknown-converted",
        "outer-legacy-map",
        "outer-legacy-map-alias",
        "outer-unknown-blocks-map",
        "outer-unknown-blocks-alias",
        "outer-unannotated",
        "outer-unknown-converted",
        "outer-list-wins-map",
        "outer-list-wins-alias",
        "outer-variant-wins",
        "outer-empty-wins",
        "outer-converted-list",
        "entry-unmarked",
        "entry-map-key-value",
        "entry-empty-logical",
        "entry-unknown-logical",
        "entry-unknown-blocks-map-key-value",
        "entry-unknown-converted",
    ]
    for control in controls
        @test control.node !== nothing
        node = control.node
        compiled = N5.n5compile(node)
        for pageversion in (:v1, :v2)
            bytes = N5.n5emitfile(control.schema, control.streams,
                length(control.rows); pageversion=pageversion)
            decoded = N5.n5decodefile(bytes)
            @test decoded.metadata.schema == control.schema
            @test isequal(decoded.streams, control.streams)
            table = Parquet.Table(bytes)
            try
                column = first(values(table.columns))
                if control.expected === :map
                    @test column isa Parquet.MapVector
                elseif control.expected === :list
                    @test column isa Parquet.ListVector
                else
                    @test column isa Parquet.StructVector
                end
                @test isequal(N5.n5normalizetable(node, table), control.rows)
                expectedtree = N5.n5expectedvectortree(node, control.rows)
                @test N5.n5comparevectortree(expectedtree, table.columns)
                actual, fields, rows = N5.n5productionstreams(table,
                    compiled)
                @test rows == length(control.rows)
                @test fields.elements == control.schema
                @test N5.n5compareproductionstreams(actual,
                    control.streams, compiled)
            finally
                close(table)
            end
        end
    end
end

@testset "N5 complete MAP rejected binding neighbors" begin
    failures = N5.n5mapbindingfailures()
    @test [failure.name for failure in failures] == [
        "entry-logical-map",
        "entry-logical-list",
        "entry-logical-variant",
        "entry-logical-primitive",
        "entry-converted-map",
        "entry-converted-list",
        "entry-converted-primitive",
        "outer-primitive-logical",
        "outer-primitive-converted",
        "outer-repeated-outside-list",
        "outer-zero-child",
        "outer-two-children",
        "entry-primitive",
        "entry-non-repeated",
        "entry-zero-child-key-absent",
        "entry-three-children",
        "key-repeated",
        "value-repeated",
    ]
    for failure in failures
        for pageversion in (:v1, :v2)
            bytes = N5.n5emitfile(failure.schema, failure.streams, 0;
                pageversion=pageversion)
            @test n5tableerror(bytes) isa Parquet.FormatError
        end
    end
end

@testset "N5 model validation" begin
    @test_throws ArgumentError N5.n5list("bad",
        N5.n5primitive("value", N5.MD.Type.INT32; optional=true);
        layout=:rule1)
    @test_throws ArgumentError N5.n5list("bad",
        N5.n5primitive("value", N5.MD.Type.INT32); layout=:rule2)
    @test_throws ArgumentError N5.n5list("bad",
        N5.n5primitive("value", N5.MD.Type.INT32); layout=:rule3)
    @test_throws ArgumentError N5.n5struct("empty", N5.N5Node[])
    required = N5.n5primitive("value", N5.MD.Type.INT32)
    @test_throws ArgumentError N5.n5shred(N5.n5compile(required), Any[missing])
    key = N5.n5primitive("key", N5.MD.Type.BYTE_ARRAY;
        optional=true, logical=:string)
    mapnode = N5.n5map("attrs", key, nothing)
    @test_throws ArgumentError N5.n5shred(N5.n5compile(mapnode),
        Any[N5.N5MapValue(N5.N5Entry[ N5.N5Entry(missing, nothing, false) ])])
    bytes = N5.n5emitfile(N5.n5schema(required),
        N5.n5shred(N5.n5compile(required), Any[Int32(1)]), 1)
    error = try
        N5.n5emitfile(N5.n5schema(required),
            N5.n5shred(N5.n5compile(required), Any[Int32(1)]),
            typemax(UInt128))
        nothing
    catch caught
        caught
    end
    @test error isa ArgumentError
    @test error.msg == "N5 row count does not fit Int"
    corrupted = copy(bytes)
    corrupted[1] = 0x00
    @test_throws ArgumentError N5.n5decodefile(corrupted)
    truncated = bytes[1:(end - 1)]
    @test_throws ArgumentError N5.n5decodefile(truncated)
end

@testset "N5 deterministic recursive property schedule" begin
    cases = N5_PROPERTY_CASES
    rejected = N5_PROPERTY_REJECTED
    @test length(cases) == N5.N5_PROPERTY_CASE_COUNT == 256
    @test rejected == length(N5.N5_PROPERTY_REJECTED_PREFIX) == 6
    @test first(cases).id == 0
    @test last(cases).id == 261
    @test issorted(case.id for case in cases)
    rejectedids = [id for (id, _) in N5.N5_PROPERTY_REJECTED_PREFIX]
    @test [id for id in 0:last(cases).id if
        all(case -> case.id != id, cases)] == rejectedids
    @test [case.pageversion for case in cases] ==
        [isodd(index) ? :v1 : :v2 for index in eachindex(cases)]
    for (id, reason) in N5.N5_PROPERTY_REJECTED_PREFIX
        assessment = N5.n5propertycandidateassessment(id)
        @test assessment.candidate === nothing
        @test assessment.reason === reason
        actual = getproperty(assessment.metrics, reason)
        limit = reason === :depth ? 6 : reason === :width ? 4 :
            reason === :astnodes ? N5.N5_PROPERTY_MAX_AST_NODES :
            reason === :leaves ? N5.N5_PROPERTY_MAX_LEAVES :
            reason === :levelentries ? N5.N5_PROPERTY_MAX_LEVEL_ENTRIES :
            reason === :payloadbytes ? N5.N5_PROPERTY_MAX_PAYLOAD_BYTES : 0
        @test actual > limit
    end
    after = N5.n5propertycandidateassessment(last(cases).id + 1)
    @test after.reason === nothing
    repeated, repeatedrejected = N5.n5propertycases()
    @test repeatedrejected == rejected
    @test [case.id for case in repeated] == [case.id for case in cases]
    @test last(repeated).id < N5.N5_PROPERTY_CANDIDATE_LAST
    coverage = N5.n5propertycoverage(cases, rejected)
    @test isempty(N5.n5propertymissingcoverage(coverage))
    @test coverage.rejected_candidates == rejected
    manifest = N5.n5encodepropertymanifest(cases, rejected)
    @test manifest == N5.n5encodepropertymanifest(repeated, repeatedrejected)
    @test N5.n5propertymanifestsha256(cases, rejected) ==
        N5.N5_PROPERTY_MANIFEST_SHA256
    @test N5.n5manifestsha256(N5.n5goldenmanifest()) ==
        N5.N5_GOLDEN_MANIFEST_SHA256
end

@testset "N5 explicit zero-row property files" begin
    cases = [case for case in N5_PROPERTY_CASES if isempty(case.rows)]
    @test length(cases) == 11
    for case in cases, pageversion in (:v1, :v2)
        bytes = N5.n5emitfile(case.schema, case.streams, 0;
            pageversion=pageversion)
        decoded = N5.n5decodefile(bytes)
        @test decoded.metadata.num_rows == 0
        @test isempty(decoded.metadata.row_groups)
        @test isempty(decoded.pageversions)
        @test all(isempty(stream.repetition) &&
            isempty(stream.definition) && isempty(stream.values)
            for stream in decoded.streams)
        table = Parquet.Table(bytes)
        try
            @test table.rows == 0
            @test isempty(table.metadata.row_groups)
            @test N5.n5schemaexact(table.metadata.schema,
                decoded.metadata.schema)
            @test isequal(N5.n5normalizetable(case.node, table), case.rows)
            expectedtree = N5.n5expectedvectortree(case.node, case.rows)
            @test N5.n5comparevectortree(expectedtree, table.columns)
        finally
            close(table)
        end
    end
end

@testset "N5 256 independent and production property cases" begin
    for case in N5_PROPERTY_CASES
        @testset "$(N5.n5propertydiagnostic(case))" begin
            @test 1 <= case.depth <= 6
            @test 1 <= case.width <= 4
            @test case.astnodes <= N5.N5_PROPERTY_MAX_AST_NODES
            @test case.leaves <= N5.N5_PROPERTY_MAX_LEAVES
            @test case.levelentries <= N5.N5_PROPERTY_MAX_LEVEL_ENTRIES
            @test case.densevalues <= N5.N5_PROPERTY_MAX_DENSE_VALUES
            @test case.payloadbytes <= N5.N5_PROPERTY_MAX_PAYLOAD_BYTES
            @test all(count(iszero, stream.repetition) == length(case.rows)
                for stream in case.streams)
            compiled = n5checkcase(case)
            independent = n5checkbytes(case, compiled, case.pageversion)
            @test independent == N5.n5emitfile(case.schema, case.streams,
                length(case.rows); pageversion=case.pageversion)
            @test n5productioncheck(case.node, case.schema, case.streams,
                case.rows, case.pageversion) == independent
            production = n5productionbytecheck(case, case.pageversion)
            source = N5.n5decodefile(independent)
            table = Parquet.Table(production)
            try
                @test N5.n5schemaexact(table.metadata.schema,
                    source.metadata.schema)
                @test isequal(N5.n5normalizetable(case.node, table), case.rows)
                expectedtree = N5.n5expectedvectortree(case.node, case.rows)
                @test N5.n5comparevectortree(expectedtree, table.columns)
            finally
                close(table)
            end
        end
    end
end

@testset "N5 stable six-codec property subset" begin
    cases = N5_PROPERTY_CASES
    subset = N5.n5propertycodecsubset(cases)
    @test Tuple(case.id for case in subset) == N5.N5_PROPERTY_CODEC_CASE_IDS
    @test length(Set(case.pageversion for case in subset)) == 2
    fixtures = N5.n5propertycodecfixtures(cases)
    @test length(fixtures) ==
        N5.N5_PROPERTY_CODEC_COUNT * length(N5.N5_PROPERTY_CODECS) == 192
    @test length(Set(fixture.filename for fixture in fixtures)) ==
        length(fixtures)
    byid = Dict(case.id => case for case in subset)
    seen = Set{Tuple{Int,Symbol}}()
    for fixture in fixtures
        case = byid[fixture.caseid]
        push!(seen, (fixture.caseid, fixture.codec))
        @test fixture.name == case.name
        @test fixture.pageversion == case.pageversion
        @test fixture.schema == case.schema
        @test fixture.paths == case.paths
        @test isequal(fixture.rows, case.rows)
        @test fixture.sha256 == bytes2hex(SHA.sha256(fixture.bytes))
        table = Parquet.Table(fixture.bytes)
        try
            @test table.rows == length(case.rows)
            @test N5.n5schemaexact(table.metadata.schema, fixture.schema)
            @test isequal(N5.n5normalizetable(case.node, table), case.rows)
            expectedtree = N5.n5expectedvectortree(case.node, case.rows)
            @test N5.n5comparevectortree(expectedtree, table.columns)
            actual, fields, rows = N5.n5productionstreams(table,
                N5.n5compile(case.node))
            @test rows == length(case.rows)
            @test N5.n5schemaexact(fields.elements, fixture.schema)
            @test N5.n5compareproductionstreams(actual, case.streams,
                N5.n5compile(case.node))
            n5propertycodecmetadata(table, fixture.codec)
        finally
            close(table)
        end
    end
    @test seen == Set((case.id, codec) for case in subset
        for codec in N5.N5_PROPERTY_CODECS)
end
