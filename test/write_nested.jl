using Dates
using Tables
using Test

const WNMD = Parquet.Metadata

struct WNDeclaredVector{T} <: AbstractVector{T}
    values::Vector{Any}
end

function Base.IndexStyle(::Type{<:WNDeclaredVector})
    return IndexLinear()
end

function Base.size(values::WNDeclaredVector)
    return size(values.values)
end

function Base.getindex(values::WNDeclaredVector, index::Int)
    return values.values[index]
end

struct WNBadMap <: AbstractDict{String,Int32} end

function Base.length(::WNBadMap)
    return 1
end

function Base.getindex(::WNBadMap, ::String)
    return Int32(1)
end

function Base.iterate(::WNBadMap, state::Bool=false)
    state && return nothing
    return missing => Int32(1), true
end

mutable struct WNTrackedRows{S}
    consumed::Base.RefValue{Int}
    rows::Int
end

Tables.istable(::Type{<:WNTrackedRows}) = true
Tables.rowaccess(::Type{<:WNTrackedRows}) = true
Tables.rows(rows::WNTrackedRows) = rows
Tables.schema(::WNTrackedRows) = Tables.Schema((:value,), (Int32,))
Base.IteratorSize(::Type{WNTrackedRows{S}}) where {S} = S()
Base.length(rows::WNTrackedRows{Base.HasLength}) = rows.rows

function Base.iterate(rows::WNTrackedRows, state::Int=1)
    state > rows.rows && return nothing
    rows.consumed[] += 1
    return (value=Int32(state),), state + 1
end

mutable struct WNFreshStringRows{S}
    consumed::Base.RefValue{Int}
    rows::Int
    width::Int
end

Tables.istable(::Type{<:WNFreshStringRows}) = true
Tables.rowaccess(::Type{<:WNFreshStringRows}) = true
Tables.rows(rows::WNFreshStringRows) = rows
Tables.schema(::WNFreshStringRows) = Tables.Schema((:value,), (String,))
Base.IteratorSize(::Type{WNFreshStringRows{S}}) where {S} = S()
Base.length(rows::WNFreshStringRows{Base.HasLength}) = rows.rows

function Base.iterate(rows::WNFreshStringRows, state::Int=1)
    state > rows.rows && return nothing
    rows.consumed[] += 1
    return (value=repeat("x", rows.width),), state + 1
end

mutable struct WNFreshPayloadRows{T,F}
    consumed::Base.RefValue{Int}
    rows::Int
    width::Int
    maker::F
end

Tables.istable(::Type{<:WNFreshPayloadRows}) = true
Tables.rowaccess(::Type{<:WNFreshPayloadRows}) = true
Tables.rows(rows::WNFreshPayloadRows) = rows
Tables.schema(::WNFreshPayloadRows{T}) where {T} =
    Tables.Schema((:value,), (T,))
Base.IteratorSize(::Type{<:WNFreshPayloadRows}) = Base.HasLength()
Base.length(rows::WNFreshPayloadRows) = rows.rows

function Base.iterate(rows::WNFreshPayloadRows, state::Int=1)
    state > rows.rows && return nothing
    rows.consumed[] += 1
    return (value=rows.maker(rows.width),), state + 1
end

function wnfreshjson(width::Int)
    return Parquet.JSONValue(codeunits(string('"', repeat("x", width), '"')))
end

function wnfreshbson(width::Int)
    total = width + 13
    bytes = UInt8[
        UInt8(total & 0xff),
        UInt8((total >> 8) & 0xff),
        UInt8((total >> 16) & 0xff),
        UInt8((total >> 24) & 0xff),
        0x05, 0x78, 0x00,
        UInt8(width & 0xff),
        UInt8((width >> 8) & 0xff),
        UInt8((width >> 16) & 0xff),
        UInt8((width >> 24) & 0xff),
        0x00,
    ]
    append!(bytes, fill(UInt8(1), width))
    push!(bytes, 0x00)
    return Parquet.BSONValue(bytes)
end

function wnfreshdecimal(width::Int)
    return Parquet.Decimal(big(1) << (8 * width), 0)
end

struct WNSchemaLessRows{T}
    values::Vector{T}
end

Tables.istable(::Type{<:WNSchemaLessRows}) = true
Tables.rowaccess(::Type{<:WNSchemaLessRows}) = true
Tables.rows(rows::WNSchemaLessRows) = rows
Base.IteratorSize(::Type{<:WNSchemaLessRows}) = Base.HasLength()
Base.IteratorEltype(::Type{<:WNSchemaLessRows}) = Base.HasEltype()
Base.eltype(::Type{WNSchemaLessRows{T}}) where {T} = T
Base.length(rows::WNSchemaLessRows) = length(rows.values)
Base.iterate(rows::WNSchemaLessRows, state...) = iterate(rows.values, state...)

struct WNZeroVector{T} <: AbstractVector{T}
    values::Vector{T}
end

Base.IndexStyle(::Type{<:WNZeroVector}) = IndexCartesian()
Base.size(values::WNZeroVector) = size(values.values)
Base.axes(values::WNZeroVector) = (0:(length(values.values) - 1),)
Base.getindex(values::WNZeroVector, index::Int) = values.values[index + 1]

mutable struct WNThrowingSizeVector{T,E} <: AbstractVector{T}
    values::Vector{T}
    calls::Int
    throw_on::Int
    exception::E
end

function Base.IndexStyle(::Type{<:WNThrowingSizeVector})
    return IndexLinear()
end

function Base.size(values::WNThrowingSizeVector)
    values.calls += 1
    values.calls == values.throw_on && throw(values.exception)
    return size(values.values)
end

function Base.length(values::WNThrowingSizeVector)
    return length(values.values)
end

function Base.axes(values::WNThrowingSizeVector)
    return (Base.OneTo(length(values.values)),)
end

function Base.firstindex(::WNThrowingSizeVector)
    return 1
end

function Base.lastindex(values::WNThrowingSizeVector)
    return length(values.values)
end

function Base.getindex(values::WNThrowingSizeVector, index::Int)
    return values.values[index]
end

function wndeepstruct(depth::Int, rows::Int)
    values::AbstractVector = iszero(rows) ? Int32[] : Int32[7]
    for level in depth:-1:1
        childname = level == depth ? "value" : "level_$(level + 1)"
        values = Parquet.StructVector(String[childname],
            AbstractVector[values]; rows=rows)
    end
    return values
end

function wnwriterpassallocations(source::AbstractVector)
    limits = Parquet.Limits(max_materialized_bytes=1_000_000_000)
    budget = Parquet._LiveByteBudget(limits)
    shape = Parquet._nestedwriteshape("value", eltype(source), source,
        limits, budget)
    shapes = Parquet._NestedWriteShape[shape]
    values = AbstractVector[source]
    rows = length(source)
    fragment = Parquet._nestedwriteschema(shape, limits, budget)
    fragments = Vector{Parquet.Metadata.SchemaElement}[fragment]
    semantic, plans = Parquet._nestedwritecompile(shapes, fragments, limits,
        budget)
    Parquet._nestedwritescanaggregates!(shapes, values, rows, limits, nothing)
    scanbytes = @allocated Parquet._nestedwritescanaggregates!(shapes, values,
        rows, limits, nothing)
    counts = Parquet._nestedwritecounts(length(semantic.leaves), budget)
    context = Parquet._NestedWriteCountContext(counts, limits)
    Parquet._nestedwritescanrows!(context, plans, values, rows)
    rowbytes = @allocated Parquet._nestedwritescanrows!(context, plans,
        values, rows)
    return scanbytes, rowbytes
end

struct WNTrackedBytes <: AbstractVector{UInt8}
    values::Vector{UInt8}
    reads::Base.RefValue{Int}
end

Base.IndexStyle(::Type{WNTrackedBytes}) = IndexLinear()
Base.size(values::WNTrackedBytes) = size(values.values)

function Base.getindex(values::WNTrackedBytes, index::Int)
    values.reads[] += 1
    return values.values[index]
end

mutable struct WNChangingStrings <: AbstractVector{String}
    calls::Base.RefValue{Int}
    small::String
    large::String
end

Base.IndexStyle(::Type{WNChangingStrings}) = IndexLinear()
Base.size(::WNChangingStrings) = (1,)

function Base.getindex(values::WNChangingStrings, ::Int)
    values.calls[] += 1
    return values.calls[] <= 2 ? values.small : values.large
end

function wnmutationattack(large::String)
    source = WNChangingStrings(Ref(0), "x", large)
    limits = Parquet.Limits(max_materialized_bytes=50_000)
    budget = Parquet._LiveByteBudget(limits)
    try
        Parquet._writefields((value=source,), limits, budget)
        error("expected nested writer mutation rejection")
    catch err
        err isa ArgumentError || rethrow()
    end
    @assert source.calls[] == 3
    @assert Parquet._budgetused(budget) == 0
    return
end

function wninspect(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    try
        metadata = Parquet.Thrift.decode(copy(file.footer.bytes),
            WNMD.FileMetaData)
        schema = Parquet.Schema(metadata.schema)
        streams = Parquet.LeafStream[]
        if !isempty(metadata.row_groups)
            rows = only(metadata.row_groups).num_rows
            for index in eachindex(schema.leaves)
                push!(streams, Parquet.readleafstream(file, metadata, schema,
                    1, index; expected_rows=rows))
            end
        end
        return (metadata=metadata, schema=schema, streams=streams)
    finally
        close(file)
    end
end

function wnexpectstream(stream::Parquet.LeafStream, repetition,
        definition, values; max_repetition::Integer,
        max_definition::Integer)
    @test stream.repetition == UInt64[repetition...]
    @test stream.definition == UInt64[definition...]
    @test stream.values == values
    @test all(level -> level <= max_repetition, stream.repetition)
    @test all(level -> level <= max_definition, stream.definition)
    @test all(pair -> first(pair) <= last(pair),
        zip(stream.repetition, stream.definition))
    return
end

function wnbytes(values::AbstractVector{<:AbstractString})
    return Vector{UInt8}[collect(codeunits(value)) for value in values]
end

function wnheaders(bytes::Vector{UInt8}, column::Int)
    file = Parquet.File(bytes)
    try
        metadata = Parquet.Thrift.decode(copy(file.footer.bytes),
            WNMD.FileMetaData)
        chunk = only(metadata.row_groups).columns[column].meta_data
        start, stop = Parquet._chunkrange(chunk, file.footer.offset)
        headers = WNMD.PageHeader[]
        position = start
        while position < stop
            frame = Parquet.readpage(file.source, position, stop,
                Parquet.Limits())
            if frame.header.data_page_header !== nothing ||
                    frame.header.data_page_header_v2 !== nothing
                push!(headers, frame.header)
            end
            position = Parquet.pageend(frame)
        end
        return headers
    finally
        close(file)
    end
end

function wncheckpagetype(bytes::Vector{UInt8}, column::Int,
        pageversion::Symbol, entries::Int, rows::Int, nulls::Int)
    headers = wnheaders(bytes, column)
    @test length(headers) == 1
    header = only(headers)
    if pageversion === :v1
        @test header.type_ == WNMD.PageType.DATA_PAGE
        @test header.data_page_header.num_values == entries
    else
        @test header.type_ == WNMD.PageType.DATA_PAGE_V2
        page = header.data_page_header_v2
        @test page.num_values == entries
        @test page.num_rows == rows
        @test page.num_nulls == nulls
    end
    return
end

function wnchecklist(column, expected)
    @test length(column) == length(expected)
    for index in eachindex(expected)
        row = expected[index]
        if ismissing(row)
            @test column[index] === missing
        else
            @test isequal(collect(column[index]), row)
        end
    end
    return
end

@testset "recursive writer optional struct" begin
    S = NamedTuple{(:a,:b),Tuple{Int32,Union{Missing,String}}}
    values = Union{Missing,S}[
        missing,
        S((Int32(1), missing)),
        S((Int32(2), "x")),
    ]
    input = (s=values,)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        result = wninspect(bytes)
        @test [element.name for element in result.metadata.schema] ==
            ["schema", "s", "a", "b"]
        @test result.metadata.schema[1].num_children == 1
        @test result.metadata.schema[2].repetition_type ==
            WNMD.FieldRepetitionType.OPTIONAL
        @test result.metadata.schema[2].num_children == 2
        @test result.metadata.schema[3].type_ == WNMD.Type.INT32
        @test result.metadata.schema[3].repetition_type ==
            WNMD.FieldRepetitionType.REQUIRED
        @test result.metadata.schema[4].type_ == WNMD.Type.BYTE_ARRAY
        @test result.metadata.schema[4].repetition_type ==
            WNMD.FieldRepetitionType.OPTIONAL
        @test result.metadata.schema[4].logicalType.STRING !== nothing
        @test result.metadata.schema[4].converted_type ==
            WNMD.ConvertedType.UTF8
        @test [leaf.path for leaf in result.schema.leaves] ==
            [["s", "a"], ["s", "b"]]
        wnexpectstream(result.streams[1], [0,0,0], [0,1,1],
            Int32[1,2]; max_repetition=0, max_definition=1)
        wnexpectstream(result.streams[2], [0,0,0], [0,1,2],
            wnbytes(["x"]); max_repetition=0, max_definition=2)
        table = Parquet.Table(bytes)
        try
            @test table.columns.s[1] === missing
            @test table.columns.s[2]["a"] == 1
            @test table.columns.s[2]["b"] === missing
            @test table.columns.s[3]["a"] == 2
            @test table.columns.s[3]["b"] == "x"
        finally
            close(table)
        end
    end
end

@testset "recursive writer canonical LIST" begin
    E = Union{Missing,Int32}
    values = Union{Missing,Vector{E}}[
        missing,
        E[],
        E[missing,10,20],
        E[30],
    ]
    input = (items=values,)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        result = wninspect(bytes)
        @test [element.name for element in result.metadata.schema] ==
            ["schema", "items", "list", "element"]
        outer, repeated, element = result.metadata.schema[2:4]
        @test outer.repetition_type == WNMD.FieldRepetitionType.OPTIONAL
        @test outer.logicalType.LIST !== nothing
        @test outer.converted_type == WNMD.ConvertedType.LIST
        @test outer.num_children == 1
        @test repeated.repetition_type == WNMD.FieldRepetitionType.REPEATED
        @test repeated.num_children == 1
        @test repeated.logicalType === nothing
        @test repeated.converted_type === nothing
        @test element.type_ == WNMD.Type.INT32
        @test element.repetition_type == WNMD.FieldRepetitionType.OPTIONAL
        @test only(result.schema.leaves).path ==
            ["items", "list", "element"]
        wnexpectstream(only(result.streams), [0,0,0,1,1,0],
            [0,1,2,3,3,3], Int32[10,20,30];
            max_repetition=1, max_definition=3)
        wncheckpagetype(bytes, 1, pageversion, 6, 4, 3)
        table = Parquet.Table(bytes)
        try
            wnchecklist(table.columns.items, values)
        finally
            close(table)
        end
    end
end

@testset "recursive writer canonical MAP" begin
    values = Parquet.MapVector(
        Int32[0,0,0,2,3],
        String["a","b","c"],
        Union{Missing,Int32}[missing,2,3];
        validity=Bool[false,true,true,true],
    )
    input = (attrs=values,)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        result = wninspect(bytes)
        @test [element.name for element in result.metadata.schema] ==
            ["schema", "attrs", "key_value", "key", "value"]
        outer, entries, key, value = result.metadata.schema[2:5]
        @test outer.repetition_type == WNMD.FieldRepetitionType.OPTIONAL
        @test outer.logicalType.MAP !== nothing
        @test outer.converted_type == WNMD.ConvertedType.MAP
        @test entries.repetition_type == WNMD.FieldRepetitionType.REPEATED
        @test entries.logicalType === nothing
        @test entries.converted_type === nothing
        @test key.type_ == WNMD.Type.BYTE_ARRAY
        @test key.repetition_type == WNMD.FieldRepetitionType.REQUIRED
        @test key.logicalType.STRING !== nothing
        @test value.type_ == WNMD.Type.INT32
        @test value.repetition_type == WNMD.FieldRepetitionType.OPTIONAL
        @test [leaf.path for leaf in result.schema.leaves] == [
            ["attrs", "key_value", "key"],
            ["attrs", "key_value", "value"],
        ]
        wnexpectstream(result.streams[1], [0,0,0,1,0],
            [0,1,2,2,2], wnbytes(["a","b","c"]);
            max_repetition=1, max_definition=2)
        wnexpectstream(result.streams[2], [0,0,0,1,0],
            [0,1,2,3,3], Int32[2,3];
            max_repetition=1, max_definition=3)
        wncheckpagetype(bytes, 1, pageversion, 5, 4, 2)
        wncheckpagetype(bytes, 2, pageversion, 5, 4, 3)
        table = Parquet.Table(bytes)
        try
            column = table.columns.attrs
            @test column[1] === missing
            @test isempty(column[2])
            @test isequal(collect(column[3]),
                Pair{String,Union{Missing,Int32}}["a" => missing, "b" => 2])
            @test collect(column[4]) == ["c" => Int32(3)]
        finally
            close(table)
        end
    end

    DictType = Dict{String,Union{Missing,Int32}}
    dictionaries = DictType[
        DictType(),
        DictType("a" => Int32(1), "b" => missing),
    ]
    table = Parquet.Table(Parquet._encodefile((attrs=dictionaries,)))
    try
        @test isempty(table.columns.attrs[1])
        @test isequal(Dict(table.columns.attrs[2]), dictionaries[2])
    finally
        close(table)
    end
end

@testset "recursive writer LIST of optional structs" begin
    T = NamedTuple{(:id,:label),Tuple{Int32,Union{Missing,String}}}
    E = Union{Missing,T}
    values = Union{Missing,Vector{E}}[
        missing,
        E[],
        E[missing, T((Int32(1), missing)), T((Int32(2), "b"))],
    ]
    input = (rows=values,)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        result = wninspect(bytes)
        @test [element.name for element in result.metadata.schema] ==
            ["schema", "rows", "list", "element", "id", "label"]
        @test result.metadata.schema[4].repetition_type ==
            WNMD.FieldRepetitionType.OPTIONAL
        @test result.metadata.schema[4].num_children == 2
        @test result.metadata.schema[6].logicalType.STRING !== nothing
        @test [leaf.path for leaf in result.schema.leaves] == [
            ["rows", "list", "element", "id"],
            ["rows", "list", "element", "label"],
        ]
        wnexpectstream(result.streams[1], [0,0,0,1,1],
            [0,1,2,3,3], Int32[1,2];
            max_repetition=1, max_definition=3)
        wnexpectstream(result.streams[2], [0,0,0,1,1],
            [0,1,2,3,4], wnbytes(["b"]);
            max_repetition=1, max_definition=4)
        table = Parquet.Table(bytes)
        try
            column = table.columns.rows
            @test column[1] === missing
            @test isempty(column[2])
            @test column[3][1] === missing
            @test column[3][2]["id"] == 1
            @test column[3][2]["label"] === missing
            @test column[3][3]["label"] == "b"
        finally
            close(table)
        end
    end
end

@testset "recursive writer struct with LIST" begin
    E = Union{Missing,Int32}
    R = NamedTuple{
        (:id,:items),
        Tuple{Int32,Union{Missing,Vector{E}}},
    }
    values = Union{Missing,R}[
        missing,
        R((Int32(1), missing)),
        R((Int32(2), E[])),
        R((Int32(3), E[missing,7])),
    ]
    input = (record=values,)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        result = wninspect(bytes)
        @test [element.name for element in result.metadata.schema] == [
            "schema", "record", "id", "items", "list", "element",
        ]
        @test result.metadata.schema[4].logicalType.LIST !== nothing
        @test [leaf.path for leaf in result.schema.leaves] == [
            ["record", "id"],
            ["record", "items", "list", "element"],
        ]
        wnexpectstream(result.streams[1], [0,0,0,0], [0,1,1,1],
            Int32[1,2,3]; max_repetition=0, max_definition=1)
        wnexpectstream(result.streams[2], [0,0,0,0,1],
            [0,1,2,3,4], Int32[7];
            max_repetition=1, max_definition=4)
        table = Parquet.Table(bytes)
        try
            column = table.columns.record
            @test column[1] === missing
            @test column[2]["items"] === missing
            @test isempty(column[3]["items"])
            @test isequal(collect(column[4]["items"]), E[missing,7])
        finally
            close(table)
        end
    end
end

@testset "recursive writer LIST of MAP" begin
    maps = Parquet.MapVector(
        Int32[0,0,2,3],
        String["a","b","c"],
        Union{Missing,Int32}[missing,2,3],
    )
    values = Parquet.ListVector(
        Int32[0,0,0,2,3], maps;
        validity=Bool[false,true,true,true],
    )
    input = (batches=values,)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        result = wninspect(bytes)
        @test [element.name for element in result.metadata.schema] == [
            "schema", "batches", "list", "element", "key_value", "key", "value",
        ]
        @test result.metadata.schema[2].logicalType.LIST !== nothing
        @test result.metadata.schema[4].logicalType.MAP !== nothing
        @test [leaf.path for leaf in result.schema.leaves] == [
            ["batches", "list", "element", "key_value", "key"],
            ["batches", "list", "element", "key_value", "value"],
        ]
        wnexpectstream(result.streams[1], [0,0,0,1,2,0],
            [0,1,2,3,3,3], wnbytes(["a","b","c"]);
            max_repetition=2, max_definition=3)
        wnexpectstream(result.streams[2], [0,0,0,1,2,0],
            [0,1,2,3,4,4], Int32[2,3];
            max_repetition=2, max_definition=4)
        table = Parquet.Table(bytes)
        try
            column = table.columns.batches
            @test column[1] === missing
            @test isempty(column[2])
            @test isempty(column[3][1])
            @test isequal(collect(column[3][2]),
                Pair{String,Union{Missing,Int32}}["a" => missing, "b" => 2])
            @test collect(column[4][1]) == ["c" => Int32(3)]
        finally
            close(table)
        end
    end
end

@testset "recursive writer MAP of optional structs" begin
    structs = Parquet.StructVector(
        ["x","y"],
        (Int32[1,2], Union{Missing,String}[missing,"z"]);
        ranks=Int32[0,0,1,2],
    )
    values = Parquet.MapVector(
        Int32[0,0,0,3], String["a","b","c"], structs;
        validity=Bool[false,true,true],
    )
    input = (objects=values,)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        result = wninspect(bytes)
        @test [element.name for element in result.metadata.schema] == [
            "schema", "objects", "key_value", "key", "value", "x", "y",
        ]
        @test result.metadata.schema[5].repetition_type ==
            WNMD.FieldRepetitionType.OPTIONAL
        @test result.metadata.schema[5].num_children == 2
        wnexpectstream(result.streams[1], [0,0,0,1,1],
            [0,1,2,2,2], wnbytes(["a","b","c"]);
            max_repetition=1, max_definition=2)
        wnexpectstream(result.streams[2], [0,0,0,1,1],
            [0,1,2,3,3], Int32[1,2];
            max_repetition=1, max_definition=3)
        wnexpectstream(result.streams[3], [0,0,0,1,1],
            [0,1,2,3,4], wnbytes(["z"]);
            max_repetition=1, max_definition=4)
        table = Parquet.Table(bytes)
        try
            column = table.columns.objects
            @test column[1] === missing
            @test isempty(column[2])
            @test column[3][1].second === missing
            @test column[3][2].second["x"] == 1
            @test column[3][2].second["y"] === missing
            @test column[3][3].second["y"] == "z"
        finally
            close(table)
        end
    end
end

@testset "recursive writer nested LIST" begin
    Scalar = Union{Missing,Int32}
    Inner = Vector{Scalar}
    OuterElement = Union{Missing,Inner}
    Outer = Vector{OuterElement}
    values = Union{Missing,Outer}[
        missing,
        OuterElement[],
        OuterElement[
            missing,
            Scalar[],
            Scalar[missing,1,2],
            Scalar[3],
        ],
    ]
    input = (nested=values,)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pageversion=pageversion)
        result = wninspect(bytes)
        @test [element.name for element in result.metadata.schema] == [
            "schema", "nested", "list", "element", "list", "element",
        ]
        @test result.metadata.schema[2].logicalType.LIST !== nothing
        @test result.metadata.schema[4].logicalType.LIST !== nothing
        wnexpectstream(only(result.streams), [0,0,0,1,1,2,2,1],
            [0,1,2,3,4,5,5,5], Int32[1,2,3];
            max_repetition=2, max_definition=5)
        wncheckpagetype(bytes, 1, pageversion, 8, 3, 5)
        table = Parquet.Table(bytes)
        try
            column = table.columns.nested
            @test column[1] === missing
            @test isempty(column[2])
            @test column[3][1] === missing
            @test isempty(column[3][2])
            @test isequal(collect(column[3][3]), Scalar[missing,1,2])
            @test collect(column[3][4]) == Int32[3]
        finally
            close(table)
        end
    end
end

@testset "recursive writer empty, all-null, and zero-row states" begin
    S = NamedTuple{(:a,:b),Tuple{Int32,Union{Missing,String}}}
    E = Union{Missing,Int32}
    zero = (
        s=Union{Missing,S}[],
        items=Union{Missing,Vector{E}}[],
        attrs=Union{Missing,Dict{String,Union{Missing,Int32}}}[],
    )
    for pageversion in (:v1, :v2)
        result = wninspect(Parquet._encodefile(zero;
            pageversion=pageversion))
        @test result.metadata.num_rows == 0
        @test isempty(result.metadata.row_groups)
        @test isempty(result.streams)
        @test result.metadata.schema[1].num_children == 3
        @test [element.name for element in result.metadata.schema] == [
            "schema", "s", "a", "b", "items", "list", "element",
            "attrs", "key_value", "key", "value",
        ]
    end

    nullstructs = Union{Missing,S}[missing,missing,missing]
    result = wninspect(Parquet._encodefile((s=nullstructs,)))
    for stream in result.streams
        @test stream.repetition == UInt64[0,0,0]
        @test stream.definition == UInt64[0,0,0]
        @test isempty(stream.values)
    end

    Present = NamedTuple{
        (:a,:b),
        Tuple{Union{Missing,Int32},Union{Missing,String}},
    }
    present = Present[Present((missing,missing)), Present((missing,missing))]
    result = wninspect(Parquet._encodefile((s=present,)))
    for (index, stream) in enumerate(result.streams)
        @test stream.repetition == UInt64[0,0]
        @test stream.definition == UInt64[0,0]
        @test result.schema.leaves[index].max_definition_level == 1
        @test isempty(stream.values)
    end

    emptylists = Vector{Int32}[Int32[],Int32[]]
    result = wninspect(Parquet._encodefile((items=emptylists,)))
    wnexpectstream(only(result.streams), [0,0], [0,0], Int32[];
        max_repetition=1, max_definition=1)

    emptymaps = Dict{String,Int32}[Dict{String,Int32}(), Dict{String,Int32}()]
    result = wninspect(Parquet._encodefile((attrs=emptymaps,)))
    for stream in result.streams
        wnexpectstream(stream, [0,0], [0,0], eltype(stream.values)[];
            max_repetition=1, max_definition=1)
    end
end

@testset "recursive writer duplicate struct names and selectors" begin
    values = Parquet.StructVector(
        ["x","x"], (Int32[1,2], String["a","b"]))
    input = (s=values,)
    bytes = Parquet._encodefile(input)
    result = wninspect(bytes)
    @test [element.name for element in result.metadata.schema] ==
        ["schema", "s", "x", "x"]
    @test [leaf.path for leaf in result.schema.leaves] ==
        [["s", "x"], ["s", "x"]]
    wnexpectstream(result.streams[1], [0,0], [0,0], Int32[1,2];
        max_repetition=0, max_definition=0)
    wnexpectstream(result.streams[2], [0,0], [0,0], wnbytes(["a","b"]);
        max_repetition=0, max_definition=0)
    table = Parquet.Table(bytes)
    try
        @test table.columns.s[1][1] == 1
        @test table.columns.s[1][2] == "a"
        @test_throws ArgumentError table.columns.s[1]["x"]
    finally
        close(table)
    end

    @test_throws ArgumentError Parquet._encodefile(input;
        encoding=((:s,:x) => :plain))
    selected = Parquet._encodefile(input; encoding=Dict{Any,Any}(
        1 => :delta_binary_packed,
        2 => :delta_byte_array,
    ))
    selectedmetadata = wninspect(selected).metadata
    @test WNMD.Encoding.DELTA_BINARY_PACKED in
        selectedmetadata.row_groups[1].columns[1].meta_data.encodings
    @test WNMD.Encoding.DELTA_BYTE_ARRAY in
        selectedmetadata.row_groups[1].columns[2].meta_data.encodings
end

@testset "recursive writer duplicate MAP keys" begin
    values = Parquet.MapVector(
        Int32[0,3], String["k","k","z"], Int32[1,2,3])
    bytes = Parquet._encodefile((m=values,))
    result = wninspect(bytes)
    wnexpectstream(result.streams[1], [0,1,1], [1,1,1],
        wnbytes(["k","k","z"]); max_repetition=1,
        max_definition=1)
    wnexpectstream(result.streams[2], [0,1,1], [1,1,1],
        Int32[1,2,3]; max_repetition=1, max_definition=1)
    table = Parquet.Table(bytes)
    try
        value = table.columns.m[1]
        @test collect(value) == ["k" => Int32(1), "k" => Int32(2),
            "z" => Int32(3)]
        @test Parquet.maplookup(value, "k") == 2
        @test Dict(value)["k"] == 2
    finally
        close(table)
    end
end

function wnlogicalpayload(values)
    return Parquet.StructVector(
        ["enum","time","timestamp","decimal"], values)
end

function wnchecklogicalschema(metadata::WNMD.FileMetaData)
    @test [element.name for element in metadata.schema] == [
        "schema", "logicals", "list", "element", "enum", "time",
        "timestamp", "decimal",
    ]
    enum, time, timestamp, decimal = metadata.schema[5:8]
    @test enum.type_ == WNMD.Type.BYTE_ARRAY
    @test enum.logicalType.ENUM !== nothing
    @test enum.converted_type == WNMD.ConvertedType.ENUM
    @test time.type_ == WNMD.Type.INT64
    @test time.logicalType.TIME.unit.MICROS !== nothing
    @test time.logicalType.TIME.isAdjustedToUTC
    @test time.converted_type == WNMD.ConvertedType.TIME_MICROS
    @test timestamp.type_ == WNMD.Type.INT64
    @test timestamp.logicalType.TIMESTAMP.unit.NANOS !== nothing
    @test !timestamp.logicalType.TIMESTAMP.isAdjustedToUTC
    @test timestamp.converted_type === nothing
    @test decimal.type_ == WNMD.Type.FIXED_LEN_BYTE_ARRAY
    @test decimal.type_length == 9
    @test decimal.precision == 20
    @test decimal.scale == 4
    @test decimal.logicalType.DECIMAL.precision == 20
    @test decimal.logicalType.DECIMAL.scale == 4
    @test decimal.converted_type == WNMD.ConvertedType.DECIMAL
    return
end

@testset "nested local millisecond timestamp compatibility annotation" begin
    input = (events=[(at=DateTime(1970, 1, 1),)],)
    metadata = wninspect(Parquet._encodefile(input)).metadata
    timestamp = only(filter(element -> element.name == "at", metadata.schema))
    @test timestamp.logicalType.TIMESTAMP.unit.MILLIS !== nothing
    @test !timestamp.logicalType.TIMESTAMP.isAdjustedToUTC
    @test timestamp.converted_type == WNMD.ConvertedType.TIMESTAMP_MILLIS
end

@testset "recursive writer explicit logical leaves" begin
    missingvalues = (
        Parquet.LogicalColumn(Missing[missing,missing], :enum),
        Parquet.LogicalColumn(Missing[missing,missing], :time;
            unit=:micros, adjusted=true),
        Parquet.LogicalColumn(Missing[missing,missing], :timestamp;
            unit=:nanos, adjusted=false),
        Parquet.LogicalColumn(Missing[missing,missing], :decimal;
            precision=20, scale=4),
    )
    input = (logicals=Parquet.ListVector(
        Int32[0,0,2], wnlogicalpayload(missingvalues)),)
    for pageversion in (:v1, :v2)
        result = wninspect(Parquet._encodefile(input;
            pageversion=pageversion))
        wnchecklogicalschema(result.metadata)
        for stream in result.streams
            wnexpectstream(stream, [0,0,1], [0,1,1],
                eltype(stream.values)[]; max_repetition=1,
                max_definition=2)
        end
    end

    presentvalues = (
        Parquet.LogicalColumn(Union{Missing,String}["alpha"], :enum),
        Parquet.LogicalColumn(Union{Missing,Time}[Time(0)], :time;
            unit=:micros, adjusted=true),
        Parquet.LogicalColumn(
            Union{Missing,Parquet.Timestamp{:nanos}}[
                Parquet.Timestamp(7, :nanos, false),
            ], :timestamp; unit=:nanos, adjusted=false),
        Parquet.LogicalColumn(Union{Missing,Parquet.Decimal}[
            Parquet.Decimal(12345, 4),
        ], :decimal; precision=20, scale=4),
    )
    present = (logicals=Parquet.ListVector(
        Int32[0,1], wnlogicalpayload(presentvalues)),)
    result = wninspect(Parquet._encodefile(present))
    wnchecklogicalschema(result.metadata)
    for stream in result.streams
        @test stream.repetition == UInt64[0]
        @test stream.definition == UInt64[2]
    end
    @test result.streams[1].values == wnbytes(["alpha"])
    @test result.streams[2].values == Int64[0]
    @test result.streams[3].values == Int64[7]
    @test result.streams[4].values == [
        UInt8[0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x30,0x39],
    ]
    table = Parquet.Table(Parquet._encodefile(present))
    try
        value = table.columns.logicals[1][1]
        @test value["enum"] == "alpha"
        @test value["time"] == Time(0)
        @test value["timestamp"] == Parquet.Timestamp(7, :nanos, false)
        @test value["decimal"] == Parquet.Decimal(12345, 4)
    finally
        close(table)
    end
end

@testset "recursive writer nested codec and encoding matrix" begin
    C = NamedTuple{
        (:flag,:i32,:i64,:f32,:f64,:text,:fixed),
        Tuple{Bool,Int32,Int64,Float32,Float64,String,NTuple{2,UInt8}},
    }
    entries = C[
        C((isodd(index), Int32(index % 7), Int64(index % 11),
            Float32(index % 5), Float64(index % 13),
            "value-$(index % 4)", (UInt8(index % 4), UInt8(0x7f))))
        for index in 1:128
    ]
    input = (records=Vector{C}[entries],)
    codecs = (
        :uncompressed,
        :snappy,
        :gzip,
        :brotli,
        :zstd,
        :lz4_raw,
    )
    for pageversion in (:v1, :v2), codec in codecs
        bytes = Parquet._encodefile(input; pageversion=pageversion,
            codec=codec)
        table = Parquet.Table(bytes)
        try
            records = table.columns.records[1]
            @test length(records) == length(entries)
            @test records[1]["i32"] == entries[1].i32
            @test records[64]["text"] == entries[64].text
            @test records[end]["fixed"] == collect(entries[end].fixed)
        finally
            close(table)
        end
        metadata = wninspect(bytes).metadata
        expectedcodec = Parquet._writecodec(codec)
        @test all(chunk.meta_data.codec == expectedcodec for chunk in
            only(metadata.row_groups).columns)
    end

    paths = Dict{Any,Any}(
        (:records,:list,:element,:flag) => :rle,
        (:records,:list,:element,:i32) => :delta_binary_packed,
        (:records,:list,:element,:i64) => :delta_binary_packed,
        (:records,:list,:element,:f32) => :byte_stream_split,
        (:records,:list,:element,:f64) => :byte_stream_split,
        (:records,:list,:element,:text) => :delta_byte_array,
        (:records,:list,:element,:fixed) => :delta_byte_array,
    )
    encodings = (
        WNMD.Encoding.RLE,
        WNMD.Encoding.DELTA_BINARY_PACKED,
        WNMD.Encoding.DELTA_BINARY_PACKED,
        WNMD.Encoding.BYTE_STREAM_SPLIT,
        WNMD.Encoding.BYTE_STREAM_SPLIT,
        WNMD.Encoding.DELTA_BYTE_ARRAY,
        WNMD.Encoding.DELTA_BYTE_ARRAY,
    )
    for pageversion in (:v1, :v2)
        metadata = wninspect(Parquet._encodefile(input;
            pageversion=pageversion, encoding=paths)).metadata
        for (chunk, encoding) in zip(only(metadata.row_groups).columns,
                encodings)
            @test encoding in chunk.meta_data.encodings
        end
        dictionary = wninspect(Parquet._encodefile(input;
            pageversion=pageversion,
            encoding=((:records,:list,:element,:text) => :dictionary),
        )).metadata
        textchunk = only(dictionary.row_groups).columns[6].meta_data
        @test textchunk.dictionary_page_offset !== nothing
        @test WNMD.Encoding.RLE_DICTIONARY in textchunk.encodings
    end
end

@testset "recursive writer malformed inputs" begin
    Empty = NamedTuple{(),Tuple{}}
    @test_throws ArgumentError Parquet._encodefile((value=Empty[Empty(())],))
    @test_throws ArgumentError Parquet._encodefile((value=
        Parquet.StructVector(String[], (); rows=1),))

    Required = NamedTuple{(:x,),Tuple{Int32}}
    required = WNDeclaredVector{Required}(Any[missing])
    @test_throws ArgumentError Parquet._encodefile((value=required,))

    badmaps = WNBadMap[WNBadMap()]
    @test_throws ArgumentError Parquet._encodefile((value=badmaps,))
    nullablekeys = Dict{Union{Missing,String},Int32}[
        Dict{Union{Missing,String},Int32}("x" => Int32(1)),
    ]
    @test_throws ArgumentError Parquet._encodefile((value=nullablekeys,))

    @test_throws ArgumentError Parquet._encodefile((value=Any[Int32[1]],))
    abstractlists = AbstractVector{Int32}[Int32[1]]
    @test_throws ArgumentError Parquet._encodefile((value=abstractlists,))
    heterogeneous = Union{Vector{Int32},Vector{String}}[Int32[1], String["x"]]
    @test_throws ArgumentError Parquet._encodefile((value=heterogeneous,))
    unparameterized = Vector[Int32[1]]
    @test_throws ArgumentError Parquet._encodefile((value=unparameterized,))

    @test_throws ArgumentError Parquet._encodefile(
        (items=Vector{Int32}[Int32[1]],);
        encoding=((:items,:list,:element) => :delta_byte_array))
    @test_throws ArgumentError Parquet._encodefile(
        (items=Vector{Int32}[Int32[1]],);
        encoding=((:missing,:list,:element) => :plain))
end

@testset "recursive writer resource preflight and arbitrary axes" begin
    for limits in (
            Parquet.Limits(max_container_elements=1),
            Parquet.Limits(max_materialized_bytes=1024),
        )
        source = WNTrackedRows{Base.HasLength}(Ref(0), 100_000)
        budget = Parquet._LiveByteBudget(limits)
        @test_throws Parquet.LimitError Parquet._writefields(source,
            limits, budget)
        @test source.consumed[] == 0
        @test Parquet._budgetused(budget) == 0
    end

    unknown = WNTrackedRows{Base.SizeUnknown}(Ref(0), 100_000)
    unknownlimits = Parquet.Limits(max_container_elements=1)
    unknownbudget = Parquet._LiveByteBudget(unknownlimits)
    @test_throws Parquet.LimitError Parquet._writefields(unknown,
        unknownlimits, unknownbudget)
    @test unknown.consumed[] == 2
    @test Parquet._budgetused(unknownbudget) == 0

    schemaless = WNSchemaLessRows([
        (value=Int32(1), items=Int32[1,2]),
        (value=Int32(2), items=Int32[]),
    ])
    table = Parquet.Table(Parquet._encodefile(schemaless))
    try
        @test table.columns.value == Int32[1,2]
        @test collect(table.columns.items[1]) == Int32[1,2]
        @test isempty(table.columns.items[2])
    finally
        close(table)
    end
    unnamed = WNSchemaLessRows(NTuple{3,Int32}[])
    unnamedlimits = Parquet.Limits(max_materialized_bytes=1,
        max_container_elements=1)
    unnamedbudget = Parquet._LiveByteBudget(unnamedlimits)
    @test_throws ArgumentError Parquet._writefields(unnamed,
        unnamedlimits, unnamedbudget)
    @test Parquet._budgetused(unnamedbudget) == 0

    wide = WNSchemaLessRows(
        NamedTuple{(:left,:right),Tuple{Int32,Int32}}[])
    widebudget = Parquet._LiveByteBudget(unnamedlimits)
    @test_throws Parquet.LimitError Parquet._writefields(wide,
        unnamedlimits, widebudget)
    @test Parquet._budgetused(widebudget) == 0

    for size in (Base.HasLength, Base.SizeUnknown)
        fresh = WNFreshStringRows{size}(Ref(0), 20, 100_000)
        freshlimits = Parquet.Limits(max_materialized_bytes=100_000)
        freshbudget = Parquet._LiveByteBudget(freshlimits)
        @test_throws Parquet.LimitError Parquet._writefields(fresh,
            freshlimits, freshbudget)
        @test fresh.consumed[] == 1
        @test Parquet._budgetused(freshbudget) == 0
    end

    for (type, maker) in (
            (Parquet.JSONValue, wnfreshjson),
            (Parquet.BSONValue, wnfreshbson),
            (Parquet.Decimal, wnfreshdecimal),
        )
        fresh = WNFreshPayloadRows{type,typeof(maker)}(
            Ref(0), 20, 100_000, maker)
        freshlimits = Parquet.Limits(max_materialized_bytes=200_000)
        freshbudget = Parquet._LiveByteBudget(freshlimits)
        @test_throws Parquet.LimitError Parquet._writefields(fresh,
            freshlimits, freshbudget)
        @test fresh.consumed[] == 2
        @test Parquet._budgetused(freshbudget) == 0
    end

    UnionType = Union{Int32,Float32}
    @test Parquet._materializedarraybytes(UnionType, 4; header=false) ==
        4 * (Base.elsize(Vector{UnionType}) + 1)

    tracked = WNTrackedBytes(fill(UInt8(1), 100_000), Ref(0))
    pagelimits = Parquet.Limits(max_page_bytes=1)
    payloadbudget = Parquet._LiveByteBudget(pagelimits)
    @test_throws Parquet.LimitError Parquet._writefields(
        (value=WNTrackedBytes[tracked],), pagelimits, payloadbudget)
    @test tracked.reads[] == 0
    @test Parquet._budgetused(payloadbudget) == 0

    large = repeat("y", 100_000)
    wnmutationattack(large)
    @test @allocated(wnmutationattack(large)) < 50_000

    scalar = WNZeroVector(Int32[10,20])
    table = Parquet.Table(Parquet._encodefile((value=scalar,)))
    try
        @test table.columns.value == Int32[10,20]
    finally
        close(table)
    end

    nested = WNZeroVector([
        WNZeroVector(Int32[1,2]),
        WNZeroVector(Int32[]),
    ])
    table = Parquet.Table(Parquet._encodefile((value=nested,)))
    try
        @test collect(table.columns.value[1]) == Int32[1,2]
        @test isempty(table.columns.value[2])
    finally
        close(table)
    end
end

@testset "ordinary writer iterative topology and exact rollback" begin
    depth = 4096
    exactlimits = Parquet.Limits(max_metadata_depth=depth + 2,
        max_container_elements=100_000,
        max_materialized_bytes=256 * 1024 * 1024)
    sources = (wndeepstruct(depth, 0), wndeepstruct(depth, 1))
    for (rows, source) in enumerate(sources)
        expectedrows = rows - 1
        budget = Parquet._LiveByteBudget(exactlimits)
        fields, written = Parquet._writefields((deep=source,), exactlimits,
            budget)
        field = only(fields)
        leaf = only(field.leaves)
        @test written == expectedrows
        @test length(field.schema) == depth + 1
        @test length(leaf.path) == depth + 1
        @test leaf.values == (iszero(expectedrows) ? Int32[] : Int32[7])
    end

    precharge = Int64(257)
    failurelimits = Parquet.Limits(max_metadata_depth=depth + 1,
        max_container_elements=100_000,
        max_materialized_bytes=256 * 1024 * 1024)
    failurebudget = Parquet._LiveByteBudget(failurelimits)
    Parquet._reserve!(failurebudget, precharge)
    caught = try
        Parquet._writefields((deep=last(sources),), failurelimits,
            failurebudget)
        nothing
    catch err
        err
    end
    @test caught isa Parquet.LimitError
    @test caught.resource == :metadata_depth
    @test caught.requested == depth + 2
    @test caught.maximum == depth + 1
    @test Parquet._budgetused(failurebudget) == precharge
    Parquet._release!(failurebudget, precharge)

    frame = Parquet._NestedWriteSourceFrame
    headercharge = Parquet._materializedsum(
        Parquet._materializedarraybytes(frame, 0),
        Parquet._MATERIALIZED_OBJECT_BYTES)
    framecharge = Parquet._materializedarraybytes(frame, 1; header=false)
    traversalcharge = headercharge + 2 * framecharge
    framesource = Parquet.StructVector(String["value"],
        AbstractVector[Int32[]]; rows=0)
    lowlimits = Parquet.Limits(
        max_materialized_bytes=precharge + traversalcharge - 1)
    lowbudget = Parquet._LiveByteBudget(lowlimits)
    Parquet._reserve!(lowbudget, precharge)
    caught = try
        Parquet._nestedwritevalidatesource(framesource, lowlimits, lowbudget)
        nothing
    catch err
        err
    end
    @test caught isa Parquet.LimitError
    @test caught.resource == :materialized_bytes
    @test caught.requested == precharge + traversalcharge
    @test caught.maximum == precharge + traversalcharge - 1
    @test Parquet._budgetused(lowbudget) == precharge
    Parquet._release!(lowbudget, precharge)

    exactframelimits = Parquet.Limits(
        max_materialized_bytes=precharge + traversalcharge)
    exactframebudget = Parquet._LiveByteBudget(exactframelimits)
    Parquet._reserve!(exactframebudget, precharge)
    Parquet._nestedwritevalidatesource(framesource, exactframelimits,
        exactframebudget)
    @test Parquet._budgetused(exactframebudget) == precharge
    Parquet._release!(exactframebudget, precharge)

    precedencecharge = headercharge + framecharge
    precedencesource = Parquet.StructVector(String["first", "second"],
        AbstractVector[Int32[], Int32[]]; rows=0)
    precedencelimits = Parquet.Limits(max_metadata_depth=1,
        max_materialized_bytes=precharge + precedencecharge)
    precedencebudget = Parquet._LiveByteBudget(precedencelimits)
    Parquet._reserve!(precedencebudget, precharge)
    caught = try
        Parquet._nestedwritevalidatesource(precedencesource,
            precedencelimits, precedencebudget)
        nothing
    catch err
        err
    end
    @test caught isa Parquet.LimitError
    @test caught.resource == :metadata_depth
    @test caught.requested == 2
    @test caught.maximum == 1
    @test Parquet._budgetused(precedencebudget) == precharge
    Parquet._release!(precedencebudget, precharge)

    sentinel = ErrorException("writer topology callback sentinel")
    backing = WNThrowingSizeVector(Int32[7], 0, 4, sentinel)
    view = Parquet.ListValue(backing, 1, 1)
    views = WNDeclaredVector{Parquet.ListValue{Int32}}(Any[view])
    callbacklimits = Parquet.Limits()
    callbackbudget = Parquet._LiveByteBudget(callbacklimits)
    Parquet._reserve!(callbackbudget, precharge)
    caught = try
        Parquet._writefields((deep=views,), callbacklimits, callbackbudget)
        nothing
    catch err
        err
    end
    @test caught === sentinel
    @test backing.calls == 4
    @test Parquet._budgetused(callbackbudget) == precharge
    Parquet._release!(callbackbudget, precharge)
end

@testset "ordinary writer reuses pass scratch stacks" begin
    rows = 1000
    structs = Parquet.StructVector(String["value"],
        AbstractVector[fill(Int32(1), rows)]; rows=rows)
    lists = Parquet.ListVector(collect(Int32(0):Int32(rows)),
        fill(Int32(1), rows))
    structscan, structrows = wnwriterpassallocations(structs)
    listscan, listrows = wnwriterpassallocations(lists)
    # These bounds catch a pass that stops reusing its scratch stacks, which costs
    # far more than a constant per row. Keep them loose: the same measurement runs
    # about a seventh higher on x86-64 than on arm64, and varies by Julia version.
    @test structscan < 1800 * rows
    @test structrows < 3300 * rows
    @test listscan < 4200 * rows
    @test listrows < 6300 * rows

    limits = Parquet.Limits()
    budget = Parquet._LiveByteBudget(limits)
    shape = Parquet._nestedwriteshape("value", Int32, Int32[], limits,
        budget)
    fragment = Parquet._nestedwriteschema(shape, limits, budget)
    semantic, plans = Parquet._nestedwritecompile(
        Parquet._NestedWriteShape[shape],
        Vector{Parquet.Metadata.SchemaElement}[fragment], limits, budget)
    counts = Parquet._nestedwritecounts(length(semantic.leaves), budget)
    context = Parquet._NestedWriteCountContext(counts, limits)
    plan = only(plans)::Parquet._NestedWriteLeafPlan
    Parquet._nestedwritescanaggregate!(shape, Int32(1), limits)
    Parquet._nestedwriteshred!(context, plan, Int32(1), UInt64(0), nothing)
    @test @allocated(Parquet._nestedwritescanaggregate!(shape, Int32(1),
        limits)) == 0
    @test @allocated(Parquet._nestedwriteshred!(context, plan, Int32(1),
        UInt64(0), nothing)) == 0

    precharge = Int64(31)
    actiontype = Parquet._NestedWriteScanAction
    headercharge = Parquet._materializedsum(
        Parquet._materializedarraybytes(actiontype, 0),
        Parquet._MATERIALIZED_OBJECT_BYTES)
    framecharge = Parquet._materializedarraybytes(actiontype, 1;
        header=false)
    lowlimits = Parquet.Limits(
        max_materialized_bytes=precharge + headercharge + framecharge - 1)
    lowbudget = Parquet._LiveByteBudget(lowlimits)
    Parquet._reserve!(lowbudget, precharge)
    lowstack = Parquet._nestedwritepassstackstart(actiontype, lowbudget)
    action = Parquet._nestedwritescanenter(shape, Int32(1), nothing)
    caught = try
        Parquet._nestedwritestackpush!(lowstack, action, lowbudget)
        nothing
    catch err
        err
    end
    @test caught isa Parquet.LimitError
    @test caught.requested == precharge + headercharge + framecharge
    @test Parquet._budgetused(lowbudget) == precharge + headercharge
    Parquet._nestedwritepassstackrelease!(lowstack, lowbudget)
    @test Parquet._budgetused(lowbudget) == precharge
    Parquet._release!(lowbudget, precharge)

    exactlimits = Parquet.Limits(
        max_materialized_bytes=precharge + headercharge + framecharge)
    exactbudget = Parquet._LiveByteBudget(exactlimits)
    Parquet._reserve!(exactbudget, precharge)
    exactstack = Parquet._nestedwritepassstackstart(actiontype, exactbudget)
    Parquet._nestedwritestackpush!(exactstack, action, exactbudget)
    Parquet._nestedwritepassstackpop!(exactstack)
    Parquet._nestedwritepassstackprocessed!(exactstack)
    @test Parquet._budgetused(exactbudget) ==
        precharge + headercharge + framecharge
    Parquet._nestedwritepassstackrelease!(exactstack, exactbudget)
    @test Parquet._budgetused(exactbudget) == precharge
    Parquet._release!(exactbudget, precharge)
end
