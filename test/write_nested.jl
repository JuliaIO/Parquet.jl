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

function wndeepstruct(depth::Int, rows::Int)
    values::AbstractVector = iszero(rows) ? Int32[] : Int32[7]
    for level in depth:-1:1
        childname = level == depth ? "value" : "level_$(level + 1)"
        values = Parquet.StructVector(String[childname],
            AbstractVector[values]; rows=rows)
    end
    return values
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

# Scalar columns are read once, so a source that changes between reads still
# writes one consistent column.
function wnchangingsource(large::String)
    source = WNChangingStrings(Ref(0), "x", large)
    table = Parquet.Table(Parquet._encodefile((value=source,)))
    values = try
        String.(collect(Parquet.Tables.getcolumn(table, :value)))
    finally
        close(table)
    end
    return source.calls[], length(values), values
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
            @test Parquet.Tables.columns(table).s[1] === missing
            @test Parquet.Tables.columns(table).s[2][:a] == 1
            @test Parquet.Tables.columns(table).s[2][:b] === missing
            @test Parquet.Tables.columns(table).s[3][:a] == 2
            @test Parquet.Tables.columns(table).s[3][:b] == "x"
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
            wnchecklist(Parquet.Tables.columns(table).items, values)
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
            column = Parquet.Tables.columns(table).attrs
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
        @test isempty(Parquet.Tables.columns(table).attrs[1])
        @test isequal(Dict(Parquet.Tables.columns(table).attrs[2]), dictionaries[2])
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
            column = Parquet.Tables.columns(table).rows
            @test column[1] === missing
            @test isempty(column[2])
            @test column[3][1] === missing
            @test column[3][2][:id] == 1
            @test column[3][2][:label] === missing
            @test column[3][3][:label] == "b"
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
            column = Parquet.Tables.columns(table).record
            @test column[1] === missing
            @test column[2][:items] === missing
            @test isempty(column[3][:items])
            @test isequal(collect(column[4][:items]), E[missing,7])
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
            column = Parquet.Tables.columns(table).batches
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
        (Int32[0,1,2], Union{Missing,String}[missing,missing,"z"]);
        validity=Bool[false,true,true],
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
            column = Parquet.Tables.columns(table).objects
            @test column[1] === missing
            @test isempty(column[2])
            @test column[3][1].second === missing
            @test column[3][2].second.x == 1
            @test column[3][2].second.y === missing
            @test column[3][3].second.y == "z"
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
            column = Parquet.Tables.columns(table).nested
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
        # Duplicate field names read back as Pair rows.
        @test Parquet.Tables.columns(table).s[1][1] == ("x" => 1)
        @test Parquet.Tables.columns(table).s[1][2] == ("x" => "a")
        @test_throws ArgumentError Parquet.Tables.columns(table).s[1]["x"]
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
        value = Parquet.Tables.columns(table).m[1]
        @test collect(value) == ["k" => Int32(1), "k" => Int32(2),
            "z" => Int32(3)]
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
            Union{Missing,Parquet.Durations.Timestamp{Dates.Nanosecond}}[
                pqstamp(7, :nanos, false),
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
        value = Parquet.Tables.columns(table).logicals[1][1]
        @test value.enum == "alpha"
        @test value.time == Time(0)
        @test value.timestamp == pqstamp(7, :nanos, false)
        @test value.decimal == decimalexpected(Parquet.Decimal(12345, 4))
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
            records = Parquet.Tables.columns(table).records[1]
            @test length(records) == length(entries)
            @test records[1][:i32] == entries[1].i32
            @test records[64][:text] == entries[64].text
            @test records[end][:fixed] == entries[end].fixed
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

    # Abstractly typed list columns are narrowed from their values.
    for lists in (Any[Int32[1]], AbstractVector{Int32}[Int32[1]], Vector[Int32[1]])
        table = Parquet.Table(Parquet._encodefile((value=lists,)))
        try
            @test collect(only(table.value)) == Int32[1]
            @test Parquet._tablemetadata(table).schema[end].type_ == WNMD.Type.INT32
        finally
            close(table)
        end
    end
    heterogeneous = Union{Vector{Int32},Vector{String}}[Int32[1], String["x"]]
    @test_throws "mixes values of types Int32 and String" Parquet._encodefile(
        (value=heterogeneous,))

    @test_throws ArgumentError Parquet._encodefile(
        (items=Vector{Int32}[Int32[1]],);
        encoding=((:items,:list,:element) => :delta_byte_array))
    @test_throws ArgumentError Parquet._encodefile(
        (items=Vector{Int32}[Int32[1]],);
        encoding=((:missing,:list,:element) => :plain))
end

@testset "recursive writer resource preflight and arbitrary axes" begin
    rowlimits = Parquet.Limits(max_container_elements=1)
    source = WNTrackedRows{Base.HasLength}(Ref(0), 100_000)
    @test_throws Parquet.LimitError Parquet._encodefile(source; limits=rowlimits)
    @test source.consumed[] == 0

    unknown = WNTrackedRows{Base.SizeUnknown}(Ref(0), 100_000)
    @test_throws Parquet.LimitError Parquet._encodefile(unknown; limits=rowlimits)
    @test unknown.consumed[] == 2

    schemaless = WNSchemaLessRows([
        (value=Int32(1), items=Int32[1,2]),
        (value=Int32(2), items=Int32[]),
    ])
    table = Parquet.Table(Parquet._encodefile(schemaless))
    try
        @test Parquet.Tables.columns(table).value == Int32[1,2]
        @test collect(Parquet.Tables.columns(table).items[1]) == Int32[1,2]
        @test isempty(Parquet.Tables.columns(table).items[2])
    finally
        close(table)
    end
    unnamed = WNSchemaLessRows(NTuple{3,Int32}[])
    @test_throws ArgumentError Parquet._encodefile(unnamed; limits=rowlimits)

    wide = WNSchemaLessRows(
        NamedTuple{(:left,:right),Tuple{Int32,Int32}}[])
    @test_throws Parquet.LimitError Parquet._encodefile(wide; limits=rowlimits)

    # Row values are checked as they are collected, so the first oversized row
    # stops the read.
    valuelimits = Parquet.Limits(max_string_bytes=50_000,
        max_decimal_bytes=50_000)
    for size in (Base.HasLength, Base.SizeUnknown)
        fresh = WNFreshStringRows{size}(Ref(0), 20, 100_000)
        @test_throws Parquet.LimitError Parquet._encodefile(fresh; limits=valuelimits)
        @test fresh.consumed[] == 1
    end

    for (type, maker) in (
            (Parquet.JSONValue, wnfreshjson),
            (Parquet.BSONValue, wnfreshbson),
            (Parquet.Decimal, wnfreshdecimal),
        )
        fresh = WNFreshPayloadRows{type,typeof(maker)}(
            Ref(0), 20, 100_000, maker)
        @test_throws Parquet.LimitError Parquet._encodefile(fresh; limits=valuelimits)
        @test fresh.consumed[] == 1
    end

    tracked = WNTrackedBytes(fill(UInt8(1), 100_000), Ref(0))
    pagelimits = Parquet.Limits(max_page_bytes=1)
    @test_throws Parquet.LimitError Parquet._encodefile(
        (value=WNTrackedBytes[tracked],); limits=pagelimits)

    calls, rows, values = wnchangingsource(repeat("y", 100_000))
    @test calls == 1
    @test rows == 1
    @test values == ["x"]

    scalar = WNZeroVector(Int32[10,20])
    table = Parquet.Table(Parquet._encodefile((value=scalar,)))
    try
        @test Parquet.Tables.columns(table).value == Int32[10,20]
    finally
        close(table)
    end

    nested = WNZeroVector([
        WNZeroVector(Int32[1,2]),
        WNZeroVector(Int32[]),
    ])
    table = Parquet.Table(Parquet._encodefile((value=nested,)))
    try
        @test collect(Parquet.Tables.columns(table).value[1]) == Int32[1,2]
        @test isempty(Parquet.Tables.columns(table).value[2])
    finally
        close(table)
    end
end

@testset "ordinary writer deep structs" begin
    depth = 4096
    exactlimits = Parquet.Limits(max_metadata_depth=depth + 2,
        max_container_elements=100_000)
    sources = (wndeepstruct(depth, 0), wndeepstruct(depth, 1))
    for (rows, source) in enumerate(sources)
        expectedrows = rows - 1
        bytes = Parquet._encodefile((deep=source,); limits=exactlimits)
        # The reader declines a schema this deep, so check the footer itself.
        footerlength = Int(reinterpret(UInt32, bytes[(end - 7):(end - 4)])[1])
        metadata = Parquet.Thrift.decode(bytes[(end - 7 - footerlength):(end - 8)],
            WNMD.FileMetaData; limits=exactlimits)
        @test metadata.num_rows == expectedrows
        @test length(metadata.schema) == depth + 2
        if !iszero(expectedrows)
            chunk = only(only(metadata.row_groups).columns).meta_data
            @test length(chunk.path_in_schema) == depth + 1
            @test chunk.num_values == 1
        end
    end

    failurelimits = Parquet.Limits(max_metadata_depth=depth + 1,
        max_container_elements=100_000)
    caught = try
        Parquet._encodefile((deep=last(sources),); limits=failurelimits)
        nothing
    catch err
        err
    end
    @test caught isa Parquet.LimitError
    @test caught.resource == :metadata_depth
    @test caught.requested == depth + 2
    @test caught.maximum == depth + 1
end

@testset "MAP values infer DECIMAL and TIMESTAMP annotations" begin
    decimals = Dict{String,Parquet.Decimal}[
        Dict("k" => Parquet.Decimal(Int128(1234), Int32(2)))]
    stamps = Dict{String,Parquet.Durations.ZonedTimestamp{Dates.Microsecond,:UTC}}[
        Dict("k" => pqstamp(5, :micros, true))]
    views = Parquet.MapVector(Int32[0, 1], String["k"],
        Parquet.Decimal[Parquet.Decimal(Int128(-56789), Int32(3))])
    for (source, check) in (
            (decimals, logical -> logical.DECIMAL.precision == 4 &&
                logical.DECIMAL.scale == 2),
            (stamps, logical -> logical.TIMESTAMP.isAdjustedToUTC),
            (views, logical -> logical.DECIMAL.precision == 5 &&
                logical.DECIMAL.scale == 3))
        bytes = Parquet._encodefile((m=source,))
        value = only(filter(element -> element.name == "value",
            wninspect(bytes).metadata.schema))
        @test check(value.logicalType)
        table = Parquet.Table(bytes)
        try
            plain(x::Parquet.DataDecimals.AbstractDecimal) = Parquet.Decimal(
                BigInt(Parquet.DataDecimals.unscaled(x)),
                Parquet.DataDecimals.scale(x))
            plain(x::AbstractString) = String(x)
            plain(x) = x
            @test isequal(
                [plain(pair.first) => plain(pair.second)
                    for pair in only(Parquet.Tables.columns(table).m)],
                [pair.first => pair.second for pair in only(source)])
        finally
            close(table)
        end
    end
end

@testset "MAP sources: struct keys, duplicate keys, null and empty rows" begin
    keys = Parquet.StructVector(["optional"],
        [Union{Missing,Int32}[missing, Int32(2)]])
    maps = Parquet.MapVector(Int32[0, 2], keys, Int32[10, 20])
    for version in (:v1, :v2)
        bytes = Parquet._encodefile((m=maps,); checksum=false,
            pageversion=version, pageindex=false)
        table = Parquet.Table(bytes)
        try
            pairs = collect(Parquet.Tables.columns(table).m[1])
            @test length(pairs) == 2
            @test pairs[1].first[1] === missing
            @test pairs[2].first[1] == Int32(2)
        finally
            close(table)
        end
    end

    duplicate = Parquet.MapVector(Int32[0, 2], Int32[1, 1], Int32[10, 11])
    table = Parquet.Table(Parquet._encodefile((m=duplicate,)))
    try
        @test collect(Parquet.Tables.columns(table).m[1]) ==
            [Int32(1) => Int32(10), Int32(1) => Int32(11)]
    finally
        close(table)
    end

    values = Parquet.MapVector(Int32[0, 0, 0, 2, 3], String["a", "b", "c"],
        Union{Missing,Int32}[missing, 2, 3];
        validity=Bool[false, true, true, true])
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile((attrs=values,);
            pageversion=pageversion, checksum=false, pageindex=false)
        reread = Parquet.Table(bytes)
        try
            @test Parquet.Tables.columns(reread).attrs[1] === missing
            @test isempty(Parquet.Tables.columns(reread).attrs[2])
            @test isequal(collect(Parquet.Tables.columns(reread).attrs[3]),
                Pair{String,Union{Missing,Int32}}[
                    "a" => missing, "b" => Int32(2)])
            @test collect(Parquet.Tables.columns(reread).attrs[4]) == ["c" => Int32(3)]
        finally
            close(reread)
        end
    end
end

# Plain Julia values with the same contents as a read-back nested value.
wnplain(value) = value
wnplain(value::AbstractString) = String(value)
wnplain(value::Union{AbstractVector,Tuple}) = Any[wnplain(item) for item in value]
wnplain(value::Union{AbstractDict,AbstractVector{<:Pair}}) =
    Pair[wnplain(key) => wnplain(item) for (key, item) in value]
wnplain(value::NamedTuple) = map(wnplain, value)

function wnroundtrip(column; kwargs...)
    table = Parquet.Table(Parquet._encodefile((value=column,); kwargs...))
    return collect(table.value), Parquet._tablemetadata(table).schema
end

@testset "plain Julia nested shapes" begin
    Inner = NamedTuple{(:x, :t),Tuple{Int,NTuple{2,Int}}}
    Element = NamedTuple{(:a, :m),Tuple{Union{Missing,Vector{Union{Missing,Int}}},
        Union{Missing,Vector{Pair{String,Union{Missing,Inner}}}}}}
    deep = Union{Missing,Vector{Union{Missing,Element}}}[
        i % 7 == 0 ? missing : Union{Missing,Element}[(i + j) % 5 == 0 ? missing :
            Element((i % 3 == 0 ? missing : Union{Missing,Int}[i, missing],
                i % 4 == 0 ? missing : Pair{String,Union{Missing,Inner}}["k$j" =>
                    (isodd(i) ? missing : Inner((i, (i, j))))]))
            for j in 1:(i % 3)] for i in 1:30]
    cases = Any[
        [[1, 2], Int[], [3]],
        Union{Missing,Vector{Union{Missing,String}}}[["a", missing], missing, String[]],
        [view([1, 2, 3], 1:2), view([4, 5], 2:2)],
        [1:2, 3:3, 1:0],
        [(1, 2), (3, 4)],
        Tuple{Int,Union{Missing,Int}}[(1, missing), (2, 3)],
        Union{Missing,NTuple{2,String}}[("a", "b"), missing],
        [[(1, 2)], Tuple{Int,Int}[]],
        [["a" => 1, "b" => 2], Pair{String,Int}[], ["a" => 3, "a" => 4]],
        Union{Missing,Vector{Pair{String,Union{Missing,Int}}}}[["a" => missing], missing],
        [Dict("a" => [1, 2]), Dict{String,Vector{Int}}()],
        [(x=1, y=(2, 3)), (x=4, y=(5, 6))],
        deep,
    ]
    for column in cases, version in (:v1, :v2)
        values, _ = wnroundtrip(column; pageversion=version)
        @test isequal(wnplain.(values), wnplain.(column))
    end

    # Tuples are lists; pair vectors and dictionaries are maps.
    _, schema = wnroundtrip([(1, 2)])
    @test schema[2].converted_type == WNMD.ConvertedType.LIST
    _, schema = wnroundtrip([["a" => 1]])
    @test schema[2].converted_type == WNMD.ConvertedType.MAP

    # Abstract columns narrow to one element type, missing included.
    for column in (Any[["a" => 1], ["b" => missing]], Any[(1, missing), (2, 3)],
            Any[Dict("a" => 1), Dict("b" => missing)], Any[[1, 2], 3:4], Any[deep...])
        values, _ = wnroundtrip(column)
        @test isequal(wnplain.(values), wnplain.(column))
    end

    @test_throws "need one element type" Parquet._encodefile((value=[(1, "a")],))
    @test_throws "fixed, nonzero number" Parquet._encodefile((value=[()],))
    @test_throws "MAP key types cannot include Missing" Parquet._encodefile(
        (value=[Pair{Union{Missing,String},Int}["a" => 1]],))
    @test_throws "mixes values" Parquet._encodefile((value=Any[(1, 2), (1, 2, 3)],))
end
