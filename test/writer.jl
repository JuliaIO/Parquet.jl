using Test
using Dates
import Tables
import DataAPI
using PooledArrays, CategoricalArrays
import SentinelArrays

const WRMD = Parquet.Metadata

function wrmetadata(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    try
        return Parquet.Thrift.decode(copy(file.footer.bytes), WRMD.FileMetaData)
    finally
        close(file)
    end
end

function wrcolumn(bytes::Vector{UInt8}, name::Symbol)
    table = Parquet.Table(bytes)
    try
        return collect(Tables.getcolumn(Tables.columns(table), name))
    finally
        close(table)
    end
end

# A table with DataAPI file and column metadata.
struct WRMetaTable
    columns::NamedTuple
    metadata::Dict{String,String}
    colmetadata::Dict{Symbol,Dict{String,String}}
end
Tables.istable(::Type{WRMetaTable}) = true
Tables.columnaccess(::Type{WRMetaTable}) = true
Tables.columns(table::WRMetaTable) = table.columns
DataAPI.metadatasupport(::Type{WRMetaTable}) = (read=true, write=false)
DataAPI.metadatakeys(table::WRMetaTable) = keys(table.metadata)
DataAPI.metadata(table::WRMetaTable, key::AbstractString; style::Bool=false) =
    table.metadata[key]
DataAPI.colmetadatasupport(::Type{WRMetaTable}) = (read=true, write=false)
DataAPI.colmetadatakeys(table::WRMetaTable, column::Symbol) =
    keys(get(table.colmetadata, column, Dict{String,String}()))
DataAPI.colmetadata(table::WRMetaTable, column::Symbol, key::AbstractString;
    style::Bool=false) = table.colmetadata[column][key]

@testset "Writer appends tables as row groups" begin
    io = IOBuffer()
    writer = Parquet.Writer(io; rowgroupsize=2)
    @test Parquet.write(writer, (id=[1, 2, 3], name=["a", "b", "c"])) === writer
    # Later tables convert exactly; columns are nullable by default.
    Parquet.write(writer, (id=Int32[4], name=[missing]))
    @test occursin("4 rows in 3 row groups", sprint(show, writer))
    close(writer)
    close(writer)
    bytes = take!(io)
    metadata = wrmetadata(bytes)
    @test [group.num_rows for group in metadata.row_groups] == [2, 1, 1]
    @test all(element -> element.repetition_type == WRMD.FieldRepetitionType.OPTIONAL,
        metadata.schema[2:end])
    @test wrcolumn(bytes, :id) == [1, 2, 3, 4]
    @test isequal(wrcolumn(bytes, :name), ["a", "b", "c", missing])
    @test_throws "closed" Parquet.write(writer, (id=[5], name=["e"]))
    @test_throws "Int64(1.5)" Parquet.Writer(IOBuffer()) do w
        Parquet.write(w, (id=[1],))
        Parquet.write(w, (id=[1.5],))
    end
    # Widening is exact: an Int64 that a Float64 cannot hold, or a Float64 that a
    # Float32 cannot, is an error rather than a rounded value.
    @test_throws "convert(Float64, 9007199254740993)" Parquet.Writer(IOBuffer()) do w
        Parquet.write(w, (x=[1.5],))
        Parquet.write(w, (x=[2^53 + 1],))
    end
    @test_throws "convert(Float32, 0.1)" Parquet.Writer(IOBuffer()) do w
        Parquet.write(w, (x=Float32[1],))
        Parquet.write(w, (x=[0.1],))
    end
    widened = take!(Parquet.Writer(IOBuffer()) do w
        Parquet.write(w, (x=[1.5], y=Float32[1], z=[1]))
        Parquet.write(w, (x=Int32[2, 3], y=[0.5, NaN], z=Int8[3, 4]))
    end)
    @test isequal(wrcolumn(widened, :x), [1.5, 2.0, 3.0])
    @test isequal(wrcolumn(widened, :y), Float32[1, 0.5, NaN])
    @test wrcolumn(widened, :z) == [1, 3, 4]
    @test_throws "but the file's column 1 is" Parquet.Writer(IOBuffer()) do w
        Parquet.write(w, (id=[1],))
        Parquet.write(w, (other=[1],))
    end
    @test_throws "no schema" close(Parquet.Writer(IOBuffer()))
    # nullable=false keeps the first table's nullability.
    @test_throws "missing values but its Parquet field is required" Parquet.Writer(
            IOBuffer(); nullable=false) do w
        Parquet.write(w, (id=[1],))
        Parquet.write(w, (id=[missing],))
    end
end

@testset "Writer to a path replaces the file only when it closes" begin
    mktempdir() do directory
        path = joinpath(directory, "out.parquet")
        write(path, "previous")
        @test Parquet.Writer(path) do writer
            Parquet.write(writer, (value=[1, 2],))
            @test read(path, String) == "previous"
        end == path
        @test wrcolumn(read(path), :value) == [1, 2]
        @test_throws ErrorException Parquet.Writer(path) do writer
            Parquet.write(writer, (value=[3],))
            error("stop")
        end
        @test wrcolumn(read(path), :value) == [1, 2]
        @test readdir(directory) == ["out.parquet"]
    end
end

@testset "write metadata keywords and DataAPI defaults" begin
    table = WRMetaTable((id=[1, 2], name=["a", "b"]),
        Dict("source" => "test", "ARROW:schema" => "stale"),
        Dict(:name => Dict("unit" => "none")))
    metadata = wrmetadata(take!(Parquet.write(IOBuffer(), table)))
    @test [(item.key, item.value) for item in metadata.key_value_metadata[1:1]] ==
        [("source", "test")]
    @test [item.key for item in metadata.key_value_metadata] == ["source", "ARROW:schema"]
    chunks = metadata.row_groups[1].columns
    @test chunks[1].meta_data.key_value_metadata === nothing
    @test [(item.key, item.value) for item in chunks[2].meta_data.key_value_metadata] ==
        [("unit", "none")]
    explicit = wrmetadata(take!(Parquet.write(IOBuffer(), (id=[1],);
        metadata=Dict("a" => 1), colmetadata=(id=("b" => "c",),))))
    @test [(item.key, item.value) for item in explicit.key_value_metadata[1:1]] ==
        [("a", "1")]
    @test [(item.key, item.value) for item in
        explicit.row_groups[1].columns[1].meta_data.key_value_metadata] == [("b", "c")]
    @test_throws "reserved" Parquet.write(IOBuffer(), (id=[1],);
        metadata=Dict("ARROW:schema" => "x"))
    @test_throws "not a column" Parquet.write(IOBuffer(), (id=[1],);
        colmetadata=(other=("b" => "c",),))
end

@testset "pooled columns write dictionary pages from their pool" begin
    pooled = PooledArray(repeat(["b", "a", missing, "c"], 50))
    category = categorical(repeat(["red", missing, "blue"], 67)[1:200])
    integers = PooledArray(repeat([30, 10, 20, 10], 50))
    bytes = take!(Parquet.write(IOBuffer(), (pooled=pooled, category=category,
        integers=integers)))
    @test isequal(wrcolumn(bytes, :pooled), collect(pooled))
    @test isequal(wrcolumn(bytes, :category),
        [ismissing(value) ? missing : String(value) for value in category])
    @test wrcolumn(bytes, :integers) == collect(integers)
    metadata = wrmetadata(bytes)
    for (chunk, (lo, hi)) in zip(metadata.row_groups[1].columns,
            (("a", "c"), ("blue", "red"), (10, 30)))
        meta = chunk.meta_data
        @test meta.dictionary_page_offset !== nothing
        @test WRMD.Encoding.RLE_DICTIONARY in meta.encodings
        bounds = (meta.statistics.min_value, meta.statistics.max_value)
        if lo isa Integer
            @test reinterpret(Int64, bounds[1])[1] == lo
            @test reinterpret(Int64, bounds[2])[1] == hi
        else
            @test String(copy(bounds[1])) == lo && String(copy(bounds[2])) == hi
        end
    end
    # Pooled columns follow `dictionary` and explicit encodings like any other.
    plain = wrmetadata(take!(Parquet.write(IOBuffer(), (pooled=pooled,); dictionary=false)))
    @test plain.row_groups[1].columns[1].meta_data.dictionary_page_offset === nothing
end

@testset "deprecated write keyword names" begin
    table = (value=[1, 2, 3],)
    @test (@test_deprecated Parquet.write(IOBuffer(), table; codec=:zstd)) isa IOBuffer
    @test wrmetadata(take!(Parquet.write(IOBuffer(), table; codec=:zstd,
        compressionlevel=5))).row_groups[1].columns[1].meta_data.codec ==
        WRMD.CompressionCodec.ZSTD
end

# Every page's ColumnIndex bounds and null counts agree with the page's rows.
function wrcheckcolumnindex(bytes::Vector{UInt8}, values::AbstractVector, column::Int,
        decode)
    file = Parquet.File(bytes)
    metadata = Parquet.Thrift.decode(copy(file.footer.bytes), WRMD.FileMetaData)
    offset = 0
    for group in metadata.row_groups
        chunk = group.columns[column]
        index = Parquet.Thrift.decode(bytes[(chunk.column_index_offset + 1):(chunk.column_index_offset + chunk.column_index_length)], WRMD.ColumnIndex)
        locations = Parquet.Thrift.decode(bytes[(chunk.offset_index_offset + 1):(chunk.offset_index_offset + chunk.offset_index_length)], WRMD.OffsetIndex).page_locations
        starts = [location.first_row_index for location in locations]
        stops = vcat(starts[2:end], group.num_rows)
        for (page, (start, stop)) in enumerate(zip(starts, stops))
            rows = values[(offset + start + 1):(offset + stop)]
            present = filter(value -> !ismissing(value) &&
                !(value isa AbstractFloat && isnan(value)), rows)
            @test index.null_counts[page] == count(ismissing, rows)
            @test index.null_pages[page] == all(ismissing, rows)
            isempty(present) && continue
            @test decode(index.min_values[page]) == minimum(present)
            @test decode(index.max_values[page]) == maximum(present)
        end
        offset += group.num_rows
    end
    close(file)
    return
end

@testset "ColumnIndex bounds match each page" begin
    rows = 50_000
    integers = [isodd(row) ? missing : (row * 7919) % 1000 - 500 for row in 1:rows]
    floats = [row % 9 == 0 ? NaN : sin(row) for row in 1:rows]
    strings = ["s$(lpad(row % 997, 4, '0'))" for row in 1:rows]
    sorted = collect(1:rows)
    table = (integers=integers, floats=floats, strings=strings, sorted=sorted)
    for options in ((;), (pageversion=:v2, compress=:zstd), (dictionary=false,
            rowgroupsize=17_000))
        bytes = take!(Parquet.write(IOBuffer(), table; options...))
        int(bytes) = reinterpret(Int64, bytes)[1]
        wrcheckcolumnindex(bytes, integers, 1, int)
        wrcheckcolumnindex(bytes, floats, 2, bytes -> reinterpret(Float64, bytes)[1])
        wrcheckcolumnindex(bytes, strings, 3, bytes -> String(copy(bytes)))
        wrcheckcolumnindex(bytes, sorted, 4, int)
        metadata = wrmetadata(bytes)
        file = Parquet.File(bytes)
        chunk = metadata.row_groups[1].columns[4]
        index = Parquet.Thrift.decode(bytes[(chunk.column_index_offset + 1):(chunk.column_index_offset + chunk.column_index_length)], WRMD.ColumnIndex)
        @test index.boundary_order == WRMD.BoundaryOrder.ASCENDING
        close(file)
    end
end

@testset "hybrid streams round trip at every bit width" begin
    for bitwidth in 1:32, count in (1, 7, 8, 9, 100, 1001)
        limit = bitwidth == 32 ? typemax(UInt32) : UInt32(1) << bitwidth - 1
        # Random values with some runs long enough for RLE.
        values = UInt32[]
        while length(values) < count
            value = rand(UInt32(0):limit)
            append!(values, fill(value, rand((1, 1, 1, 3, 12))))
        end
        resize!(values, count)
        bytes = Parquet._encodehybrid!(UInt8[], values, bitwidth)
        decoded, position = Parquet.decode_hybrid(bytes, count, bitwidth)
        @test decoded == values
        @test position == length(bytes) + 1
    end
    # An all-present page's definition levels are one RLE run.
    run = Parquet._encodelevelrun!(UInt8[], 20, UInt16(1), 1, true)
    @test run == Parquet._encodelevels!(UInt8[], fill(UInt16(1), 20), 1, true)
end

@testset "dictionary probe keeps repeated values and skips distinct ones" begin
    rows = 20_000
    encodings(values) = only(wrmetadata(take!(Parquet.write(IOBuffer(), (v=values,))
        )).row_groups).columns[1].meta_data.encodings
    distinct = collect(1:rows) .* 7919
    @test !(WRMD.Encoding.RLE_DICTIONARY in encodings(distinct))
    @test WRMD.Encoding.RLE_DICTIONARY in encodings([distinct[mod1(i, 2500)] for i in 1:rows])
    @test WRMD.Encoding.RLE_DICTIONARY in encodings([distinct[cld(i, 4)] for i in 1:rows])
    @test !(WRMD.Encoding.RLE_DICTIONARY in encodings(["s$i" for i in distinct]))
    @test WRMD.Encoding.RLE_DICTIONARY in encodings(["s$(i % 3000)" for i in 1:rows])
end

@testset "the generated ARROW:schema carries time zones and durations" begin
    P = Dates.Microsecond
    zone = Symbol("America/New_York")
    stamps(Z) = [Parquet.Durations.ZonedTimestamp{P,Z}(
        Parquet.Durations.Timestamp{P}(Dates.UTInstant(P(1_700_000_000_000_000 + i))),
        Dates.UTC) for i in 1:3]
    table = (ny=stamps(zone), utc=stamps(:UTC), ms=Dates.Millisecond.(1:3),
        lists=[stamps(zone)[1:i] for i in 1:3], plain=[1, 2, 3])
    bytes = take!(Parquet.write(IOBuffer(), table; metadata=Dict("k" => "v")))
    @test [item.key for item in wrmetadata(bytes).key_value_metadata] ==
        ["k", "ARROW:schema"]
    readback = Parquet.Table(bytes)
    # The reader turns the entry into column types instead of user metadata.
    @test DataAPI.metadatakeys(readback) == ["k"]
    @test "ARROW:schema" in [item.key for item in
        Parquet.metadata(readback).key_value_metadata]
    @test eltype(readback.ny) == Parquet.Durations.ZonedTimestamp{P,zone}
    @test readback.ny == table.ny
    @test eltype(readback.utc) == Parquet.Durations.ZonedTimestamp{P,:UTC}
    @test eltype(readback.ms) == Dates.Millisecond
    @test collect(readback.lists[3]) == table.lists[3]
    @test eltype(readback.plain) == Int64
    # A Table rewrite generates the schema again rather than copying it.
    rewritten = Parquet.Table(take!(Parquet.write(IOBuffer(), readback)))
    @test eltype(rewritten.ny) == Parquet.Durations.ZonedTimestamp{P,zone}
    @test collect(rewritten.lists[2]) == table.lists[2]
    @test eltype(rewritten.ms) == Dates.Millisecond
end

@testset "a map without values is a list of keys in the ARROW:schema" begin
    corpus = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))
    source = Parquet.Table(joinpath(corpus, "data", "map_no_value.parquet"))
    bytes = take!(Parquet.write(IOBuffer(), source))
    entry = last(wrmetadata(bytes).key_value_metadata)
    @test entry.key == "ARROW:schema"
    fields = Parquet._decodearrowschema(entry.value, Parquet.Limits())
    types = Dict(field.name => field.type for field in fields)
    # pyarrow reads such a map as list<key>; an Arrow Map would need a value field.
    @test types["my_map_no_v"] == 0x0c
    @test types["my_map"] == 0x11
    @test isequal(collect(Parquet.Table(bytes).my_map_no_v), collect(source.my_map_no_v))
end

@testset "0.8 read_parquet and write_parquet forward with a deprecation" begin
    mktempdir() do directory
        path = joinpath(directory, "old.parquet")
        @test (@test_deprecated write_parquet(path, (a=[1, 2],);
            compression_codec="ZSTD")) == path
        table = @test_deprecated read_parquet(path)
        @test table.a == [1, 2]
        file = Parquet.File(path)
        @test only(only(Parquet.metadata(file).row_groups).columns
            ).meta_data.codec == WRMD.CompressionCodec.ZSTD
        close(file)
    end
end

@testset "STRING and ENUM values must be UTF-8" begin
    bad = String(UInt8[0x61, 0xff])
    sink() = IOBuffer()
    invalid(column, row) = "column \"$column\" has a string at row $row that is not valid UTF-8"
    @test_throws invalid("s", 3) Parquet.write(sink(), (s=["a", "b", bad],))
    @test_throws "write binary data as Vector{UInt8} values" Parquet.write(sink(), (s=[bad],))
    @test_throws invalid("s", 3) Parquet.write(sink(),
        (s=Union{Missing,String}["a", missing, bad],))
    @test_throws invalid("s", 5) Parquet.write(sink(), (s=["a", "b", "c", "d", bad],);
        rowgroupsize=2)
    @test_throws invalid("s", 10) Parquet.write(sink(), (s=[fill("ok", 9); bad],);
        pagesize=10)
    @test_throws invalid("s", 3) Parquet.write(sink(), (s=repeat(["a", "b", bad], 1000),))
    @test_throws invalid("s", 2) Parquet.write(sink(), (s=PooledArray(["a", bad, "a"]),))
    @test_throws invalid("s", 4) Parquet.write(sink(),
        (s=SentinelArrays.ChainedVector([["a", "b"], ["c", bad]]),))
    @test_throws invalid("s", 2) Parquet.write(sink(), (s=[:a, Symbol(bad)],))
    @test_throws invalid("s", 2) Parquet.write(sink(), (s=['a', '\xff'],))
    @test_throws invalid("value", 2) Parquet.write(sink(),
        (value=Parquet.LogicalColumn(String["ok", bad], :enum),))
    @test_throws "column \"s\" (field \"s.list.element\") has a string at row 2" Parquet.write(
        sink(), (s=[["a"], ["b", bad]],))
    @test_throws "column \"s\" (field \"s.x\") has a string at row 2" Parquet.write(sink(),
        (s=[(x="a",), (x=bad,)],))
    @test_throws "column \"s\" (field \"s.key_value.key\") has a string at row 2" Parquet.write(
        sink(), (s=[Dict("a" => 1), Dict(bad => 2)],))
    # Rows count from the start of the table given to this call, across its partitions.
    writer = Parquet.Writer(sink())
    Parquet.write(writer, (s=["a", "b"],))
    @test_throws invalid("s", 2) Parquet.write(writer, (s=["c", bad],))
    @test_throws invalid("s", 4) Parquet.write(sink(),
        Tables.partitioner([(s=["a", "b"],), (s=["c", bad],)]))
    mktempdir() do directory
        @test_throws invalid("s", 4) Parquet.write(joinpath(directory, "data"),
            (k=[1, 2, 1, 2], s=["a", "b", "c", bad]); partitionby=:k)
    end
    text = [repeat("é", 200), "ü"^3, "abc", ""]
    # A 200-byte value's length prefix has its high bit set.
    ascii = [repeat("x", 200), "abc", "", "d"]
    table = Parquet.Table(take!(Parquet.write(sink(),
        (s=text, a=ascii, p=PooledArray(text)))))
    try
        @test table.s == text
        @test table.a == ascii
        @test table.p == text
    finally
        close(table)
    end
end
