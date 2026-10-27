using Dates
using Tables
import DataAPI
import Random

# A caller's source that records each read and how many run at once.
mutable struct HandleTrackingSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    concurrent::Bool
    delay::Float64
    lock::ReentrantLock
    calls::Vector{Tuple{Int64,Int64}}
    inflight::Int
    maxinflight::Int
    closes::Int
end

function HandleTrackingSource(bytes::Vector{UInt8}; concurrent::Bool=true, delay=0.0)
    return HandleTrackingSource(bytes, concurrent, delay, ReentrantLock(),
        Tuple{Int64,Int64}[], 0, 0, 0)
end

Parquet.sourcelength(src::HandleTrackingSource) = Int64(length(src.bytes))
Parquet.concurrentreads(src::HandleTrackingSource) = src.concurrent
Parquet.close!(src::HandleTrackingSource) = (src.closes += 1; nothing)

function Parquet.readrange(src::HandleTrackingSource, offset::Integer, count::Integer)
    lock(src.lock) do
        push!(src.calls, (Int64(offset), Int64(count)))
        src.inflight += 1
        src.maxinflight = max(src.maxinflight, src.inflight)
    end
    src.delay > 0 && sleep(src.delay)
    lock(() -> src.inflight -= 1, src.lock)
    return src.bytes[(Int(offset) + 1):(Int(offset) + Int(count))]
end

handlebytes(table; kw...) = Parquet._encodefile(table; kw...)

@testset "read keywords" begin
    days = Date(2025, 1, 1) .+ Day.(0:9)
    bytes = handlebytes((a=collect(1:10), b=[string(i) for i in 1:10],
        c=Union{Missing,Float64}[isodd(i) ? missing : i / 2 for i in 1:10], d=days))
    table(; kw...) = Parquet.Table(bytes; kw...)
    @test Tables.columnnames(table(select=[:c, :a])) == (:c, :a)
    @test Tables.columnnames(table(select=:b)) == (:b,)
    @test Tables.columnnames(table(drop=[:b, :d])) == (:a, :c)
    @test Tables.columnnames(table(drop=r"[bc]")) == (:a, :d)
    @test table(filter=Parquet.col(:a) > 7).a == [8, 9, 10]
    @test table(filter=Parquet.col(:a) > 2, limit=3).a == [3, 4, 5]
    @test table(limit=2, select=[:b]).b == ["1", "2"]
    @test table(filter=Parquet.colcmp(>=, Parquet.col(:d), Date(2025, 1, 9))).a == [9, 10]
    @test table(filter=Parquet.colin(Parquet.col(:b), ("2", "5"))).a == [2, 5]
    @test table(filter=Parquet.isnull(Parquet.col(:c))).a == [1, 3, 5, 7, 9]
    # Filter columns are read but not returned unless selected.
    @test Tables.columnnames(table(select=[:b], filter=Parquet.col(:a) > 8)) == (:b,)
    @test_throws ArgumentError table(select=[:a], scan=Tables.Scan())
    @test_throws ArgumentError table(limit=1, scan=Tables.Scan())
    @test_throws ArgumentError table(select=[:a], drop=[:b])
    @test Parquet.col === Tables.col && Parquet.colcmp === Tables.colcmp
    @test Parquet.colin === Tables.colin && Parquet.isnull === Tables.isnull
end

@testset "Table releases its file" begin
    mktempdir() do dir
        path = joinpath(dir, "t.parquet")
        Parquet.write(path, (a=collect(1:5), s=["x", "yy", "a string past twelve bytes",
            "z", "w"]))
        table = Parquet.Table(path)
        # The file is unmapped and closed, so it can be replaced while the table lives
        # (Windows refuses to replace or delete a mapped file).
        Parquet.write(path, table)
        @test Parquet.Table(path).s == table.s
        rm(path)
        @test table.a == 1:5
        @test table.s[3] == "a string past twelve bytes"
        close(table)
        @test table.a == 1:5

        Parquet.write(path, (a=[1, 2],))
        file = Parquet.File(path)
        @test only(collect(file)).a == [1, 2]
        close(file)
        close(file)
        @test_throws ArgumentError file[1]
        rm(path)
    end
end

@testset "File handles" begin
    n = 10
    bytes = handlebytes((a=collect(1:n), b=[string(i) for i in 1:n],
        c=Union{Missing,Float64}[isodd(i) ? missing : i / 2 for i in 1:n]);
        rowgroupsize=4)
    file = Parquet.File(bytes)
    @test sprint(show, file) == "Parquet.File(10 rows, 3 columns, 3 row groups)"
    @test length(file) == 3
    @test Tables.partitions(file) === file
    @test Tables.istable(file)
    @test [Tables.rowcount(table) for table in file] == [4, 4, 2]
    @test file[2].a == 5:8
    @test_throws BoundsError file[4]
    @test DataAPI.nrow(file) == n
    @test Tables.schema(file) == Tables.Schema((:a, :b, :c),
        (Int64, Parquet.DataStrings.DataString, Union{Missing,Float64}))
    @test Tables.columnnames(file) == (:a, :b, :c)
    @test Tables.columns(file).a == 1:n
    @test Parquet.metadata(file) === file.metadata
    @test Parquet.metadata(file).num_rows == n
    close(file)

    # Keywords shape every table the file yields.
    selected = Parquet.File(bytes; scan=Tables.Scan(select=[:c => :half, :a]))
    @test Tables.columnnames(selected) == (:half, :a)
    @test Tables.schema(selected).types == (Union{Missing,Float64}, Int64)
    @test isequal(selected[1].half, [missing, 1.0, missing, 2.0])
    windowed = Parquet.File(bytes; scan=Tables.Scan(offset=3, limit=5))
    @test length(windowed) == 2
    @test [table.a for table in windowed] == [[4], [5, 6, 7, 8]]
    @test DataAPI.nrow(windowed) == 5
    @test windowed[2].b == ["5", "6", "7", "8"]
    filtered = Parquet.File(bytes; filter=Parquet.col(:a) > 1, limit=6)
    @test [table.a for table in filtered] == [[2, 3, 4], [5, 6, 7]]
    @test_throws ArgumentError length(filtered)
    @test_throws ArgumentError filtered[1]
    @test_throws ArgumentError DataAPI.nrow(filtered)
    @test_throws ArgumentError DataAPI.nrow(Parquet.File(bytes; filter=Parquet.col(:a) > 1))
    # Row groups whose statistics rule the filter out are skipped.
    @test [table.a for table in Parquet.File(bytes;
        filter=(Parquet.col(:a) < 1) | (Parquet.col(:a) > 8))] == [[9, 10]]

    # Key-value metadata through DataAPI; a repeated key reads as its last value.
    metadata = Parquet.File(bytes).metadata
    entries = [Parquet.Metadata.KeyValue(key="origin", value="test"),
        Parquet.Metadata.KeyValue(key="origin", value="last"),
        Parquet.Metadata.KeyValue(key="empty", value=nothing)]
    tagged = tablerewritefooter(bytes, tablereplace(metadata; key_value_metadata=entries))
    for x in (Parquet.File(tagged), Parquet.Table(tagged))
        @test DataAPI.metadatasupport(typeof(x)) == (read=true, write=false)
        @test DataAPI.metadatakeys(x) == ["origin", "empty"]
        @test DataAPI.metadata(x, "origin") == "last"
        @test DataAPI.metadata(x, "origin"; style=true) == ("last", :default)
        @test DataAPI.metadata(x, "empty") === nothing
        @test DataAPI.metadata(x, "missing", 0) == 0
        @test_throws KeyError DataAPI.metadata(x, "missing")
        @test DataAPI.metadata(x) == Dict("origin" => "last", "empty" => nothing)
    end
    @test DataAPI.metadatakeys(Parquet.Table(bytes)) == String[]
end

@testset "Parquet.read" begin
    bytes = handlebytes((a=[1, 2, 3], b=["x", "y", "z"]))
    @test Parquet.read(bytes, identity) isa Tables.CopiedColumns
    columns = Parquet.read(bytes, Tables.columntable; select=[:b])
    @test columns == (b=["x", "y", "z"],)
    @test_throws ArgumentError Parquet.read(bytes)
end

@testset "AbstractSource reads" begin
    # A small file: the footer comes from one read of the whole file, and the chunks
    # a read needs from one more.
    small = handlebytes((a=collect(1:100), b=collect(1.0:100.0)))
    src = HandleTrackingSource(small)
    table = Parquet.Table(src)
    @test table.a == 1:100
    @test first(src.calls) == (0, length(small))
    @test length(src.calls) == 2
    @test src.closes == 0

    # Column chunks far apart are separate reads, run concurrently when the source
    # allows it; a large file's footer is still one read from its end.
    rng = Random.Xoshiro(1)
    rows = 50_000
    big = handlebytes((a=rand(rng, Int64, rows), b=rand(rng, Int64, rows),
        c=rand(rng, Int64, rows)))
    expected = Parquet.Table(big; select=[:a, :c])
    for (concurrent, inflight) in ((true, 2), (false, 1))
        src = HandleTrackingSource(big; concurrent, delay=0.2)
        table = Parquet.Table(src; select=[:a, :c], ntasks=2)
        @test table.a == expected.a && table.c == expected.c
        @test length(src.calls) == 3
        footer = first(src.calls)
        @test footer[1] + footer[2] == length(big) && footer[2] < length(big)
        @test src.maxinflight == inflight
        @test src.closes == 0
    end

    # A file handle over a caller's source reads only what each row group needs and
    # leaves the source open.
    src = HandleTrackingSource(handlebytes((a=collect(1:9),); rowgroupsize=3))
    file = Parquet.File(src)
    reads = length(src.calls)
    @test Tables.columnnames(file) == (:a,)
    @test length(src.calls) == reads
    @test file[3].a == 7:9
    @test length(src.calls) == reads + 1
    close(file)
    @test src.closes == 0
end
