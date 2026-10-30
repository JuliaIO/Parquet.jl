struct ShiftedExactBytes <: AbstractVector{UInt8}
    bytes::Vector{UInt8}
end

struct UnitRangeExactBytes <: AbstractVector{UInt8}
    bytes::Vector{UInt8}
end

function Base.IndexStyle(::Type{ShiftedExactBytes})
    return IndexLinear()
end

function Base.size(bytes::ShiftedExactBytes)
    return (length(bytes.bytes),)
end

function Base.axes(bytes::ShiftedExactBytes)
    return (2:(length(bytes.bytes) + 1),)
end

function Base.getindex(bytes::ShiftedExactBytes, index::Int)
    checkbounds(bytes, index)
    return bytes.bytes[index - 1]
end

function Base.IndexStyle(::Type{UnitRangeExactBytes})
    return IndexLinear()
end

function Base.size(bytes::UnitRangeExactBytes)
    return (length(bytes.bytes),)
end

function Base.axes(bytes::UnitRangeExactBytes)
    return (1:length(bytes.bytes),)
end

function Base.getindex(bytes::UnitRangeExactBytes, index::Int)
    checkbounds(bytes, index)
    return bytes.bytes[index]
end

mutable struct SourceCallbackSentinel <: Exception
    id::Int
end

mutable struct ExactReadSource <: Parquet.AbstractSource
    lengthvalue::Any
    readvalue::Any
    lengthcalls::Int
    readcalls::Int
end

function Parquet.sourcelength(source::ExactReadSource)
    source.lengthcalls += 1
    source.lengthvalue isa Exception && throw(source.lengthvalue)
    return source.lengthvalue
end

function Parquet.readrange(source::ExactReadSource, offset::Integer,
        count::Integer)
    source.readcalls += 1
    source.readvalue isa Exception && throw(source.readvalue)
    return source.readvalue
end

@testset "range caches reserve their bytes before fetching" begin
    source = ExactReadSource(4, UInt8[1, 2, 3, 4], 0, 0)
    budget = Parquet._ByteBudget(Parquet.Limits(max_materialized_bytes=3))
    cached = Parquet._CoalescedSource(source, [(Int64(0), Int64(4))], budget)
    @test_throws Parquet.LimitError Parquet.readrange(cached, 0, 4)
    @test source.readcalls == 0

    budget = Parquet._ByteBudget(Parquet.Limits(max_materialized_bytes=4))
    cached = Parquet._CoalescedSource(source, [(Int64(0), Int64(4))], budget)
    @test collect(Parquet.readrange(cached, 0, 4)) == UInt8[1, 2, 3, 4]
    @test collect(Parquet.readrange(cached, 1, 2)) == UInt8[2, 3]
    @test source.readcalls == 1
    @test budget.used == 4

    budget = Parquet._ByteBudget(Parquet.Limits(max_materialized_bytes=3))
    @test_throws Parquet.LimitError Parquet._ChunkSource(source, Int64(0), Int64(4), budget)
    @test source.readcalls == 1
end

mutable struct FailingPathIO <: IO
    closed::Bool
    failure::SourceCallbackSentinel
end

function Base.filesize(io::FailingPathIO)
    throw(io.failure)
end

function Base.close(io::FailingPathIO)
    io.closed = true
    return
end

function Base.isopen(io::FailingPathIO)
    return !io.closed
end

struct FailingSourcePath <: AbstractString
    io::FailingPathIO
end

function Base.open(path::FailingSourcePath, mode::AbstractString)
    return path.io
end

function Base.isdir(::FailingSourcePath)
    return false
end

@testset "byte sources" begin
    bytes = UInt8[0x10, 0x20, 0x30, 0x40]
    src = Parquet.source(bytes)
    @test Parquet.sourcelength(src) == 4
    @test Parquet.concurrentreads(src)
    slice = Parquet.readrange(src, 1, 2)
    @test collect(slice) == UInt8[0x20, 0x30]
    @test_throws BoundsError Parquet.readrange(src, 3, 2)
    @test_throws BoundsError Parquet.readrange(src, typemax(Int64), 1)
    @test_throws ArgumentError Parquet.readrange(src, 0, -1)
    Parquet.close!(src)
    Parquet.close!(src)
    @test_throws ArgumentError slice[1]
    @test_throws ArgumentError Parquet.sourcelength(src)

    shifted = Parquet.source(ShiftedExactBytes(bytes))
    shiftedslice = Parquet.readrange(shifted, 1, 2)
    @test axes(shiftedslice) == (Base.OneTo(2),)
    @test collect(shiftedslice) == UInt8[0x20, 0x30]
    @test Parquet._contiguous(shiftedslice) == UInt8[0x20, 0x30]
    Parquet.close!(shifted)
end

@testset "exact source adapter contract" begin
    valid = ExactReadSource(Int64(4), UInt8[0x01, 0x02], 0, 0)
    @test Parquet._checkedsourcelength(valid) == 4
    @test Parquet._readrangeexact(valid, Int64(4), Int64(1), Int64(2)) ==
        UInt8[0x01, 0x02]
    @test valid.lengthcalls == 1
    @test valid.readcalls == 1

    for value in (UInt8[0x01], UInt8[0x01, 0x02, 0x03],
            Int8[0x01, 0x02], ShiftedExactBytes(UInt8[0x01, 0x02]),
            UnitRangeExactBytes(UInt8[0x01, 0x02]))
        source = ExactReadSource(Int64(4), value, 0, 0)
        @test_throws ArgumentError Parquet._readrangeexact(source, Int64(4),
            Int64(1), Int64(2))
        @test source.readcalls == 1
    end

    sentinel = SourceCallbackSentinel(1)
    throwing = ExactReadSource(Int64(4), sentinel, 0, 0)
    error = try
        Parquet._readrangeexact(throwing, Int64(4), Int64(1), Int64(2))
        nothing
    catch err
        err
    end
    @test error === sentinel
    @test throwing.readcalls == 1

    untouched = ExactReadSource(Int64(4), UInt8[], 0, 0)
    @test_throws ArgumentError Parquet._readrangeexact(untouched, Int64(4),
        Int64(-1), Int64(1))
    @test_throws ArgumentError Parquet._readrangeexact(untouched, Int64(4),
        Int64(0), Int64(-1))
    @test_throws ArgumentError Parquet._readrangeexact(untouched, Int64(4),
        typemax(Int64), Int64(1))
    @test_throws ArgumentError Parquet._readrangeexact(untouched, Int64(4),
        Int64(3), Int64(2))
    @test untouched.readcalls == 0

    unsigned = ExactReadSource(UInt64(4), UInt8[], 0, 0)
    @test Parquet._checkedsourcelength(unsigned) == 4
    wide = ExactReadSource(UInt128(4), UInt8[], 0, 0)
    @test Parquet._checkedsourcelength(wide) == 4
    wrongtype = ExactReadSource(4.0, UInt8[], 0, 0)
    @test_throws ArgumentError Parquet._checkedsourcelength(wrongtype)
    boolean = ExactReadSource(true, UInt8[], 0, 0)
    @test_throws ArgumentError Parquet._checkedsourcelength(boolean)
    oversized = ExactReadSource(UInt128(typemax(Int64)) + UInt128(1),
        UInt8[], 0, 0)
    @test_throws ArgumentError Parquet._checkedsourcelength(oversized)
    negative = ExactReadSource(Int64(-1), UInt8[], 0, 0)
    @test_throws ArgumentError Parquet._checkedsourcelength(negative)
    lengthsentinel = SourceCallbackSentinel(2)
    lengththrowing = ExactReadSource(lengthsentinel, UInt8[], 0, 0)
    lengtherror = try
        Parquet._checkedsourcelength(lengththrowing)
        nothing
    catch err
        err
    end
    @test lengtherror === lengthsentinel
end


@testset "budgeted IO buffering" begin
    limits = Parquet.Limits(max_materialized_bytes=1000)
    oversized = IOBuffer(zeros(UInt8, 2000))
    @test_throws Parquet.LimitError Parquet.source(oversized;
        budget=Parquet._ByteBudget(limits))
    @test isopen(oversized)
    @test_throws Parquet.LimitError Parquet.Table(
        IOBuffer(zeros(UInt8, 2000)); limits=limits)
    src = Parquet.source(IOBuffer(zeros(UInt8, 12)); budget=Parquet._ByteBudget(limits))
    @test Parquet.sourcelength(src) == 12
    close(src)
    close(src)
end

@testset "mapped file source" begin
    path, io = mktemp()
    try
        write(io, UInt8[0x50, 0x41, 0x52, 0x31])
        close(io)
        src = Parquet.source(path)
        @test collect(Parquet.readrange(src, 0, 4)) == UInt8[0x50, 0x41, 0x52, 0x31]
        Parquet.close!(src)
        @test_throws ArgumentError Parquet.readrange(src, 0, 1)
    finally
        isopen(io) && close(io)
        # `close!` releases the descriptor, but the mapping itself lives until the
        # garbage collector finalizes it, so Windows can still hold the file open
        # here. That is the documented source lifetime, not a leak, so treat a
        # refused removal of this temporary file as acceptable.
        GC.gc()
        try
            rm(path; force=true)
        catch err
            err isa Base.IOError || rethrow()
        end
    end
end

@testset "directory paths" begin
    directory = mktempdir()
    error = try
        Parquet.Table(directory)
        nothing
    catch err
        err
    end
    # An empty directory is a dataset without files; a File is one file.
    @test error isa ArgumentError
    @test occursin("no Parquet files found", sprint(showerror, error))
    error = try
        Parquet.File(directory)
        nothing
    catch err
        err
    end
    @test error isa ArgumentError
    @test occursin("is a directory; read it with Parquet.Dataset", sprint(showerror, error))
end

@testset "path setup cleanup" begin
    sentinel = SourceCallbackSentinel(3)
    io = FailingPathIO(false, sentinel)
    error = try
        Parquet.source(FailingSourcePath(io))
        nothing
    catch err
        err
    end
    @test error === sentinel
    @test !isopen(io)
end

@testset "close guards are atomic" begin
    # Regression: closed was a plain field, so concurrent close! calls could both
    # pass the guard.
    src = Parquet.source(UInt8[0x00])
    region = src.region
    @test Base.isfieldatomic(typeof(region), :closed)
    Parquet.close!(src)
    Parquet.close!(src)
    owned = Parquet.source(IOBuffer(zeros(UInt8, 12)))
    @sync for _ in 1:32
        errormonitor(Threads.@spawn Parquet.close!(owned))
    end
    @test_throws ArgumentError Parquet.readrange(owned, 0, 1)

    file = Parquet.File(Parquet._encodefile((a=Int32[1],)))
    @test Base.isfieldatomic(typeof(file), :closed)
    close(file)
    close(file)
    # A table holds no file, so closing it is harmless.
    table = Parquet.Table(Parquet._encodefile((a=Int32[1],)))
    close(table)
    @test table.a == Int32[1]
end

@testset "URL strings point at CloudStore" begin
    for url in ("s3://bucket/trips.parquet", "https://example.com/trips.parquet",
            "gs://bucket/lake/*.parquet")
        for read in (Parquet.Table, Parquet.File, Parquet.Dataset)
            err = try
                read(url)
            catch e
                e
            end
            @test err isa ArgumentError && occursin("is a URL", err.msg) &&
                occursin("CloudStore", err.msg)
        end
    end
end
