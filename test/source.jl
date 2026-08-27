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
    budget = Parquet._LiveByteBudget(limits)
    oversized = IOBuffer(zeros(UInt8, 2000))
    @test_throws Parquet.LimitError Parquet.source(oversized; budget=budget)
    @test Parquet._budgetused(budget) == 0
    @test isopen(oversized)
    @test_throws Parquet.LimitError Parquet.Table(
        IOBuffer(zeros(UInt8, 2000)); limits=limits)

    smallbudget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(smallbudget, Int64(17))
    src = Parquet.source(IOBuffer(zeros(UInt8, 12)); budget=smallbudget)
    @test Parquet.sourcelength(src) == 12
    @test Parquet._budgetused(smallbudget) ==
        17 + Parquet._materializedarraybytes(UInt8, 12)
    close(src)
    @test Parquet._budgetused(smallbudget) == 17
    close(src)
    @test Parquet._budgetused(smallbudget) == 17
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
        rm(path; force=true)
    end
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
