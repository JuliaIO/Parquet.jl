function parquetbytes(footer::Vector{UInt8}; encrypted::Bool=false)
    magic = encrypted ? UInt8[0x50, 0x41, 0x52, 0x45] : UInt8[0x50, 0x41, 0x52, 0x31]
    lengthbytes = reinterpret(UInt8, [htol(UInt32(length(footer)))])
    return vcat(magic, footer, lengthbytes, magic)
end

struct TestSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
end

function Parquet.sourcelength(src::TestSource)
    return Int64(length(src.bytes))
end

function Parquet.readrange(src::TestSource, offset::Integer, count::Integer)
    first = Int(offset) + 1
    return @view src.bytes[first:(first + Int(count) - 1)]
end

struct ShiftedFooterBytes <: AbstractVector{UInt8}
    bytes::Vector{UInt8}
end

function Base.IndexStyle(::Type{ShiftedFooterBytes})
    return IndexLinear()
end

function Base.size(bytes::ShiftedFooterBytes)
    return (length(bytes.bytes),)
end

function Base.axes(bytes::ShiftedFooterBytes)
    return (2:(length(bytes.bytes) + 1),)
end

function Base.getindex(bytes::ShiftedFooterBytes, index::Int)
    checkbounds(bytes, index)
    return bytes.bytes[index - 1]
end

mutable struct FooterCallbackSentinel <: Exception
    id::Int
end

mutable struct FooterProbeSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    mode::Symbol
    faultread::Int
    lengthcalls::Int
    reads::Vector{Tuple{Int64,Int64}}
    closes::Int
    sentinel::Union{Nothing,FooterCallbackSentinel}
end


function FooterProbeSource(bytes::Vector{UInt8}; mode::Symbol=:normal,
        faultread::Int=0, sentinel=nothing)
    return FooterProbeSource(bytes, mode, faultread, 0,
        Tuple{Int64,Int64}[], 0, sentinel)
end

function Parquet.sourcelength(source::FooterProbeSource)
    source.lengthcalls += 1
    source.mode === :length_throw && throw(something(source.sentinel))
    source.mode === :length_type && return Float64(length(source.bytes))
    source.mode === :length_negative && return Int64(-1)
    source.mode === :length_changing && source.lengthcalls > 1 && return Int64(0)
    return Int64(length(source.bytes))
end

function Parquet.readrange(source::FooterProbeSource, offset::Integer,
        count::Integer)
    offset64 = Int64(offset)
    count64 = Int64(count)
    push!(source.reads, (offset64, count64))
    if length(source.reads) == source.faultread
        source.mode === :throw && throw(something(source.sentinel))
        source.mode === :short && return fill(UInt8(0), max(Int(count64) - 1, 0))
        source.mode === :long && return fill(UInt8(0), Int(count64) + 1)
        source.mode === :wrong_type && return fill(Int8(0), Int(count64))
        source.mode === :wrong_axes && return ShiftedFooterBytes(
            fill(UInt8(0), Int(count64)))
    end
    first = Int(offset64) + 1
    return @view source.bytes[first:(first + Int(count64) - 1)]
end

function Parquet.close!(source::FooterProbeSource)
    source.closes += 1
    return
end

@testset "footer framing" begin
    input = parquetbytes(UInt8[0x01, 0x02, 0x03])
    file = Parquet.File(input)
    @test file.footer.offset == 4
    @test file.footer.length == 3
    @test !file.footer.encrypted
    @test collect(file.footer.bytes) == UInt8[0x01, 0x02, 0x03]
    close(file)
    close(file)

    encrypted = Parquet.File(parquetbytes(UInt8[0xaa]; encrypted=true))
    @test encrypted.footer.encrypted
    close(encrypted)

    custom = Parquet.File(TestSource(input))
    @test custom.footer.length == 3
    @test !Parquet.concurrentreads(custom.source)
    close(custom)

    @test_throws Parquet.FormatError Parquet.File(UInt8[])
    @test_throws Parquet.FormatError Parquet.File(vcat(UInt8[0x00, 0x00, 0x00, 0x00], input[5:end]))
    @test_throws Parquet.LimitError Parquet.File(input; limits=Parquet.Limits(max_footer_bytes=2))

    invalidlength = copy(input)
    invalidlength[(end - 7):(end - 4)] .= 0xff
    @test_throws Parquet.FormatError Parquet.File(invalidlength)

    oversizefooter = copy(input)
    oversizefooter[(end - 7):(end - 4)] .= UInt8[0x04, 0x00, 0x00, 0x00]
    @test_throws Parquet.FormatError Parquet.File(oversizefooter)
end


@testset "footer exact source reads" begin
    input = parquetbytes(UInt8[0x01, 0x02, 0x03])
    source = FooterProbeSource(input; mode=:length_changing)
    file = Parquet.File(source)
    @test source.lengthcalls == 1
    @test source.reads == [(Int64(0), Int64(4)),
        (Int64(length(input) - 8), Int64(8)), (Int64(4), Int64(3))]
    close(file)
    @test source.closes == 1

    invalid = copy(input)
    invalid[1] = 0x00
    invalidsource = FooterProbeSource(invalid)
    @test_throws Parquet.FormatError Parquet.File(invalidsource)
    @test invalidsource.lengthcalls == 1
    @test invalidsource.reads == [(Int64(0), Int64(4))]
    @test invalidsource.closes == 0

    for mode in (:short, :long, :wrong_type, :wrong_axes)
        for faultread in 1:3
            malformed = FooterProbeSource(input; mode=mode,
                faultread=faultread)
            @test_throws ArgumentError Parquet.File(malformed)
            @test length(malformed.reads) == faultread
            @test malformed.closes == 0
        end
    end

    for faultread in 1:3
        sentinel = FooterCallbackSentinel(faultread)
        throwing = FooterProbeSource(input; mode=:throw,
            faultread=faultread, sentinel=sentinel)
        error = try
            Parquet.File(throwing)
            nothing
        catch err
            err
        end
        @test error === sentinel
        @test length(throwing.reads) == faultread
        @test throwing.closes == 0
    end

    for mode in (:length_type, :length_negative)
        invalidlength = FooterProbeSource(input; mode=mode)
        @test_throws ArgumentError Parquet.File(invalidlength)
        @test invalidlength.lengthcalls == 1
        @test isempty(invalidlength.reads)
        @test invalidlength.closes == 0
    end
    sentinel = FooterCallbackSentinel(4)
    throwinglength = FooterProbeSource(input; mode=:length_throw,
        sentinel=sentinel)
    error = try
        Parquet.File(throwinglength)
        nothing
    catch err
        err
    end
    @test error === sentinel
    @test isempty(throwinglength.reads)
    @test throwinglength.closes == 0
end

@testset "File source ownership and copied IO budget" begin
    input = parquetbytes(UInt8[0x01, 0x02, 0x03])
    failed = FooterProbeSource(UInt8[])
    @test_throws Parquet.FormatError Parquet.File(failed)
    @test failed.closes == 0

    adopted = FooterProbeSource(input)
    file = Parquet.File(adopted)
    @test adopted.closes == 0
    close(file)
    close(file)
    @test adopted.closes == 1

    tablefailure = FooterProbeSource(input)
    @test_throws Parquet.FormatError Parquet.Table(tablefailure)
    @test tablefailure.closes == 1

    limits = Parquet.Limits(max_materialized_bytes=100_000)
    budget = Parquet._LiveByteBudget(limits)
    Parquet._reserve!(budget, Int64(23))
    invalidio = IOBuffer(UInt8[])
    @test_throws Parquet.FormatError Parquet.File(invalidio; limits=limits,
        budget=budget)
    @test Parquet._budgetused(budget) == 23
    @test isopen(invalidio)

    validio = IOBuffer(input)
    copied = Parquet.File(validio; limits=limits, budget=budget)
    @test Parquet._budgetused(budget) ==
        23 + Parquet._materializedarraybytes(UInt8, length(input))
    close(copied)
    @test Parquet._budgetused(budget) == 23
    @test isopen(validio)
end
