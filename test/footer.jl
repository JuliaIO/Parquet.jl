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
    footer = Parquet.readfooter(Parquet.source(input))
    @test footer.offset == 4
    @test footer.length == 3
    @test !footer.encrypted
    @test collect(footer.bytes) == UInt8[0x01, 0x02, 0x03]
    # File decodes the footer it frames.
    @test_throws Parquet.FormatError Parquet.File(input)

    encryptedbytes = parquetbytes(UInt8[0xaa]; encrypted=true)
    @test Parquet.readfooter(Parquet.source(encryptedbytes)).encrypted
    @test_throws Parquet.UnsupportedFeatureError Parquet.File(encryptedbytes)

    @test Parquet.readfooter(TestSource(input)).length == 3
    valid = Parquet._encodefile((a=Int32[1, 2],))
    custom = Parquet.File(TestSource(valid))
    @test custom.metadata.num_rows == 2
    @test !Parquet.concurrentreads(custom.source)
    close(custom)
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
    # A source that is not in memory gives its footer in one read of its last 64 KiB
    # (the whole of a small file, whose leading magic is then checked too), and a second
    # read only for a footer that does not fit.
    input = parquetbytes(UInt8[0x01, 0x02, 0x03])
    source = FooterProbeSource(input; mode=:length_changing)
    Parquet.readfooter(source)
    @test source.lengthcalls == 1
    @test source.reads == [(Int64(0), Int64(length(input)))]

    large = parquetbytes(fill(UInt8(0x07), 70_000))
    largesource = FooterProbeSource(large)
    footer = Parquet.readfooter(largesource)
    @test footer.length == 70_000
    @test collect(footer.bytes) == fill(UInt8(0x07), 70_000)
    @test largesource.reads == [(Int64(length(large) - 65_536), Int64(65_536)),
        (Int64(4), Int64(70_000))]

    invalid = copy(input)
    invalid[1] = 0x00
    invalidsource = FooterProbeSource(invalid)
    @test_throws Parquet.FormatError Parquet.File(invalidsource)
    @test invalidsource.lengthcalls == 1
    @test invalidsource.reads == [(Int64(0), Int64(length(invalid)))]
    @test invalidsource.closes == 0

    for (bytes, reads) in ((input, 1), (large, 2))
        for mode in (:short, :long, :wrong_type, :wrong_axes), faultread in 1:reads
            malformed = FooterProbeSource(bytes; mode=mode, faultread=faultread)
            @test_throws ArgumentError Parquet.File(malformed)
            @test length(malformed.reads) == faultread
            @test malformed.closes == 0
        end
        for faultread in 1:reads
            sentinel = FooterCallbackSentinel(faultread)
            throwing = FooterProbeSource(bytes; mode=:throw, faultread=faultread,
                sentinel=sentinel)
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

@testset "File source ownership and copied IO" begin
    input = Parquet._encodefile((a=Int32[1, 2, 3],))
    failed = FooterProbeSource(UInt8[])
    @test_throws Parquet.FormatError Parquet.File(failed)
    @test failed.closes == 0

    # Parquet never closes a source it was given, even when the file is closed.
    given = FooterProbeSource(input)
    file = Parquet.File(given)
    @test given.closes == 0
    close(file)
    close(file)
    @test given.closes == 0

    # A source whose footer does not decode is not adopted.
    tablefailure = FooterProbeSource(parquetbytes(UInt8[0x01, 0x02, 0x03]))
    @test_throws Parquet.FormatError Parquet.Table(tablefailure)
    @test tablefailure.closes == 0

    limits = Parquet.Limits(max_materialized_bytes=100_000)
    invalidio = IOBuffer(UInt8[])
    @test_throws Parquet.FormatError Parquet.File(invalidio; limits=limits)
    @test isopen(invalidio)

    validio = IOBuffer(input)
    copied = Parquet.File(validio; limits=limits)
    close(copied)
    @test isopen(validio)
    # The copy of an IO source counts against the read's byte cap.
    @test_throws Parquet.LimitError Parquet.File(IOBuffer(input);
        limits=Parquet.Limits(max_materialized_bytes=length(input) - 1))
end
