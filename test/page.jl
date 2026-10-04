using Random

if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end
if !isdefined(Parquet, :readpage)
    Base.include(Parquet, joinpath(@__DIR__, "..", "src", "page.jl"))
end

const PAGE_CORPUS = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))

function pagecorpus(parts...)
    return joinpath(PAGE_CORPUS, "data", parts...)
end

function pagev1header(count::Integer; encoding=MD.Encoding.PLAIN, levelencoding=MD.Encoding.RLE)
    return MD.DataPageHeader(num_values=Int32(count), encoding=encoding, definition_level_encoding=levelencoding,
        repetition_level_encoding=MD.Encoding.RLE)
end

function pagev2header(count::Integer; nulls=0, rows=count, definition=0, repetition=0,
    encoding=MD.Encoding.PLAIN, is_compressed=nothing)
    return MD.DataPageHeaderV2(num_values=Int32(count), num_nulls=Int32(nulls),
        num_rows=Int32(rows), encoding=encoding,
        definition_levels_byte_length=Int32(definition),
        repetition_levels_byte_length=Int32(repetition), is_compressed=is_compressed)
end

# Serialize a page: Thrift header followed by the payload. `crc` is :valid, :none, or an Int32.
function pagebytes(payload::Vector{UInt8}; type=MD.PageType.DATA_PAGE, v1=nothing,
    index=nothing, dict=nothing, v2=nothing,
    crc=:valid, compressed=length(payload), uncompressed=compressed)
    crcvalue = crc === :valid ? reinterpret(Int32, Parquet.pagechecksum(payload)) : crc === :none ? nothing : Int32(crc)
    header = MD.PageHeader(type_=type, uncompressed_page_size=Int32(uncompressed), compressed_page_size=Int32(compressed),
        crc=crcvalue, data_page_header=v1, index_page_header=index,
        dictionary_page_header=dict, data_page_header_v2=v2)
    return vcat(TH.encode(header), payload)
end

function plainpage(values::Vector{Int32}; kwargs...)
    return pagebytes(Parquet.encode_plain(values); v1=pagev1header(length(values)), kwargs...)
end

function readframe(bytes::Vector{UInt8}; offset=0, stop=length(bytes), limits=Parquet.Limits())
    src = Parquet.source(bytes)
    return Parquet.readpage(src, Int64(offset), Int64(stop), limits)
end

mutable struct PageBoundSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    reads::Int
end

function Parquet.sourcelength(source::PageBoundSource)
    return Int64(length(source.bytes))
end

function Parquet.readrange(source::PageBoundSource, offset::Integer,
        count::Integer)
    source.reads += 1
    first = Int(offset) + 1
    return @view source.bytes[first:(first + Int(count) - 1)]
end

struct ShiftedPageBytes <: AbstractVector{UInt8}
    bytes::Vector{UInt8}
end

function Base.IndexStyle(::Type{ShiftedPageBytes})
    return IndexLinear()
end

function Base.size(bytes::ShiftedPageBytes)
    return (length(bytes.bytes),)
end

function Base.axes(bytes::ShiftedPageBytes)
    return (2:(length(bytes.bytes) + 1),)
end

function Base.getindex(bytes::ShiftedPageBytes, index::Int)
    checkbounds(bytes, index)
    return bytes.bytes[index - 1]
end

mutable struct PageCallbackSentinel <: Exception
    id::Int
end

mutable struct PageContractSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    mode::Symbol
    faultread::Int
    lengthcalls::Int
    reads::Vector{Tuple{Int64,Int64}}
    sentinel::Union{Nothing,PageCallbackSentinel}
end

function PageContractSource(bytes::Vector{UInt8}; mode::Symbol=:normal,
        faultread::Int=0, sentinel=nothing)
    return PageContractSource(bytes, mode, faultread, 0,
        Tuple{Int64,Int64}[], sentinel)
end

function Parquet.sourcelength(source::PageContractSource)
    source.lengthcalls += 1
    source.mode === :length_throw && throw(something(source.sentinel))
    source.mode === :length_type && return Float64(length(source.bytes))
    source.mode === :length_negative && return Int64(-1)
    source.mode === :length_changing && source.lengthcalls > 1 && return Int64(0)
    return Int64(length(source.bytes))
end

function Parquet.readrange(source::PageContractSource, offset::Integer,
        count::Integer)
    offset64 = Int64(offset)
    count64 = Int64(count)
    push!(source.reads, (offset64, count64))
    if length(source.reads) == source.faultread
        source.mode === :throw && throw(something(source.sentinel))
        source.mode === :short && return fill(UInt8(0), max(Int(count64) - 1, 0))
        source.mode === :long && return fill(UInt8(0), Int(count64) + 1)
        source.mode === :wrong_type && return fill(Int8(0), Int(count64))
        source.mode === :wrong_axes && return ShiftedPageBytes(
            fill(UInt8(0), Int(count64)))
    end
    first = Int(offset64) + 1
    return @view source.bytes[first:(first + Int(count64) - 1)]
end

@testset "page header parsing and bounds" begin
    page = plainpage(Int32[1, 2, 3])
    src = Parquet.source(page)
    header, headerlength = Parquet.readpageheader(src, Int64(0), Int64(length(page)), Parquet.Limits())
    @test header.type_ == MD.PageType.DATA_PAGE && header.compressed_page_size == 12
    @test headerlength == length(page) - 12
    @test header.data_page_header.num_values == 3 && header.crc !== nothing
    @test Parquet.validatepageheader(header) === :data_v1
    sharedbudget = Parquet._LiveByteBudget(Parquet.Limits())
    sharedheader, sharedlength = Parquet.readpageheader(src, Int64(0),
        Int64(length(page)), Parquet.Limits(); budget=sharedbudget)
    @test sharedheader == header
    @test sharedlength == headerlength
    retainedcharge = Parquet._budgetused(sharedbudget)
    @test retainedcharge > 0
    Parquet._release!(sharedbudget, retainedcharge)
    frame = readframe(page)
    @test frame.offset == 0 && frame.headerlength == headerlength && Parquet.pageend(frame) == length(page)
    @test collect(frame.payload) == Parquet.encode_plain(Int32[1, 2, 3]) && Parquet.pagekind(frame) === :data_v1
    padded = vcat(UInt8[0xaa, 0xbb], page, UInt8[0xcc])
    shifted = readframe(padded; offset=2, stop=2 + length(page))
    @test shifted.offset == 2 && Parquet.pageend(shifted) == 2 + length(page)
    @test collect(shifted.payload) == collect(frame.payload)
    @test_throws Parquet.FormatError readframe(page; offset=length(page))
    @test_throws Parquet.FormatError readframe(page; offset=-1)
    @test_throws Parquet.FormatError readframe(page; stop=headerlength - 1)
    @test_throws Parquet.FormatError readframe(page; stop=headerlength + 5)
    @test_throws Parquet.FormatError readframe(page[1:(end - 1)])
    @test_throws Parquet.FormatError Parquet.readpageheader(src, Int64(0), Int64(headerlength - 1), Parquet.Limits())
    outofbounds = PageBoundSource(page, 0)
    zerolimit = Parquet.Limits(max_page_header_bytes=0)
    headererror = try
        Parquet.readpageheader(outofbounds, Int64(0),
            Int64(length(page) + 1), zerolimit)
        nothing
    catch err
        err
    end
    @test headererror isa Parquet.FormatError
    @test headererror.message ==
        "page read stop $(length(page) + 1) is past the end of the source"
    @test outofbounds.reads == 0
    pageerror = try
        Parquet.readpage(outofbounds, Int64(0),
            Int64(length(page) + 1), zerolimit)
        nothing
    catch err
        err
    end
    @test pageerror isa Parquet.FormatError
    @test pageerror.message == headererror.message
    @test outofbounds.reads == 0
    for n in 0:(headerlength - 1)
        @test_throws Parquet.FormatError readframe(page[1:n])
    end
    truncatedheader = page[1:(headerlength - 1)]
    @test_throws Parquet.FormatError readframe(truncatedheader;
        limits=Parquet.Limits(max_page_header_bytes=length(truncatedheader)))
    tight = Parquet.Limits(max_page_header_bytes=headerlength)
    @test Parquet.readpage(src, Int64(0), Int64(length(page)), tight).headerlength == headerlength
    @test_throws Parquet.LimitError readframe(page; limits=Parquet.Limits(max_page_header_bytes=headerlength - 1))
    @test_throws Parquet.LimitError readframe(page; limits=Parquet.Limits(max_page_header_bytes=1))
    @test_throws Parquet.LimitError readframe(page; limits=Parquet.Limits(max_page_header_bytes=0))
    @test_throws Parquet.LimitError readframe(page; limits=Parquet.Limits(max_page_bytes=11))
    @test readframe(page; limits=Parquet.Limits(max_page_bytes=12)).header.compressed_page_size == 12
    bigger = pagebytes(Parquet.encode_plain(Int32[1, 2, 3]); v1=pagev1header(3), uncompressed=4096)
    @test_throws Parquet.LimitError readframe(bigger; limits=Parquet.Limits(max_page_bytes=4095))
    @test_throws Parquet.FormatError readframe(pagebytes(UInt8[]; v1=pagev1header(0), compressed=-1, uncompressed=0))
    @test_throws Parquet.FormatError readframe(pagebytes(UInt8[]; v1=pagev1header(0), compressed=0, uncompressed=-1))
    @test_throws Parquet.FormatError readframe(pagebytes(UInt8[0x01];
        v1=pagev1header(0), compressed=typemax(Int32)))
    truncated = pagebytes(UInt8[0x01]; v1=pagev1header(0), compressed=2)
    @test_throws Parquet.FormatError readframe(truncated)
    @test_throws Parquet.FormatError readframe(truncated;
        limits=Parquet.Limits(max_page_bytes=1))
end

@testset "page exact source reads" begin
    page = plainpage(Int32[1, 2, 3])
    changing = PageContractSource(page; mode=:length_changing)
    frame = Parquet.readpage(changing, Int64(0), Int64(length(page)),
        Parquet.Limits())
    @test collect(frame.payload) == Parquet.encode_plain(Int32[1, 2, 3])
    @test changing.lengthcalls == 1
    @test length(changing.reads) == 2

    for mode in (:short, :long, :wrong_type, :wrong_axes)
        for faultread in 1:2
            malformed = PageContractSource(page; mode=mode,
                faultread=faultread)
            budget = Parquet._LiveByteBudget(Parquet.Limits())
            @test_throws ArgumentError Parquet.readpage(malformed, Int64(0),
                Int64(length(page)), Parquet.Limits(); budget=budget)
            @test length(malformed.reads) == faultread
            @test Parquet._budgetused(budget) == 0
        end
    end

    for faultread in 1:2
        sentinel = PageCallbackSentinel(faultread)
        throwing = PageContractSource(page; mode=:throw,
            faultread=faultread, sentinel=sentinel)
        budget = Parquet._LiveByteBudget(Parquet.Limits())
        error = try
            Parquet.readpage(throwing, Int64(0), Int64(length(page)),
                Parquet.Limits(); budget=budget)
            nothing
        catch err
            err
        end
        @test error === sentinel
        @test length(throwing.reads) == faultread
        @test Parquet._budgetused(budget) == 0
    end

    for mode in (:length_type, :length_negative)
        malformed = PageContractSource(page; mode=mode)
        @test_throws ArgumentError Parquet.readpage(malformed, Int64(0),
            Int64(length(page)), Parquet.Limits())
        @test malformed.lengthcalls == 1
        @test isempty(malformed.reads)
    end
    sentinel = PageCallbackSentinel(3)
    throwinglength = PageContractSource(page; mode=:length_throw,
        sentinel=sentinel)
    error = try
        Parquet.readpage(throwinglength, Int64(0), Int64(length(page)),
            Parquet.Limits())
        nothing
    catch err
        err
    end
    @test error === sentinel
    @test isempty(throwinglength.reads)
end

@testset "offset-index exact source reads" begin
    bytes = TH.encode(MD.OffsetIndex(page_locations=MD.PageLocation[]))
    normal = PageContractSource(bytes; mode=:length_changing)
    footer = Parquet.Footer(Int64(length(bytes)), Int64(0), false, UInt8[])
    file = Parquet.File(normal, footer, false)
    index = Parquet._decodeoffsetindex(file, Int64(0), Int64(length(bytes)),
        Parquet.Limits(), Parquet._LiveByteBudget(Parquet.Limits()))
    @test isempty(index.page_locations)
    @test normal.lengthcalls == 1
    @test normal.reads == [(Int64(0), Int64(length(bytes)))]

    for mode in (:short, :long, :wrong_type, :wrong_axes)
        malformed = PageContractSource(bytes; mode=mode, faultread=1)
        malformedfile = Parquet.File(malformed, footer, false)
        budget = Parquet._LiveByteBudget(Parquet.Limits())
        @test_throws ArgumentError Parquet._decodeoffsetindex(malformedfile,
            Int64(0), Int64(length(bytes)), Parquet.Limits(), budget)
        @test malformed.lengthcalls == 1
        @test length(malformed.reads) == 1
        @test Parquet._budgetused(budget) == 0
    end

    sentinel = PageCallbackSentinel(4)
    throwing = PageContractSource(bytes; mode=:throw, faultread=1,
        sentinel=sentinel)
    throwingfile = Parquet.File(throwing, footer, false)
    budget = Parquet._LiveByteBudget(Parquet.Limits())
    error = try
        Parquet._decodeoffsetindex(throwingfile, Int64(0),
            Int64(length(bytes)), Parquet.Limits(), budget)
        nothing
    catch err
        err
    end
    @test error === sentinel
    @test Parquet._budgetused(budget) == 0
end

@testset "checked page frame count and arithmetic" begin
    exactlimits = Parquet.Limits(max_container_elements=1)
    @test Parquet._nextpageframecount(Int64(0), exactlimits) == 1
    limiterror = try
        Parquet._nextpageframecount(Int64(1), exactlimits)
        nothing
    catch err
        err
    end
    @test limiterror isa Parquet.LimitError
    @test limiterror.resource == :container_elements
    @test limiterror.requested == 2
    @test limiterror.maximum == 1
    @test_throws ArgumentError Parquet._nextpageframecount(Int64(-1),
        exactlimits)
    overflow = try
        Parquet._nextpageframecount(typemax(Int64), Parquet.Limits(
            max_container_elements=typemax(Int64)))
        nothing
    catch err
        err
    end
    @test overflow isa Parquet.LimitError
    @test overflow.resource == :container_elements
    @test overflow.requested == typemax(Int64)

    frame = readframe(plainpage(Int32[1]))
    compressed = Int64(frame.header.compressed_page_size)
    offset = typemax(Int64) - Int64(frame.headerlength) - compressed
    exact = Parquet.PageFrame(offset, frame.header, frame.headerlength,
        frame.payload, frame.materializedcharge)
    @test Parquet.pageend(exact) == typemax(Int64)
    beyond = Parquet.PageFrame(offset + 1, frame.header,
        frame.headerlength, frame.payload, frame.materializedcharge)
    frameerror = try
        Parquet.pageend(beyond)
        nothing
    catch err
        err
    end
    @test frameerror isa Parquet.FormatError
    @test frameerror.message == "page frame end overflows Int64"
    @test_throws Parquet.FormatError Parquet._pageframeend(Int64(-1), 1, 0)
    @test_throws Parquet.FormatError Parquet._pageframeend(Int64(0), -1, 0)
    @test_throws Parquet.FormatError Parquet._pageframeend(Int64(0),
        typemax(UInt128), 0)
end

@testset "page header materialization preflight" begin
    payload = fill(UInt8(0x61), 100_000)
    statistics = MD.Statistics(max=payload)
    data = MD.DataPageHeader(num_values=Int32(0), encoding=MD.Encoding.PLAIN,
        definition_level_encoding=MD.Encoding.RLE,
        repetition_level_encoding=MD.Encoding.RLE, statistics=statistics)
    header = MD.PageHeader(type_=MD.PageType.DATA_PAGE,
        uncompressed_page_size=Int32(0), compressed_page_size=Int32(0),
        data_page_header=data)
    bytes = TH.encode(header)
    src = Parquet.source(bytes)
    limits = Parquet.Limits(max_materialized_bytes=700,
        max_page_header_bytes=200_000)
    function rejectlargestatistics()
        budget = Parquet._LiveByteBudget(limits)
        @test_throws Parquet.LimitError Parquet.readpage(src, Int64(0),
            Int64(length(bytes)), limits; budget=budget)
        @test Parquet._budgetused(budget) == 0
        return
    end
    rejectlargestatistics()
    GC.gc()
    @test @allocated(rejectlargestatistics()) < 10_000
end

@testset "page CRC32" begin
    payload = collect(codeunits("123456789"))
    @test Parquet.pagechecksum(payload) == 0xcbf43926
    page = pagebytes(payload; v1=pagev1header(0))
    frame = readframe(page)
    @test frame.header.crc == reinterpret(Int32, 0xcbf43926) && frame.header.crc < 0
    @test collect(frame.payload) == payload
    corrupted = copy(page)
    corrupted[end] ⊻= 0x01
    @test_throws Parquet.FormatError readframe(corrupted)
    wrongcrc = pagebytes(payload; v1=pagev1header(0), crc=Int32(0))
    @test_throws Parquet.FormatError readframe(wrongcrc)
    nocrc = pagebytes(payload; v1=pagev1header(0), crc=:none)
    @test readframe(nocrc).header.crc === nothing
    damaged = copy(nocrc)
    damaged[end] ⊻= 0x01
    @test collect(readframe(damaged).payload) != payload
    empty = pagebytes(UInt8[]; v1=pagev1header(0))
    @test readframe(empty).header.crc == 0 && isempty(readframe(empty).payload)

    src = Parquet.source(page)
    probe = Parquet._LiveByteBudget(Parquet.Limits())
    probeframe = Parquet.readpage(src, Int64(0), Int64(length(page)),
        Parquet.Limits(); budget=probe)
    retained = Parquet._budgetused(probe)
    scratch = Parquet._pagechecksumscratch(probeframe.payload)
    Parquet._release!(probe, probeframe.materializedcharge)
    constrainedlimits = Parquet.Limits(
        max_materialized_bytes=retained + scratch - 1)
    constrained = Parquet._LiveByteBudget(constrainedlimits)
    @test_throws Parquet.LimitError Parquet.readpage(src, Int64(0),
        Int64(length(page)), constrainedlimits; budget=constrained)
    @test Parquet._budgetused(constrained) == 0
    exactlimits = Parquet.Limits(max_materialized_bytes=retained + scratch)
    exact = Parquet._LiveByteBudget(exactlimits)
    exactframe = Parquet.readpage(src, Int64(0), Int64(length(page)),
        exactlimits; budget=exact)
    @test Parquet._budgetused(exact) == exactframe.materializedcharge == retained
    Parquet._release!(exact, exactframe.materializedcharge)
    @test Parquet._budgetused(exact) == 0
    Parquet.close!(src)
end

@testset "page type and header consistency" begin
    payload = Parquet.encode_plain(Int32[7])
    v1 = pagev1header(1)
    index = MD.IndexPageHeader()
    dict = MD.DictionaryPageHeader(num_values=Int32(1), encoding=MD.Encoding.PLAIN)
    v2 = pagev2header(1)
    @test Parquet.pagekind(readframe(pagebytes(payload; type=MD.PageType.DICTIONARY_PAGE, dict=dict))) === :dictionary
    @test Parquet.pagekind(readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2, v2=v2))) === :data_v2
    @test Parquet.pagekind(readframe(pagebytes(payload; type=MD.PageType.INDEX_PAGE,
        index=index))) === :index
    unknown = readframe(pagebytes(payload; type=MD.PageType.T(9)))
    @test Parquet.pagekind(unknown) === :unknown && Parquet.pageend(unknown) == length(pagebytes(payload; type=MD.PageType.T(9)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE, dict=dict))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE, v1=v1, v2=v2))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE, v1=v1, dict=dict))
    @test_throws Parquet.FormatError readframe(pagebytes(payload;
        type=MD.PageType.DATA_PAGE, v1=v1, index=index))
    @test_throws Parquet.FormatError readframe(pagebytes(payload;
        type=MD.PageType.INDEX_PAGE))
    @test_throws Parquet.FormatError readframe(pagebytes(payload;
        type=MD.PageType.INDEX_PAGE, index=index, v1=v1))
    @test_throws Parquet.FormatError readframe(pagebytes(payload;
        type=MD.PageType.INDEX_PAGE, index=index, dict=dict))
    @test_throws Parquet.FormatError readframe(pagebytes(payload;
        type=MD.PageType.INDEX_PAGE, index=index, v2=v2))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DICTIONARY_PAGE, v1=v1))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DICTIONARY_PAGE))
    @test_throws Parquet.FormatError readframe(pagebytes(payload;
        type=MD.PageType.DICTIONARY_PAGE, dict=dict, index=index))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2, v1=v1))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2, v2=v2, dict=dict))
    @test_throws Parquet.FormatError readframe(pagebytes(payload;
        type=MD.PageType.DATA_PAGE_V2, v2=v2, index=index))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(-1)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(1; nulls=-1)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(1; nulls=2)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(1; rows=-1)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(1; rows=2)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(1; definition=-1)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(1; repetition=-1)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(1; definition=length(payload) + 1)))
    @test_throws Parquet.FormatError readframe(pagebytes(payload; type=MD.PageType.DATA_PAGE_V2,
        v2=pagev2header(1; repetition=length(payload) + 1)))
    oversized = pagebytes(payload; type=MD.PageType.T(9), compressed=length(payload) + 1)
    @test_throws Parquet.FormatError readframe(oversized)
end

@testset "page decompression gate" begin
    payload = Parquet.encode_plain(Int32[1, 2])
    frame = readframe(pagebytes(payload; v1=pagev1header(2)))
    @test collect(Parquet.decompresspage(frame, MD.CompressionCodec.UNCOMPRESSED)) == payload
    @test_throws Parquet.FormatError Parquet.decompresspage(frame, MD.CompressionCodec.SNAPPY)
    @test_throws Parquet.FormatError Parquet.decompresspage(frame, MD.CompressionCodec.ZSTD)
    @test_throws Parquet.FormatError Parquet.decompresspage(frame, MD.CompressionCodec.T(99))
    mismatch = readframe(pagebytes(payload; v1=pagev1header(2), uncompressed=length(payload) + 1))
    @test_throws Parquet.FormatError Parquet.decompresspage(mismatch, MD.CompressionCodec.UNCOMPRESSED)
end

@testset "page mutation fuzz" begin
    page = plainpage(Int32[1, 2, 3, 4])
    rng = MersenneTwister(2026)
    outcomes = Set{Symbol}()
    for trial in 1:600
        mutated = copy(page)
        for _ in 1:rand(rng, 1:3)
            mutated[rand(rng, eachindex(mutated))] = rand(rng, UInt8)
        end
        result = try
            readframe(mutated)
            :ok
        catch err
            err
        end
        if result === :ok
            push!(outcomes, :ok)
        else
            @test result isa Union{Parquet.FormatError,Parquet.LimitError}
            push!(outcomes, nameof(typeof(result)))
        end
    end
    @test :FormatError in outcomes
    for n in 0:(length(page) - 1)
        @test_throws Parquet.FormatError readframe(page[1:n])
    end
end

# Page summaries of one column chunk: (kind, num_values, crc present, crc matches, sizes equal) or the error.
function corpuspages(path::String, column::Int)
    file = Parquet.File(path)
    meta = TH.decode(file.footer.bytes, MD.FileMetaData)
    md = meta.row_groups[1].columns[column].meta_data
    start = Int64(md.data_page_offset)
    stop = start + Int64(md.total_compressed_size)
    pages = []
    position = start
    while position < stop
        frame = try
            Parquet.readpage(file.source, position, stop, Parquet.Limits())
        catch err
            err
        end
        frame isa Exception && (push!(pages, frame); break)
        header = frame.header
        push!(pages, (Parquet.pagekind(frame), Int(header.data_page_header.num_values), header.crc !== nothing,
            header.crc !== nothing && Parquet.pagechecksum(frame.payload) == reinterpret(UInt32, header.crc),
            header.compressed_page_size == header.uncompressed_page_size))
        position = Parquet.pageend(frame)
    end
    close(file)
    return pages
end

@testset "official checksum fixtures" begin
    if !isdir(pagecorpus())
        @warn "parquet-testing corpus not found; skipping page corpus tests" PAGE_CORPUS
    else
        for column in 1:2
            pages = corpuspages(pagecorpus("datapage_v1-uncompressed-checksum.parquet"), column)
            @test length(pages) == 2 && all(page -> page isa Tuple, pages)
            @test all(page -> page[1] === :data_v1 && page[3] && page[4] && page[5], pages)
            @test sum(page -> page[2], pages) == 5120
        end
        # README: column a has a bad CRC on page 0, column b on page 1
        first = corpuspages(pagecorpus("datapage_v1-corrupt-checksum.parquet"), 1)
        @test length(first) == 1 && first[1] isa Parquet.FormatError
        second = corpuspages(pagecorpus("datapage_v1-corrupt-checksum.parquet"), 2)
        @test length(second) == 2 && second[1] isa Tuple && second[1][4] && second[2] isa Parquet.FormatError
        tiny = corpuspages(pagecorpus("alltypes_tiny_pages_plain.parquet"), 1)
        @test length(tiny) == 325 && all(page -> page isa Tuple && page[1] === :data_v1 && !page[3], tiny)
        @test sum(page -> page[2], tiny) == 7300
    end
end
