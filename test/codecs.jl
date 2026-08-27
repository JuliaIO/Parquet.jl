using Random

if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end
if !isdefined(Parquet, :decompress)
    Base.include(Parquet, joinpath(@__DIR__, "..", "src", "codecs.jl"))
end

const CODEC_CORPUS = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))
const CODEC_LARGE_PAGES = get(ENV, "PARQUET_TEST_LARGE_PAGES", "0") == "1"
const CC = MD.CompressionCodec
const WRITABLE_CODECS = (CC.UNCOMPRESSED, CC.SNAPPY, CC.GZIP, CC.BROTLI, CC.ZSTD, CC.LZ4_RAW)
const COMPRESSING_CODECS = (CC.SNAPPY, CC.GZIP, CC.BROTLI, CC.ZSTD, CC.LZ4_RAW)

struct ShiftedCodecBytes <: AbstractVector{UInt8}
    bytes::Vector{UInt8}
end

function Base.IndexStyle(::Type{ShiftedCodecBytes})
    return IndexLinear()
end

function Base.size(bytes::ShiftedCodecBytes)
    return (length(bytes.bytes),)
end

function Base.axes(bytes::ShiftedCodecBytes)
    return (2:(length(bytes.bytes) + 1),)
end

function Base.getindex(bytes::ShiftedCodecBytes, index::Int)
    checkbounds(bytes, index)
    return bytes.bytes[index - 1]
end

function codeccorpus(parts...)
    return joinpath(CODEC_CORPUS, "data", parts...)
end

# Bytes with mixed entropy: runs, text, and random noise.
function codecsample(rng::AbstractRNG, count::Int)
    output = UInt8[]
    while length(output) < count
        kind = rand(rng, 1:3)
        kind == 1 && append!(output, fill(rand(rng, UInt8), rand(rng, 1:64)))
        kind == 2 && append!(output, codeunits("parquet page "))
        kind == 3 && append!(output, rand(rng, UInt8, rand(rng, 1:32)))
    end
    return output[1:count]
end

function bigendian32(value::Integer)
    return reinterpret(UInt8, [hton(UInt32(value))])
end

# One Hadoop LZ4 block can contain multiple compressed chunks.
function hadoopblock(chunks::Vector{Vector{UInt8}})
    output = UInt8[]
    append!(output, bigendian32(sum(length, chunks)))
    for chunk in chunks
        block = Parquet.compress(CC.LZ4_RAW, chunk)
        append!(output, bigendian32(length(block)))
        append!(output, block)
    end
    return output
end

function hadoopframe(chunks::Vector{Vector{UInt8}})
    return vcat((hadoopblock([chunk]) for chunk in chunks)...)
end

# Decompressed pages of one column chunk: (kind, uncompressed level bytes, data bytes, header, metadata).
function codeccorpuspages(path::String, column::Int; limits=Parquet.Limits())
    file = Parquet.File(path)
    meta = TH.decode(copy(file.footer.bytes), MD.FileMetaData)
    md = meta.row_groups[1].columns[column].meta_data
    start = Int64(md.data_page_offset)
    dictionary = md.dictionary_page_offset
    dictionary !== nothing && dictionary > 0 && (start = min(start, Int64(dictionary)))
    stop = start + md.total_compressed_size
    pages = Any[]
    position = start
    while position < stop
        frame = Parquet.readpage(file.source, position, stop, limits)
        header = frame.header
        v2 = header.data_page_header_v2
        if v2 !== nothing
            levellength = Int(v2.definition_levels_byte_length + v2.repetition_levels_byte_length)
            payload = collect(frame.payload)
            encoded = payload[(levellength + 1):end]
            expected = header.uncompressed_page_size - levellength
            compressed = something(v2.is_compressed, true)
            data = isempty(encoded) && expected == 0 ? UInt8[] :
                Parquet.decompress(compressed ? md.codec : CC.UNCOMPRESSED, encoded, expected; limits=limits)
            push!(pages, (kind=:v2, levels=payload[1:levellength], data=data, header=header, md=md))
        else
            kind = header.dictionary_page_header !== nothing ? :dict : :v1
            data = collect(Parquet.decompress(md.codec, frame.payload, header.uncompressed_page_size; limits=limits))
            push!(pages, (kind=kind, levels=UInt8[], data=data, header=header, md=md))
        end
        position = Parquet.pageend(frame)
    end
    close(file)
    return pages
end

# Data section of a V1 page after its length-prefixed RLE definition levels.
function afterlevels(data::Vector{UInt8})
    levellength = Int(reinterpret(UInt32, data[1:4])[1])
    return data[(5 + levellength):end]
end

@testset "codec table" begin
    @test all(Parquet.codecreadable, (CC.UNCOMPRESSED, CC.SNAPPY, CC.GZIP, CC.BROTLI, CC.LZ4, CC.ZSTD, CC.LZ4_RAW))
    @test !Parquet.codecreadable(CC.LZO) && !Parquet.codecreadable(CC.T(42))
    @test all(Parquet.codecwritable, WRITABLE_CODECS)
    @test !Parquet.codecwritable(CC.LZ4) && !Parquet.codecwritable(CC.LZO) && !Parquet.codecwritable(CC.T(42))
    @test Parquet.codecname(CC.ZSTD) == "ZSTD" && Parquet.codecname(CC.T(42)) == "CompressionCodec.T(42)"
    @test Parquet._readbe32(fill(UInt8(0xff), 4), 1) == Int64(typemax(UInt32))
    @test_throws Parquet.FormatError Parquet.decompress(CC.LZO, UInt8[0x00], 1)
    @test_throws Parquet.FormatError Parquet.decompress(CC.T(42), UInt8[0x00], 1)
    @test_throws ArgumentError Parquet.compress(CC.LZO, UInt8[0x00])
    @test_throws ArgumentError Parquet.compress(CC.LZ4, UInt8[0x00])
    @test_throws ArgumentError Parquet.compress(CC.T(42), UInt8[0x00])
    @test_throws ArgumentError Parquet.compress(CC.UNCOMPRESSED, UInt8[0x00]; level=1)
end

@testset "codec round trips" begin
    rng = MersenneTwister(2026)
    samples = [UInt8[], UInt8[0x2a], zeros(UInt8, 255), rand(rng, UInt8, 4096), codecsample(rng, 100_000),
        collect(codeunits(repeat("parquet", 5000)))]
    for codec in WRITABLE_CODECS, sample in samples
        encoded = Parquet.compress(codec, sample)
        @test encoded isa Vector{UInt8}
        decoded = Parquet.decompress(codec, encoded, length(sample))
        @test decoded == sample && decoded isa Vector{UInt8}
        codec == CC.UNCOMPRESSED && @test encoded == sample && encoded !== sample
        padded = vcat(UInt8[0xff], encoded, UInt8[0xee])
        @test Parquet.decompress(codec, view(padded, 2:(1 + length(encoded))), length(sample)) == sample
        slice = Parquet.readrange(Parquet.source(padded), 1, length(encoded))
        @test Parquet.decompress(codec, slice, length(sample)) == sample
        @test Parquet.decompress(codec, Parquet.compress(codec, view(padded, 2:(1 + length(encoded)))), length(encoded)) == encoded
    end
    sample = codecsample(rng, 20_000)
    for (codec, levels) in ((CC.GZIP, (0, 1, 9)), (CC.ZSTD, (-5, 1, 19)), (CC.BROTLI, (0, 5, 11)), (CC.LZ4_RAW, (0, 1, 12)))
        for level in levels
            @test Parquet.decompress(codec, Parquet.compress(codec, sample; level=level), length(sample)) == sample
        end
    end
    @test_throws ArgumentError Parquet.compress(CC.GZIP, sample; level=10)
    @test_throws ArgumentError Parquet.compress(CC.BROTLI, sample; level=12)
    @test_throws ArgumentError Parquet.compress(CC.LZ4_RAW, sample; level=-1)
    @test_throws ArgumentError Parquet.compress(CC.ZSTD, sample; level=23)
    @test_throws ArgumentError Parquet.compress(CC.SNAPPY, sample; level=1)
    bytes = UInt8[1, 2, 3]
    @test Parquet.decompress(CC.UNCOMPRESSED, bytes, 3) === bytes
    @test_throws Parquet.FormatError Parquet.decompress(CC.UNCOMPRESSED, bytes, 2)
    @test_throws Parquet.FormatError Parquet.decompress(CC.UNCOMPRESSED, bytes, 4)
end

@testset "expected size enforcement" begin
    rng = MersenneTwister(7)
    sample = codecsample(rng, 3000)
    for codec in WRITABLE_CODECS
        encoded = Parquet.compress(codec, sample)
        for wrong in (length(sample) - 1, length(sample) + 1, 0, 1, 2 * length(sample))
            @test_throws Parquet.FormatError Parquet.decompress(codec, encoded, wrong)
        end
        @test_throws Parquet.FormatError Parquet.decompress(codec, encoded, -1)
        @test_throws Parquet.LimitError Parquet.decompress(codec, encoded, length(sample);
            limits=Parquet.Limits(max_page_bytes=length(sample) - 1))
        @test_throws Parquet.LimitError Parquet.decompress(codec, encoded, length(sample);
            limits=Parquet.Limits(max_page_bytes=min(length(sample), length(encoded)) - 1))
        @test Parquet.decompress(codec, encoded, length(sample);
            limits=Parquet.Limits(max_page_bytes=max(length(sample), length(encoded)))) == sample
        @test_throws Parquet.LimitError Parquet.decompress(codec, encoded, typemax(Int64))
        @test_throws Parquet.LimitError Parquet.decompress(codec, encoded, Int64(2)^40)
    end
    for codec in (CC.SNAPPY, CC.GZIP, CC.BROTLI, CC.ZSTD, CC.LZ4_RAW, CC.LZ4)
        @test_throws Parquet.FormatError Parquet.decompress(codec, UInt8[], 0)
        @test_throws Parquet.FormatError Parquet.decompress(codec, UInt8[], 1)
    end
    for codec in COMPRESSING_CODECS
        empty = Parquet.compress(codec, UInt8[])
        @test !isempty(empty) && Parquet.decompress(codec, empty, 0) == UInt8[]
        @test_throws Parquet.FormatError Parquet.decompress(codec, empty, 1)
    end
    strided = view(UInt8[1, 9, 2, 9, 3], 1:2:5)
    @test Parquet.decompress(CC.SNAPPY, Parquet.compress(CC.SNAPPY, strided), 3) == UInt8[1, 2, 3]

    shiftedencoded = Parquet.compress(CC.SNAPPY, sample)
    shiftedsrc = Parquet.source(ShiftedCodecBytes(shiftedencoded))
    shiftedslice = Parquet.readrange(shiftedsrc, 0, length(shiftedencoded))
    copycharge = Parquet._contiguouscopycharge(shiftedslice)
    constrained = Parquet._LiveByteBudget(Parquet.Limits(
        max_materialized_bytes=copycharge - 1))
    @test_throws Parquet.LimitError Parquet.decompress(CC.SNAPPY,
        shiftedslice, length(sample); budget=constrained)
    @test Parquet._budgetused(constrained) == 0
    sufficient = Parquet._LiveByteBudget(Parquet.Limits(
        max_materialized_bytes=copycharge))
    @test Parquet.decompress(CC.SNAPPY, shiftedslice, length(sample);
        budget=sufficient) == sample
    @test Parquet._budgetused(sufficient) == 0
    Parquet.close!(shiftedsrc)
    closedsource = Parquet.source(Parquet.compress(CC.ZSTD, sample))
    slice = Parquet.readrange(closedsource, 0, Parquet.sourcelength(closedsource))
    Parquet.close!(closedsource)
    @test_throws ArgumentError Parquet.decompress(CC.ZSTD, slice, length(sample))
end

@testset "malformed and truncated streams" begin
    rng = MersenneTwister(11)
    sample = codecsample(rng, 2000)
    for codec in COMPRESSING_CODECS
        encoded = Parquet.compress(codec, sample)
        for n in 0:(length(encoded) - 1)
            @test_throws Parquet.FormatError Parquet.decompress(codec, encoded[1:n], length(sample))
        end
        @test_throws Parquet.FormatError Parquet.decompress(codec, vcat(encoded, UInt8[0x00]), length(sample))
        @test_throws Parquet.FormatError Parquet.decompress(codec, vcat(encoded, encoded), length(sample))
        @test_throws Parquet.FormatError Parquet.decompress(codec, rand(rng, UInt8, 64), 64)
        @test_throws Parquet.FormatError Parquet.decompress(codec, zeros(UInt8, 64), 64)
        @test_throws Parquet.FormatError Parquet.decompress(codec, fill(0xff, 64), 64)
        outcomes = Set{Symbol}()
        for trial in 1:300
            mutated = copy(encoded)
            for _ in 1:rand(rng, 1:3)
                mutated[rand(rng, eachindex(mutated))] = rand(rng, UInt8)
            end
            result = try
                Parquet.decompress(codec, mutated, length(sample))
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
    end
end

@testset "concatenated members" begin
    rng = MersenneTwister(5)
    a = codecsample(rng, 1500)
    b = rand(rng, UInt8, 700)
    for codec in (CC.GZIP, CC.ZSTD)
        joined = vcat(Parquet.compress(codec, a), Parquet.compress(codec, b))
        @test Parquet.decompress(codec, joined, length(a) + length(b)) == vcat(a, b)
        @test_throws Parquet.FormatError Parquet.decompress(codec, joined, length(a))
        @test_throws Parquet.FormatError Parquet.decompress(codec, joined, length(a) + length(b) + 1)
        triple = vcat(joined, Parquet.compress(codec, UInt8[]))
        @test Parquet.decompress(codec, triple, length(a) + length(b)) == vcat(a, b)
    end
    for codec in (CC.BROTLI, CC.SNAPPY, CC.LZ4_RAW)
        joined = vcat(Parquet.compress(codec, a), Parquet.compress(codec, b))
        @test_throws Parquet.FormatError Parquet.decompress(codec, joined, length(a) + length(b))
    end
end

@testset "deprecated LZ4 framing" begin
    rng = MersenneTwister(9)
    chunks = [codecsample(rng, 70_000), rand(rng, UInt8, 1000), UInt8[0x01], codecsample(rng, 300)]
    whole = vcat(chunks...)
    framed = hadoopframe(chunks)
    @test Parquet.decompress(CC.LZ4, framed, length(whole)) == whole
    @test Parquet.decompress(CC.LZ4, hadoopframe([whole]), length(whole)) == whole
    @test Parquet.decompress(CC.LZ4, hadoopblock(chunks), length(whole)) == whole
    single = hadoopframe([chunks[2]])
    @test Parquet.decompress(CC.LZ4, single, 1000) == chunks[2]
    raw = Parquet.compress(CC.LZ4_RAW, whole)
    @test Parquet.decompress(CC.LZ4, raw, length(whole)) == whole
    emptypair = zeros(UInt8, 8)
    @test Parquet.decompress(CC.LZ4, vcat(emptypair, single), 1000) == chunks[2]
    @test Parquet.decompress(CC.LZ4, vcat(single, emptypair), 1000) == chunks[2]
    @test Parquet.decompress(CC.LZ4, emptypair, 0) == UInt8[]
    @test Parquet.decompress(CC.LZ4, bigendian32(0), 0) == UInt8[]
    @test Parquet.decompress(CC.LZ4,
        vcat(bigendian32(0), bigendian32(1), UInt8[0x00]), 0) == UInt8[]
    @test_throws Parquet.FormatError Parquet.decompress(CC.LZ4, vcat(single, UInt8[0x00]), 1000)
    @test_throws Parquet.FormatError Parquet.decompress(CC.LZ4, vcat(single, UInt8[0, 0, 0, 1, 0, 0, 0, 0]), 1000)
    @test_throws Parquet.FormatError Parquet.decompress(CC.LZ4, single, 999)
    @test_throws Parquet.FormatError Parquet.decompress(CC.LZ4, single, 1001)
    @test_throws Parquet.FormatError Parquet.decompress(CC.LZ4, single, 0)
    for n in 0:(length(single) - 1)
        @test_throws Parquet.FormatError Parquet.decompress(CC.LZ4, single[1:n], 1000)
    end
    badoriginal = copy(single)
    badoriginal[4] = 0xff
    @test_throws Parquet.FormatError Parquet.decompress(CC.LZ4, badoriginal, 1000)
    badcompressed = copy(single)
    badcompressed[8] ⊻= 0x01
    @test_throws Parquet.FormatError Parquet.decompress(CC.LZ4, badcompressed, 1000)
    @test_throws Parquet.LimitError Parquet.decompress(CC.LZ4, framed, length(whole); limits=Parquet.Limits(max_page_bytes=1000))
    outcomes = Set{Symbol}()
    for trial in 1:300
        mutated = copy(framed)
        for _ in 1:rand(rng, 1:3)
            mutated[rand(rng, eachindex(mutated))] = rand(rng, UInt8)
        end
        result = try
            Parquet.decompress(CC.LZ4, mutated, length(whole))
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
end

@testset "official corpus codec fixtures" begin
    if !isdir(codeccorpus())
        @warn "parquet-testing corpus not found; skipping codec corpus tests" CODEC_CORPUS
    else
        # SNAPPY: the compressed checksum fixture must decompress to its uncompressed twin, page by page
        snappy = [codeccorpuspages(codeccorpus("datapage_v1-snappy-compressed-checksum.parquet"), column) for column in 1:2]
        plain = [codeccorpuspages(codeccorpus("datapage_v1-uncompressed-checksum.parquet"), column) for column in 1:2]
        @test [[page.data for page in column] for column in snappy] == [[page.data for page in column] for column in plain]
        @test all(page -> page.kind === :v1 && page.header.crc !== nothing && length(page.data) == 10240, snappy[1])
        a = vcat([Parquet.decode_plain(Int32, page.data, Int(page.header.data_page_header.num_values))[1] for page in snappy[1]]...)
        @test length(a) == 5120 && sum(Int64, a) == 43118090240 && a[1:4] == Int32[50462976, 117835012, 185207048, 252579084]
        snappytable = Parquet.Table(codeccorpus("datapage_v1-snappy-compressed-checksum.parquet"))
        @test length(snappytable) == 5120
        @test sum(Int64, snappytable.columns.a) == 43118090240
        @test sum(Int64, snappytable.columns.b) == 129016125440
        close(snappytable)
        types = codeccorpuspages(codeccorpus("alltypes_plain.snappy.parquet"), 1)
        @test [(page.kind, length(page.data)) for page in types] == [(:dict, 8), (:v1, 9)]
        @test [(page.kind, length(page.data)) for page in codeccorpuspages(codeccorpus("alltypes_plain.snappy.parquet"), 11)] == [(:dict, 24), (:v1, 9)]
        v2 = codeccorpuspages(codeccorpus("datapage_v2.snappy.parquet"), 5)
        @test [(page.kind, length(page.levels), length(page.data)) for page in v2] == [(:dict, 0, 12), (:v2, 8, 4)]
        @test v2[2].header.data_page_header_v2.repetition_levels_byte_length == 3
        emptyv2 = codeccorpuspages(codeccorpus("datapage_v2_empty_datapage.snappy.parquet"), 1)
        @test [(page.kind, length(page.levels), length(page.data)) for page in emptyv2] == [(:v2, 2, 0)]
        nested = codeccorpuspages(codeccorpus("nested_lists.snappy.parquet"), 1)
        @test [(page.kind, length(page.data)) for page in nested] == [(:dict, 30), (:v1, 33)]
        # GZIP: concatenated members and V2 pages
        gzip = codeccorpuspages(codeccorpus("concatenated_gzip_members.parquet"), 1)[1]
        @test gzip.kind === :v2 && length(gzip.levels) == 3 && reinterpret(Int64, gzip.data) == 1:513
        booleans = codeccorpuspages(codeccorpus("rle_boolean_encoding.parquet"), 1)[1]
        @test booleans.kind === :v2 && length(booleans.levels) == 13 && length(booleans.data) == 13
        @test booleans.header.data_page_header_v2.repetition_levels_byte_length == 2 && booleans.data[1:4] == UInt8[0x09, 0x00, 0x00, 0x00]
        for (plaincolumn, splitcolumn, T) in ((3, 4, Float32), (5, 6, Float64), (7, 8, Int32), (9, 10, Int64))
            plainpage = codeccorpuspages(codeccorpus("byte_stream_split_extended.gzip.parquet"), plaincolumn)[1]
            splitpage = codeccorpuspages(codeccorpus("byte_stream_split_extended.gzip.parquet"), splitcolumn)[1]
            plainvalues = Parquet.decode_plain(T, afterlevels(plainpage.data), 200)[1]
            splitvalues = Parquet.decode_byte_stream_split(T, afterlevels(splitpage.data), 200)[1]
            @test reinterpret(Parquet._splitbits(T), splitvalues) == reinterpret(Parquet._splitbits(T), plainvalues)
        end
        # BROTLI: small pages decode; the 1 GiB dictionary page is rejected by the default limit before allocation
        value = codeccorpuspages(codeccorpus("large_string_map.brotli.parquet"), 2)
        @test [(page.kind, length(page.data)) for page in value] == [(:dict, 4), (:v1, 15)]
        @test reinterpret(Int32, value[1].data) == Int32[1]
        @test_throws Parquet.LimitError codeccorpuspages(codeccorpus("large_string_map.brotli.parquet"), 1)
        @test_throws Parquet.LimitError Parquet.decompress(CC.BROTLI, UInt8[0x00], 1073741828)
        if CODEC_LARGE_PAGES
            key = codeccorpuspages(codeccorpus("large_string_map.brotli.parquet"), 1; limits=Parquet.Limits(max_page_bytes=Int64(2)^31))
            @test [(page.kind, length(page.data)) for page in key] == [(:dict, 1073741828), (:v1, 15), (:v1, 1073741840)]
        end
        # ZSTD
        delta = codeccorpuspages(codeccorpus("delta_length_byte_array.parquet"), 1)[1]
        @test delta.kind === :v2 && length(delta.levels) == 3 && length(delta.data) == 23711
        @test Parquet.decode_delta_length_byte_array(delta.data, 1000)[1] == [Vector{UInt8}(codeunits("apple_banana_mango$((index - 1)^2)")) for index in 1:1000]
        split = codeccorpuspages(codeccorpus("byte_stream_split.zstd.parquet"), 2)[1]
        doubles = Parquet.decode_byte_stream_split(Float64, afterlevels(split.data), 300)[1]
        @test minimum(doubles) == reinterpret(Float64, split.md.statistics.min_value)[1]
        @test maximum(doubles) == reinterpret(Float64, split.md.statistics.max_value)[1]
        alp = codeccorpuspages(codeccorpus("alp_extended.zstd.parquet"), 1)[1]
        @test alp.kind === :v1 && length(alp.data) == 24583 && length(afterlevels(alp.data)) == 4 * 6144
        emptyzstd = codeccorpuspages(codeccorpus("page_v2_empty_compressed.parquet"), 1)
        @test [(page.kind, length(page.levels), length(page.data)) for page in emptyzstd] == [(:dict, 0, 0), (:v2, 2, 1)]
        @test emptyzstd[1].header.compressed_page_size == 9 && emptyzstd[1].header.uncompressed_page_size == 0
        @test emptyzstd[2].data == UInt8[0x00] && emptyzstd[2].header.data_page_header_v2.num_nulls == 10
        # LZ4_RAW and the deprecated LZ4 codec decode the same data
        raw = codeccorpuspages(codeccorpus("lz4_raw_compressed_larger.parquet"), 1)
        hadoop = codeccorpuspages(codeccorpus("hadoop_lz4_compressed_larger.parquet"), 1)
        @test length(raw) == 1 && length(raw[1].data) == 400000 && raw[1].data == hadoop[1].data
        @test raw[1].md.codec == CC.LZ4_RAW && hadoop[1].md.codec == CC.LZ4
        small = codeccorpuspages(codeccorpus("lz4_raw_compressed.parquet"), 1)
        @test [(page.kind, length(page.data)) for page in small] == [(:v1, 32)]
        @test reinterpret(Int64, small[1].data) == Int64[1593604800, 1593604800, 1593604801, 1593604801]
        @test [length(codeccorpuspages(codeccorpus("lz4_raw_compressed.parquet"), column)[1].data) for column in 2:3] == [28, 38]
        for column in 1:3
            framed = codeccorpuspages(codeccorpus("hadoop_lz4_compressed.parquet"), column)
            bare = codeccorpuspages(codeccorpus("non_hadoop_lz4_compressed.parquet"), column)
            @test [page.kind for page in framed] == [:dict, :v1] == [page.kind for page in bare]
            @test framed[1].data == bare[1].data && length(framed[1].data) == framed[1].header.uncompressed_page_size
        end
        @test reinterpret(Int64, codeccorpuspages(codeccorpus("hadoop_lz4_compressed.parquet"), 1)[1].data) == Int64[1593604800, 1593604801]
        for fixture in ("hadoop_lz4_compressed.parquet", "non_hadoop_lz4_compressed.parquet",
            "lz4_raw_compressed.parquet")
            table = Parquet.Table(codeccorpus(fixture))
            @test table.columns.c0 == Int64[1593604800, 1593604800, 1593604801, 1593604801]
            @test table.columns.c1 == Vector{UInt8}[collect(codeunits(value)) for value in ("abc", "def", "abc", "def")]
            @test isequal(table.columns.v11, Union{Missing,Float64}[42.0, 7.7, 42.125, 7.7])
            close(table)
        end
        # seeded mutations of a real SNAPPY page payload
        file = Parquet.File(codeccorpus("datapage_v1-snappy-compressed-checksum.parquet"))
        meta = TH.decode(copy(file.footer.bytes), MD.FileMetaData)
        md = meta.row_groups[1].columns[1].meta_data
        frame = Parquet.readpage(file.source, Int64(md.data_page_offset), Int64(md.data_page_offset + md.total_compressed_size), Parquet.Limits())
        payload = collect(frame.payload)
        close(file)
        rng = MersenneTwister(99)
        for trial in 1:300
            mutated = copy(payload)
            for _ in 1:rand(rng, 1:2)
                mutated[rand(rng, eachindex(mutated))] = rand(rng, UInt8)
            end
            result = try
                Parquet.decompress(CC.SNAPPY, mutated, 10240)
                :ok
            catch err
                err
            end
            @test result === :ok || result isa Union{Parquet.FormatError,Parquet.LimitError}
        end
    end
end
