using Random

if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end

const BSS_CORPUS = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))

function bsscorpus(parts...)
    return joinpath(BSS_CORPUS, "data", parts...)
end

function bssbits(values::AbstractVector{T}) where {T}
    return reinterpret(Parquet._splitbits(T), values)
end

# Uncompressed V1 data page of a column chunk: (bytes after the RLE definition levels, value count, chunk metadata).
function bssfixturepage(path::String, column::Int)
    file = Parquet.File(path)
    meta = TH.decode(copy(file.footer.bytes), MD.FileMetaData)
    close(file)
    bytes = read(path)
    chunk = meta.row_groups[1].columns[column]
    md = chunk.meta_data
    r = TH.Reader(bytes, md.data_page_offset + 1, length(bytes))
    header = TH.decode(r, MD.PageHeader)
    start = md.data_page_offset + TH.consumed(r) + 1
    compressed = view(bytes, start:(start + header.compressed_page_size - 1))
    page = Parquet.decompress(md.codec, compressed, header.uncompressed_page_size)
    levellength = Int(reinterpret(UInt32, page[1:4])[1])
    data = page[(5 + levellength):end]
    return data, header.data_page_header.num_values, md, meta.schema[column + 1]
end

@testset "BYTE_STREAM_SPLIT specification example" begin
    raw = UInt8[0xaa, 0xbb, 0xcc, 0xdd, 0x00, 0x11, 0x22, 0x33, 0xa3, 0xb4, 0xc5, 0xd6]
    split = UInt8[0xaa, 0x00, 0xa3, 0xbb, 0x11, 0xb4, 0xcc, 0x22, 0xc5, 0xdd, 0x33, 0xd6]
    ints = collect(reinterpret(Int32, raw))
    floats = collect(reinterpret(Float32, raw))
    @test Parquet.encode_byte_stream_split(ints) == split
    @test Parquet.encode_byte_stream_split(floats) == split
    @test Parquet.decode_byte_stream_split(Int32, split, 3) == (ints, 13)
    @test bssbits(Parquet.decode_byte_stream_split(Float32, split, 3)[1]) == bssbits(floats)
    matrix = reshape(raw, 4, 3)
    @test Parquet.encode_byte_stream_split_fixed(matrix) == split
    @test Parquet.decode_byte_stream_split_fixed(split, 3, 4) == (matrix, 13)
    @test Parquet.decode_byte_stream_split(Int32, vcat(UInt8[0x00], split, UInt8[0xff]), 3; offset=2) == (ints, 14)
end

@testset "BYTE_STREAM_SPLIT bit patterns" begin
    f32 = collect(reinterpret(Float32, UInt32[0x7fc00000, 0x7fc0dead, 0xffc00001, 0x80000000, 0x00000000, 0x00000001, 0x7f800000, 0xff800000, 0x3f800000]))
    @test bssbits(Parquet.decode_byte_stream_split(Float32, Parquet.encode_byte_stream_split(f32), 9)[1]) == bssbits(f32)
    f64 = collect(reinterpret(Float64, UInt64[0x7ff8000000000000, 0x7ff800deadbeef00, 0xfff8000000000001, 0x8000000000000000, 0x0000000000000000, 0x0000000000000001, 0x7ff0000000000000, 0xfff0000000000000]))
    @test bssbits(Parquet.decode_byte_stream_split(Float64, Parquet.encode_byte_stream_split(f64), 8)[1]) == bssbits(f64)
    for values in (Int32[typemin(Int32), -1, 0, 1, typemax(Int32)], Int64[typemin(Int64), -1, 0, 1, typemax(Int64)])
        @test Parquet.decode_byte_stream_split(eltype(values), Parquet.encode_byte_stream_split(values), 5)[1] == values
    end
    @test Parquet.encode_byte_stream_split(Float64[]) == UInt8[]
    @test Parquet.decode_byte_stream_split(Float64, UInt8[], 0) == (Float64[], 1)
    @test Parquet.decode_byte_stream_split(Int64, Parquet.encode_byte_stream_split(Int64[-2]), 1) == (Int64[-2], 9)
    @test_throws Parquet.FormatError Parquet.decode_byte_stream_split_fixed(UInt8[], 4, 0)
    @test_throws ArgumentError Parquet.encode_byte_stream_split_fixed(Matrix{UInt8}(undef, 0, 4))
    @test Parquet.decode_byte_stream_split_fixed(UInt8[], 0, 3) == (Matrix{UInt8}(undef, 3, 0), 1)
    @test Parquet.encode_byte_stream_split_fixed(Matrix{UInt8}(undef, 3, 0)) == UInt8[]
    half = reshape(UInt8[0x00, 0x3c, 0x00, 0xbc, 0x00, 0x7e, 0x01, 0x00], 2, 4)
    @test Parquet.encode_byte_stream_split_fixed(half) == UInt8[0x00, 0x00, 0x00, 0x01, 0x3c, 0xbc, 0x7e, 0x00]
    @test Parquet.decode_byte_stream_split_fixed(Parquet.encode_byte_stream_split_fixed(half), 4, 2)[1] == half
end

@testset "BYTE_STREAM_SPLIT randomized round trips" begin
    rng = MersenneTwister(4242)
    for T in (Int32, Int64, Float32, Float64), count in (1, 2, 3, 17, 256, 1001)
        values = T <: AbstractFloat ? collect(reinterpret(T, rand(rng, Parquet._splitbits(T), count))) : rand(rng, T, count)
        encoded = Parquet.encode_byte_stream_split(values)
        @test length(encoded) == count * sizeof(T)
        decoded, next = Parquet.decode_byte_stream_split(T, encoded, count)
        @test bssbits(decoded) == bssbits(values) && next == length(encoded) + 1
        padded = vcat(rand(rng, UInt8, 3), encoded, rand(rng, UInt8, 2))
        @test Parquet.decode_byte_stream_split(T, padded, count; offset=4)[2] == 4 + length(encoded)
        @test bssbits(Parquet.decode_byte_stream_split(T, view(padded, 4:(3 + length(encoded))), count)[1]) == bssbits(values)
        slice = Parquet.readrange(Parquet.source(padded), 3, length(encoded))
        @test bssbits(Parquet.decode_byte_stream_split(T, slice, count)[1]) == bssbits(values)
        output = Vector{T}(undef, count)
        @test Parquet.decode_byte_stream_split!(output, padded; offset=4) == 4 + length(encoded)
        @test bssbits(output) == bssbits(values)
    end
    for width in (1, 2, 5, 16), count in (1, 7, 300)
        matrix = rand(rng, UInt8, width, count)
        encoded = Parquet.encode_byte_stream_split_fixed(matrix)
        @test Parquet.decode_byte_stream_split_fixed(encoded, count, width) == (matrix, length(encoded) + 1)
        slice = Parquet.readrange(Parquet.source(encoded), 0, length(encoded))
        @test Parquet.decode_byte_stream_split_fixed(slice, count, width)[1] == matrix
    end
end

@testset "BYTE_STREAM_SPLIT malformed input and limits" begin
    F = Parquet.FormatError
    L = Parquet.LimitError
    encoded = Parquet.encode_byte_stream_split(Float64.(1:10))
    for n in 0:(length(encoded) - 1)
        @test_throws F Parquet.decode_byte_stream_split(Float64, encoded[1:n], 10)
    end
    @test_throws F Parquet.decode_byte_stream_split!(Vector{Int32}(undef, 4), UInt8[0x01, 0x02, 0x03])
    @test_throws F Parquet.decode_byte_stream_split(Float64, encoded, 10; offset=2)
    @test_throws ArgumentError Parquet.decode_byte_stream_split(Float64, encoded, -1)
    @test_throws L Parquet.decode_byte_stream_split(Float64, encoded, 10; limits=Parquet.Limits(max_container_elements=5))
    @test_throws L Parquet.decode_byte_stream_split(Float64, encoded, 10; limits=Parquet.Limits(max_page_bytes=79))
    @test_throws L Parquet.decode_byte_stream_split(Float64, encoded, big(typemax(Int64)) + 1)
    @test Parquet.decode_byte_stream_split(Float64, encoded, 10; limits=Parquet.Limits(max_page_bytes=80))[1] == Float64.(1:10)
    @test_throws F Parquet.decode_byte_stream_split_fixed(UInt8[], 1, 4)
    @test_throws F Parquet.decode_byte_stream_split_fixed(zeros(UInt8, 15), 4, 4)
    @test_throws L Parquet.decode_byte_stream_split_fixed(zeros(UInt8, 16), 4, 4; limits=Parquet.Limits(max_page_bytes=8))
    @test_throws L Parquet.decode_byte_stream_split_fixed(zeros(UInt8, 16), 4, 4; limits=Parquet.Limits(max_string_bytes=3))
    @test_throws L Parquet.decode_byte_stream_split_fixed(zeros(UInt8, 16), 4, 4; limits=Parquet.Limits(max_container_elements=3))
    @test_throws F Parquet.decode_byte_stream_split_fixed(zeros(UInt8, 16), 4, -1)
    @test_throws ArgumentError Parquet.decode_byte_stream_split_fixed(zeros(UInt8, 16), -4, 1)
    unbounded = Parquet.Limits(max_container_elements=typemax(Int64), max_string_bytes=typemax(Int64), max_page_bytes=typemax(Int64))
    @test_throws F Parquet.decode_byte_stream_split_fixed(UInt8[], typemax(Int) ÷ 2, 4; limits=unbounded)
    @test_throws F Parquet.decode_byte_stream_split_fixed(UInt8[], 4, typemax(Int) ÷ 2; limits=unbounded)
    @test_throws L Parquet.decode_byte_stream_split_fixed(UInt8[], 0, big(typemax(Int64)) + 1)
end

@testset "BYTE_STREAM_SPLIT gzip corpus fixture" begin
    if !isdir(bsscorpus())
        @info "parquet-testing corpus is not available; skipping byte_stream_split_extended.gzip.parquet"
    else
        path = bsscorpus("byte_stream_split_extended.gzip.parquet")
        pairs = ((1, 2, 2), (3, 4, Float32), (5, 6, Float64), (7, 8, Int32), (9, 10, Int64), (11, 12, 5), (13, 14, 4))
        for (plaincolumn, splitcolumn, kind) in pairs
            plaindata, count, plainmd, plainelement = bssfixturepage(path, plaincolumn)
            splitdata, splitcount, splitmd, splitelement = bssfixturepage(path, splitcolumn)
            @test count == splitcount == 200 && plainmd.statistics.null_count == 0 == splitmd.statistics.null_count
            @test splitmd.encodings == [MD.Encoding.RLE, MD.Encoding.BYTE_STREAM_SPLIT]
            if kind isa Integer
                @test plainelement.type_length == splitelement.type_length == kind
                plainvalues, _ = Parquet.decode_plain_fixed(plaindata, count, kind)
                splitvalues, next = Parquet.decode_byte_stream_split_fixed(splitdata, count, kind)
                @test splitvalues == plainvalues && next == length(splitdata) + 1
                @test Parquet.encode_byte_stream_split_fixed(splitvalues) == splitdata
            else
                plainvalues, _ = Parquet.decode_plain(kind, plaindata, count)
                splitvalues, next = Parquet.decode_byte_stream_split(kind, splitdata, count)
                @test bssbits(splitvalues) == bssbits(plainvalues) && next == length(splitdata) + 1
                @test Parquet.encode_byte_stream_split(splitvalues) == splitdata
            end
        end
    end
end

@testset "BYTE_STREAM_SPLIT zstd corpus fixture" begin
    if !isdir(bsscorpus())
        @info "parquet-testing corpus is not available; skipping byte_stream_split.zstd.parquet"
    else
        path = bsscorpus("byte_stream_split.zstd.parquet")
        for (column, T) in ((1, Float32), (2, Float64))
            data, count, md, _ = bssfixturepage(path, column)
            @test count == 300 && md.statistics.null_count == 0
            values, next = Parquet.decode_byte_stream_split(T, data, count)
            @test next == length(data) + 1 && all(isfinite, values)
            @test minimum(values) == reinterpret(T, md.statistics.min_value)[1]
            @test maximum(values) == reinterpret(T, md.statistics.max_value)[1]
            @test Parquet.encode_byte_stream_split(values) == data
        end
    end
end
