using Random

if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end

const DELTA_CORPUS = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))

function deltacorpus(parts...)
    return joinpath(DELTA_CORPUS, "data", parts...)
end

function deltauleb!(output::Vector{UInt8}, value::Integer)
    value = UInt64(value)
    while value >= 0x80
        push!(output, UInt8(value & 0x7f) | 0x80)
        value >>= 7
    end
    push!(output, UInt8(value))
    return
end

function deltazigzag!(output::Vector{UInt8}, value::Int64)
    deltauleb!(output, reinterpret(UInt64, (value << 1) ⊻ (value >> 63)))
    return
end

function deltazigzag!(output::Vector{UInt8}, value::Int32)
    deltauleb!(output, reinterpret(UInt32, (value << 1) ⊻ (value >> 31)))
    return
end

function deltapackbits!(output::Vector{UInt8}, values, width::Int, slots::Int)
    width == 0 && return
    bits = falses(slots * width)
    for (index, value) in enumerate(values), bit in 0:(width - 1)
        bits[(index - 1) * width + bit + 1] = (UInt64(value) >> bit) & 0x01 == 0x01
    end
    for byte in 1:(length(bits) ÷ 8)
        packed = UInt8(0)
        for bit in 0:7
            bits[(byte - 1) * 8 + bit + 1] && (packed |= UInt8(1) << bit)
        end
        push!(output, packed)
    end
    return
end

# Independent reference encoder with a configurable block layout (bit-by-bit packing).
function referencedelta(::Type{T}, values; blocksize::Int=128, miniblocks::Int=4) where {T}
    output = UInt8[]
    deltauleb!(output, blocksize)
    deltauleb!(output, miniblocks)
    deltauleb!(output, length(values))
    deltazigzag!(output, isempty(values) ? zero(T) : T(values[1]))
    slots = blocksize ÷ miniblocks
    deltas = T[T(values[i]) - T(values[i - 1]) for i in 2:length(values)]
    for block in Iterators.partition(deltas, blocksize)
        low = minimum(block)
        deltazigzag!(output, low)
        relative = [UInt64(reinterpret(unsigned(T), delta - low)) for delta in block]
        chunks = [relative[((m - 1) * slots + 1):min(m * slots, end)] for m in 1:miniblocks if (m - 1) * slots < length(relative)]
        widths = zeros(UInt8, miniblocks)
        for (m, chunk) in enumerate(chunks)
            widths[m] = UInt8(64 - leading_zeros(maximum(chunk)))
        end
        append!(output, widths)
        for (m, chunk) in enumerate(chunks)
            deltapackbits!(output, chunk, Int(widths[m]), slots)
        end
    end
    return output
end

function deltaheaderbytes(blocksize::Integer, miniblocks::Integer, count::Integer, first::Int64)
    output = UInt8[]
    deltauleb!(output, blocksize)
    deltauleb!(output, miniblocks)
    deltauleb!(output, count)
    deltazigzag!(output, first)
    return output
end

function fixturecsvline(line::String)
    fields = String[]
    index = firstindex(line)
    while true
        if index <= lastindex(line) && line[index] == '"'
            close = findnext('"', line, index + 1)
            push!(fields, line[(index + 1):(close - 1)])
            index = close + 1
            index > lastindex(line) && break
            line[index] == ',' || error("unexpected CSV character")
            index += 1
            index > lastindex(line) && (push!(fields, ""); break)
        else
            comma = findnext(',', line, index)
            comma === nothing && (push!(fields, line[index:end]); break)
            push!(fields, line[index:(comma - 1)])
            index = comma + 1
            index > lastindex(line) && (push!(fields, ""); break)
        end
    end
    return fields
end

function fixturecsv(path::String)
    lines = readlines(path)
    return fixturecsvline(lines[1]), [fixturecsvline(line) for line in lines[2:end]]
end

function fixturefooter(path::String)
    file = Parquet.File(path)
    meta = TH.decode(copy(file.footer.bytes), MD.FileMetaData)
    close(file)
    return meta
end

# The data section of the single V2 data page of a column chunk (levels skipped by length).
function fixturepage(path::String, column::Int)
    meta = fixturefooter(path)
    bytes = read(path)
    chunk = meta.row_groups[1].columns[column]
    md = chunk.meta_data
    r = TH.Reader(bytes, md.data_page_offset + 1, length(bytes))
    header = TH.decode(r, MD.PageHeader)
    v2 = header.data_page_header_v2
    start = md.data_page_offset + TH.consumed(r) + v2.definition_levels_byte_length + v2.repetition_levels_byte_length + 1
    stop = md.data_page_offset + TH.consumed(r) + header.compressed_page_size
    return view(bytes, start:stop), v2.num_values - v2.num_nulls, md.type_, meta.schema[column + 1].name, md, header
end

@testset "DELTA_BINARY_PACKED specification examples" begin
    @test Parquet.encode_delta_binary_packed(Int32[1, 2, 3, 4, 5]) == UInt8[0x80, 0x01, 0x04, 0x05, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00]
    example = UInt8[0x80, 0x01, 0x04, 0x08, 0x0e, 0x03, 0x02, 0x00, 0x00, 0x00, 0xc0, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
    values = Int32[7, 5, 3, 1, 2, 3, 4, 5]
    @test Parquet.encode_delta_binary_packed(values) == example
    @test referencedelta(Int32, values) == example
    @test Parquet.decode_delta_binary_packed(Int32, example, 8) == (values, 19)
    @test Parquet.decode_delta_binary_packed(Int64, example, 8)[1] == Int64.(values)
    @test Parquet.decode_delta_binary_packed(Int32, vcat(example, UInt8[0xff]), 8)[2] == 19
    @test Parquet.encode_delta_binary_packed(Int32[]) == UInt8[0x80, 0x01, 0x04, 0x00, 0x00]
    @test Parquet.decode_delta_binary_packed(Int32, UInt8[0x80, 0x01, 0x04, 0x00, 0x00], 0) == (Int32[], 6)
    @test Parquet.encode_delta_binary_packed(Int64[-1]) == UInt8[0x80, 0x01, 0x04, 0x01, 0x01]
    @test Parquet.decode_delta_binary_packed(Int64, UInt8[0x80, 0x01, 0x04, 0x01, 0x01], 1) == (Int64[-1], 6)
    output = zeros(Int32, 8)
    padded = vcat(UInt8[0xaa, 0xbb], example)
    @test Parquet.decode_delta_binary_packed!(output, padded; offset=3) == length(padded) + 1
    @test output == values
    @test Parquet.decode_delta_binary_packed(Int32, view(padded, 3:length(padded)), 8)[1] == values
    slice = Parquet.readrange(Parquet.source(padded), 2, length(padded) - 2)
    @test Parquet.decode_delta_binary_packed(Int32, slice, 8) == (values, length(example) + 1)
end

@testset "DELTA_BINARY_PACKED layouts and boundaries" begin
    rng = MersenneTwister(20260821)
    for T in (Int32, Int64), count in (0, 1, 2, 31, 32, 33, 127, 128, 129, 130, 255, 256, 257, 1000)
        values = rand(rng, T, count)
        encoded = Parquet.encode_delta_binary_packed(values)
        @test encoded == referencedelta(T, values)
        @test Parquet.decode_delta_binary_packed(T, encoded, count) == (values, length(encoded) + 1)
        for (blocksize, miniblocks) in ((128, 1), (128, 2), (256, 8), (512, 4), (1024, 32))
            stream = referencedelta(T, values; blocksize=blocksize, miniblocks=miniblocks)
            @test Parquet.decode_delta_binary_packed(T, stream, count) == (values, length(stream) + 1)
        end
    end
    for width in 0:63
        limit = width == 63 ? typemax(Int64) : (Int64(1) << width) - 1
        values = rand(rng, Int64(0):limit, 300)
        encoded = Parquet.encode_delta_binary_packed(values)
        @test encoded == referencedelta(Int64, values)
        @test Parquet.decode_delta_binary_packed(Int64, encoded, 300)[1] == values
    end
    for values in (fill(Int32(-7), 200), Int32.(-100:99), Int32[typemax(Int32), typemin(Int32), typemax(Int32)],
            Int32[typemin(Int32), typemax(Int32)], Int64[typemax(Int64), typemin(Int64), 0, -1, typemax(Int64)],
            Int64[typemin(Int64)], Int64.(-(1:129)), Int32[0, typemin(Int32)])
        encoded = Parquet.encode_delta_binary_packed(values)
        @test encoded == referencedelta(eltype(values), values)
        @test Parquet.decode_delta_binary_packed(eltype(values), encoded, length(values))[1] == values
    end
end

@testset "DELTA_BINARY_PACKED two's-complement wrapping" begin
    for values in (Int32[typemax(Int32), typemin(Int32)], Int32[typemin(Int32), typemax(Int32)],
            Int32[typemax(Int32), typemin(Int32), typemax(Int32), typemin(Int32)], Int32[0, typemax(Int32), typemin(Int32), 0],
            Int64[typemax(Int64), typemin(Int64)], Int64[typemin(Int64), typemax(Int64)],
            Int64[typemax(Int64), typemin(Int64), typemax(Int64), typemin(Int64)], Int64[-1, typemax(Int64), typemin(Int64), 1],
            Int32[typemin(Int32), 0, typemax(Int32), typemin(Int32) + 1], Int64[typemax(Int64) - 1, typemin(Int64) + 1])
        T = eltype(values)
        encoded = Parquet.encode_delta_binary_packed(values)
        @test encoded == referencedelta(T, values)
        decoded, next = Parquet.decode_delta_binary_packed(T, encoded, length(values))
        @test decoded == values && next == length(encoded) + 1
        @test reinterpret(unsigned(T), decoded) == reinterpret(unsigned(T), values)
    end
    # typemax -> typemin wraps to a delta of +1 and typemin -> typemax to -1 at the physical width
    for (values, mindelta) in ((Int32[typemax(Int32), typemin(Int32)], 0x02), (Int32[typemin(Int32), typemax(Int32)], 0x01),
            (Int64[typemax(Int64), typemin(Int64)], 0x02), (Int64[typemin(Int64), typemax(Int64)], 0x01))
        headerlength = length(deltaheaderbytes(128, 4, 2, Int64(values[1])))
        encoded = Parquet.encode_delta_binary_packed(values)
        @test encoded[headerlength + 1] == mindelta && encoded[(headerlength + 2):end] == UInt8[0x00, 0x00, 0x00, 0x00]
    end
    # INT32 streams must stay within the physical width: 33-bit miniblocks and out-of-range header values are rejected
    wide = referencedelta(Int64, Int64[0, typemax(Int32), typemin(Int32)])
    @test Parquet.decode_delta_binary_packed(Int64, wide, 3)[1] == Int64[0, typemax(Int32), typemin(Int32)]
    @test_throws Parquet.FormatError Parquet.decode_delta_binary_packed(Int32, wide, 3)
    @test_throws Parquet.FormatError Parquet.decode_delta_binary_packed(Int32, referencedelta(Int64, Int64[Int64(2)^31]), 1)
    @test_throws Parquet.FormatError Parquet.decode_delta_binary_packed(Int32, referencedelta(Int64, Int64[-Int64(2)^31 - 1]), 1)
    @test_throws Parquet.FormatError Parquet.decode_delta_binary_packed(Int32, referencedelta(Int64, Int64[0, -Int64(2)^31 - 5]), 2)
    @test_throws Parquet.FormatError Parquet.decode_delta_binary_packed(Int32, referencedelta(Int64, Int64[0, Int64(2)^31]), 2)
    @test Parquet.decode_delta_binary_packed(Int32, referencedelta(Int64, Int64[0, typemin(Int32)]), 2)[1] == Int32[0, typemin(Int32)]
    @test Parquet.decode_delta_binary_packed(Int32, referencedelta(Int32, Int32[0, typemin(Int32)]), 2)[1] == Int32[0, typemin(Int32)]
end

@testset "DELTA_BINARY_PACKED unused miniblock widths and padding" begin
    # values 1, 2, 3: one used miniblock of width 0; the three unused width bytes hold 0xff
    unused = vcat(deltaheaderbytes(128, 4, 3, Int64(1)), UInt8[0x02, 0x00, 0xff, 0xff, 0xff])
    @test Parquet.decode_delta_binary_packed(Int64, unused, 3) == (Int64[1, 2, 3], length(unused) + 1)
    @test Parquet.decode_delta_binary_packed(Int32, unused, 3) == (Int32[1, 2, 3], length(unused) + 1)
    # a used miniblock still validates its width against the physical type
    @test_throws Parquet.FormatError Parquet.decode_delta_binary_packed(Int64, vcat(deltaheaderbytes(128, 4, 3, Int64(1)), UInt8[0x02, 0xff, 0x00, 0x00, 0x00]), 3)
    @test_throws Parquet.FormatError Parquet.decode_delta_binary_packed(Int32, vcat(deltaheaderbytes(128, 4, 3, Int64(1)), UInt8[0x02, 0x21, 0x00, 0x00, 0x00], zeros(UInt8, 132)), 3)
    @test Parquet.decode_delta_binary_packed(Int64, vcat(deltaheaderbytes(128, 4, 3, Int64(1)), UInt8[0x02, 0x21, 0x00, 0x00, 0x00], zeros(UInt8, 132)), 3)[1] == Int64[1, 2, 3]
    # padding bits of a partially used miniblock are arbitrary
    padded = vcat(deltaheaderbytes(128, 4, 3, Int64(1)), UInt8[0x02, 0x01, 0xff, 0xff, 0xff, 0xfc, 0xff, 0xff, 0xff])
    @test Parquet.decode_delta_binary_packed(Int64, padded, 3) == (Int64[1, 2, 3], length(padded) + 1)
    # the second block of a multi-block stream also tolerates unused width bytes
    values = Int64.(1:140)
    stream = referencedelta(Int64, values)
    stream[end - 2:end] .= 0xff
    @test Parquet.decode_delta_binary_packed(Int64, stream, 140)[1] == values
end

@testset "DELTA_BINARY_PACKED malformed input and limits" begin
    F = Parquet.FormatError
    L = Parquet.LimitError
    good = Parquet.encode_delta_binary_packed(Int64.(1:300))
    for n in 0:(length(good) - 1)
        @test_throws F Parquet.decode_delta_binary_packed(Int64, good[1:n], 300)
    end
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(0, 4, 1, Int64(0)), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(100, 4, 1, Int64(0)), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(Int64(2)^31, 4, 1, Int64(0)), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(128, 0, 1, Int64(0)), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(128, 3, 1, Int64(0)), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(128, 8, 1, Int64(0)), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(128, 129, 1, Int64(0)), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(128, 4, 5, Int64(0)), 8)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, deltaheaderbytes(128, 4, Int64(2)^32, Int64(0)), 8)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, vcat(deltaheaderbytes(128, 4, 3, Int64(0)), UInt8[0x00, 0x41, 0x00, 0x00, 0x00]), 3)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, vcat(deltaheaderbytes(128, 4, 3, Int64(0)), UInt8[0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]), 3)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, vcat(deltaheaderbytes(128, 4, 3, Int64(0)), UInt8[0x00, 0x01, 0x00]), 3)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, fill(0x80, 11), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, vcat(fill(0x80, 9), UInt8[0x02]), 1)
    @test_throws F Parquet.decode_delta_binary_packed(Int64, UInt8[0x80], 1)
    @test_throws L Parquet.decode_delta_binary_packed(Int64, good, 300; limits=Parquet.Limits(max_container_elements=10))
    @test_throws L Parquet.decode_delta_binary_packed(Int64, good, 300; limits=Parquet.Limits(max_page_bytes=2000))
    # layout fields are charged to limits before any allocation
    @test_throws L Parquet.decode_delta_binary_packed!(Vector{Int64}(undef, 2), deltaheaderbytes(128, 4, 2, Int64(0)); limits=Parquet.Limits(max_container_elements=100))
    huge = deltaheaderbytes(Int64(2)^30, Int64(2)^25, 2, Int64(0))
    @test_throws L Parquet.decode_delta_binary_packed!(Vector{Int64}(undef, 2), huge; limits=Parquet.Limits(max_container_elements=Int64(2)^20))
    @test_throws L Parquet.decode_delta_binary_packed!(Vector{Int64}(undef, 2), huge)
    @test_throws F Parquet.decode_delta_binary_packed!(Vector{Int64}(undef, 2), huge; limits=Parquet.Limits(max_container_elements=Int64(2)^31))
    @test_throws L Parquet.decode_delta_binary_packed!(Vector{Int64}(undef, 2), vcat(deltaheaderbytes(Int64(2)^30, 1, 2, Int64(0)), UInt8[0x00, 0x40]))
    @test_throws F Parquet.decode_delta_binary_packed!(Vector{Int64}(undef, 2), vcat(deltaheaderbytes(Int64(2)^30, 1, 2, Int64(0)), UInt8[0x00, 0x40]); limits=Parquet.Limits(max_page_bytes=Int64(2)^40, max_container_elements=Int64(2)^31))
    @test_throws F Parquet.decode_delta_binary_packed(Int32, vcat(deltaheaderbytes(128, 4, 3, Int64(0)), UInt8[0x00, 0x21, 0x00, 0x00, 0x00]), 3)
    @test_throws ArgumentError Parquet.decode_delta_binary_packed(Int64, good, -1)
    @test_throws L Parquet.decode_delta_binary_packed(Int64, good, big(typemax(Int64)) + 1)
    @test Parquet.decode_delta_binary_packed!(Int64[], UInt8[0x80, 0x01, 0x04, 0x00, 0x00]) == 6
    rng = MersenneTwister(7)
    for trial in 1:400
        mutated = copy(good)
        for _ in 1:rand(rng, 1:3)
            mutated[rand(rng, eachindex(mutated))] = rand(rng, UInt8)
        end
        result = try
            Parquet.decode_delta_binary_packed(Int64, mutated, 300)
            :ok
        catch err
            err
        end
        @test result === :ok || result isa Union{F,L}
    end
end

@testset "DELTA_LENGTH_BYTE_ARRAY" begin
    F = Parquet.FormatError
    L = Parquet.LimitError
    words = ["Hello", "World", "Foobar", "ABCDEF"]
    expected = [Vector{UInt8}(codeunits(word)) for word in words]
    encoded = Parquet.encode_delta_length_byte_array(words)
    @test encoded == vcat(Parquet.encode_delta_binary_packed(Int32[5, 5, 6, 6]), Vector{UInt8}(codeunits("HelloWorldFoobarABCDEF")))
    @test Parquet.encode_delta_length_byte_array(expected) == encoded
    @test Parquet.decode_delta_length_byte_array(encoded, 4) == (expected, length(encoded) + 1)
    offsets, next = Parquet.decode_delta_length_byte_array_offsets(encoded, 4)
    @test length(offsets) == 5 && next == offsets[end] == length(encoded) + 1
    @test offsets[1] == length(Parquet.encode_delta_binary_packed(Int32[5, 5, 6, 6])) + 1
    @test String(encoded[offsets[3]:(offsets[4] - 1)]) == "Foobar"
    mixed = [UInt8[], UInt8[0x00], UInt8[], UInt8[0xff, 0x00]]
    @test Parquet.decode_delta_length_byte_array(Parquet.encode_delta_length_byte_array(mixed), 4)[1] == mixed
    @test Parquet.decode_delta_length_byte_array(Parquet.encode_delta_length_byte_array(String[]), 0) == (Vector{UInt8}[], 6)
    rng = MersenneTwister(3)
    for count in (1, 7, 129, 1000)
        values = [rand(rng, UInt8, rand(rng, 0:20)) for _ in 1:count]
        stream = Parquet.encode_delta_length_byte_array(values)
        padded = vcat(UInt8[0x01], stream)
        @test Parquet.decode_delta_length_byte_array(padded, count; offset=2) == (values, length(padded) + 1)
        slice = Parquet.readrange(Parquet.source(padded), 1, length(stream))
        @test Parquet.decode_delta_length_byte_array(slice, count)[1] == values
    end
    @test_throws F Parquet.decode_delta_length_byte_array(Parquet.encode_delta_binary_packed(Int32[-1]), 1)
    @test_throws F Parquet.decode_delta_length_byte_array(Parquet.encode_delta_binary_packed(Int32[10]), 1)
    @test_throws F Parquet.decode_delta_length_byte_array(encoded, 3)
    @test_throws L Parquet.decode_delta_length_byte_array(encoded, 4; limits=Parquet.Limits(max_string_bytes=3))
    @test_throws L Parquet.decode_delta_length_byte_array(encoded, 4; limits=Parquet.Limits(max_page_bytes=10))
    @test_throws L Parquet.decode_delta_length_byte_array(encoded, 4; limits=Parquet.Limits(max_container_elements=2))
    for n in 0:(length(encoded) - 1)
        @test_throws F Parquet.decode_delta_length_byte_array(encoded[1:n], 4)
    end
end

@testset "DELTA_BYTE_ARRAY" begin
    F = Parquet.FormatError
    L = Parquet.LimitError
    words = ["axis", "axle", "babble", "babyhood"]
    expected = [Vector{UInt8}(codeunits(word)) for word in words]
    encoded = Parquet.encode_delta_byte_array(words)
    @test encoded == vcat(Parquet.encode_delta_binary_packed(Int32[0, 2, 0, 3]), Parquet.encode_delta_length_byte_array(["axis", "le", "babble", "yhood"]))
    @test Parquet.decode_delta_byte_array(encoded, 4) == (expected, length(encoded) + 1)
    data, offsets, next = Parquet.decode_delta_byte_array_buffer(encoded, 4)
    @test String(data) == "axisaxlebabblebabyhood" && offsets == [1, 5, 9, 15, 23] && next == length(encoded) + 1
    repeated = ["same", "same", "", "same", "sam", "samba"]
    @test Parquet.decode_delta_binary_packed(Int32, Parquet.encode_delta_byte_array(repeated), 6)[1] == Int32[0, 4, 0, 0, 3, 3]
    @test Parquet.decode_delta_byte_array(Parquet.encode_delta_byte_array(repeated), 6)[1] == [Vector{UInt8}(codeunits(w)) for w in repeated]
    @test Parquet.decode_delta_byte_array(Parquet.encode_delta_byte_array(String[]), 0) == (Vector{UInt8}[], 11)
    @test Parquet.decode_delta_byte_array(Parquet.encode_delta_byte_array([UInt8[0xff]]), 1)[1] == [UInt8[0xff]]
    fixed = UInt8[1 1 2; 2 2 2; 3 4 4]
    stream = Parquet.encode_delta_byte_array_fixed(fixed)
    @test Parquet.decode_delta_byte_array_fixed(stream, 3, 3) == (fixed, length(stream) + 1)
    @test_throws ArgumentError Parquet.encode_delta_byte_array_fixed(Matrix{UInt8}(undef, 0, 4))
    @test_throws F Parquet.decode_delta_byte_array_fixed(stream, 3, 0)
    @test Parquet.decode_delta_byte_array_fixed(Parquet.encode_delta_byte_array_fixed(Matrix{UInt8}(undef, 2, 0)), 0, 2)[1] == Matrix{UInt8}(undef, 2, 0)
    @test_throws F Parquet.decode_delta_byte_array_fixed(Parquet.encode_delta_byte_array(["ab", "abc"]), 2, 2)
    @test_throws F Parquet.decode_delta_byte_array(vcat(Parquet.encode_delta_binary_packed(Int32[1]), Parquet.encode_delta_length_byte_array(["x"])), 1)
    @test_throws F Parquet.decode_delta_byte_array(vcat(Parquet.encode_delta_binary_packed(Int32[0, 5]), Parquet.encode_delta_length_byte_array(["ab", "c"])), 2)
    @test_throws F Parquet.decode_delta_byte_array(vcat(Parquet.encode_delta_binary_packed(Int32[-1]), Parquet.encode_delta_length_byte_array(["x"])), 1)
    @test_throws F Parquet.decode_delta_byte_array(encoded, 3)
    count = 200
    bomb = vcat(Parquet.encode_delta_binary_packed(Int32[i - 1 for i in 1:count]), Parquet.encode_delta_length_byte_array(fill("x", count)))
    @test_throws L Parquet.decode_delta_byte_array(bomb, count; limits=Parquet.Limits(max_page_bytes=1000))
    @test_throws L Parquet.decode_delta_byte_array(bomb, count; limits=Parquet.Limits(max_string_bytes=100))
    materialized = Parquet.Limits(max_materialized_bytes=5000)
    function rejectmaterializedbomb()
        budget = Parquet._LiveByteBudget(materialized)
        @test_throws L Parquet.decode_delta_byte_array(bomb, count;
            limits=materialized, budget=budget)
        @test Parquet._budgetused(budget) == 0
        return
    end
    rejectmaterializedbomb()
    GC.gc()
    @test @allocated(rejectmaterializedbomb()) < 10_000
    @test_throws L Parquet.decode_delta_byte_array_fixed(UInt8[], 0, big(typemax(Int64)) + 1)
    @test length(Parquet.decode_delta_byte_array(bomb, count)[1][end]) == count
    rng = MersenneTwister(11)
    for count in (1, 5, 129, 700)
        values = [rand(rng, UInt8[0x61, 0x62, 0x63], rand(rng, 0:12)) for _ in 1:count]
        stream = Parquet.encode_delta_byte_array(values)
        padded = vcat(UInt8[0x00, 0x00], stream)
        @test Parquet.decode_delta_byte_array(padded, count; offset=3) == (values, length(padded) + 1)
        slice = Parquet.readrange(Parquet.source(padded), 2, length(stream))
        @test Parquet.decode_delta_byte_array(slice, count)[1] == values
    end
    good = Parquet.encode_delta_byte_array(["alpha", "alphabet", "beta", "", "gamma"])
    for n in 0:(length(good) - 1)
        @test_throws F Parquet.decode_delta_byte_array(good[1:n], 5)
    end
    for trial in 1:400
        mutated = copy(good)
        for _ in 1:rand(rng, 1:3)
            mutated[rand(rng, eachindex(mutated))] = rand(rng, UInt8)
        end
        result = try
            Parquet.decode_delta_byte_array(mutated, 5)
            :ok
        catch err
            err
        end
        @test result === :ok || result isa Union{F,L}
    end
end

@testset "delta encodings corpus fixtures" begin
    if !isdir(deltacorpus())
        @warn "parquet-testing corpus not found; skipping delta fixture tests" DELTA_CORPUS
    else
        for (name, identical) in (("delta_binary_packed.parquet", false), ("delta_encoding_required_column.parquet", true),
                ("delta_encoding_optional_column.parquet", true), ("delta_byte_array.parquet", false))
            path = deltacorpus(name)
            header, rows = fixturecsv(deltacorpus(replace(name, ".parquet" => "_expect.csv")))
            for column in eachindex(header)
                payload, count, type, _, _, _ = fixturepage(path, column)
                expected = [row[column] for row in rows if row[column] != ""]
                @test length(expected) == count
                if type == MD.Type.INT32 || type == MD.Type.INT64
                    ET = type == MD.Type.INT32 ? Int32 : Int64
                    values, next = Parquet.decode_delta_binary_packed(ET, payload, count)
                    @test values == parse.(ET, expected)
                    @test next == length(payload) + 1
                    reencoded = Parquet.encode_delta_binary_packed(values)
                    identical && @test reencoded == payload
                    @test Parquet.decode_delta_binary_packed(ET, reencoded, count)[1] == values
                else
                    values, next = Parquet.decode_delta_byte_array(payload, count)
                    @test values == [Vector{UInt8}(codeunits(value)) for value in expected]
                    @test next == length(payload) + 1
                    reencoded = Parquet.encode_delta_byte_array(values)
                    identical && @test reencoded == payload
                    @test Parquet.decode_delta_byte_array(reencoded, count)[1] == values
                end
            end
        end
        widths = fixturefooter(deltacorpus("delta_binary_packed.parquet")).schema[2:end]
        @test [element.name for element in widths] == vcat(["bitwidth$i" for i in 0:64], ["int_value"])
        payload, count, _, name, _, _ = fixturepage(deltacorpus("delta_byte_array.parquet"), 7)
        @test name == "c_login" && count == 0
        @test Parquet.decode_delta_byte_array(payload, 0) == (Vector{UInt8}[], length(payload) + 1)
    end
end

@testset "DELTA_LENGTH_BYTE_ARRAY zstd corpus fixture" begin
    if !isdir(deltacorpus())
        @info "parquet-testing corpus is not available; skipping delta_length_byte_array.parquet"
    else
        path = deltacorpus("delta_length_byte_array.parquet")
        compressed, count, type, name, md, header = fixturepage(path, 1)
        v2 = header.data_page_header_v2
        payload = Parquet.decompress(md.codec, compressed,
            header.uncompressed_page_size - v2.definition_levels_byte_length -
                v2.repetition_levels_byte_length)
        @test length(payload) == header.uncompressed_page_size - v2.definition_levels_byte_length
        @test name == "FRUIT" && count == 1000
        values, next = Parquet.decode_delta_length_byte_array(payload, count)
        @test next == length(payload) + 1 && length(values) == 1000
        @test md.statistics === nothing
        @test values == [Vector{UInt8}(codeunits("apple_banana_mango$((index - 1)^2)")) for index in 1:1000]
        @test Parquet.decode_delta_length_byte_array(Parquet.encode_delta_length_byte_array(values), count)[1] == values
    end
end
