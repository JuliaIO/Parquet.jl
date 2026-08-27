if !@isdefined(TH)
    const TH = Parquet.Thrift
end

function thriftbytes(f)
    w = TH.Writer()
    f(w)
    return w.buffer
end

struct ThriftShiftedBytes <: AbstractVector{UInt8}
    bytes::Vector{UInt8}
    offset::Int
end

Base.size(bytes::ThriftShiftedBytes) = (length(bytes.bytes),)
Base.axes(bytes::ThriftShiftedBytes) =
    (bytes.offset:(bytes.offset + length(bytes.bytes) - 1),)
Base.IndexStyle(::Type{ThriftShiftedBytes}) = IndexLinear()

function Base.getindex(bytes::ThriftShiftedBytes, index::Int)
    checkbounds(bytes, index)
    return bytes.bytes[index - bytes.offset + 1]
end

struct ThriftShiftedValue
    bytes::ThriftShiftedBytes
end

function TH.encode!(writer::TH.Writer, value::ThriftShiftedValue)
    append!(writer.buffer, value.bytes)
    return
end

function thriftallocationmetadata(groups::Int)
    MD = Parquet.Metadata
    rowgroup = MD.RowGroup(columns=MD.ColumnChunk[],
        total_byte_size=Int64(0), num_rows=Int64(0))
    return MD.FileMetaData(
        version=Int32(1),
        schema=MD.SchemaElement[MD.SchemaElement(name="schema",
            num_children=Int32(0))],
        num_rows=Int64(0),
        row_groups=fill(rowgroup, groups),
        created_by="allocation-test",
    )
end

function thriftwriterallocations(value)
    exact = TH._encodedsize(value)
    bytes = Vector{UInt8}(undef, Int(exact))
    TH._encodedsize(value)
    TH._encodefixed!(bytes, value)
    count = @allocated TH._encodedsize(value)
    fixed = @allocated TH._encodefixed!(bytes, value)
    return count, fixed
end

@testset "compact protocol scalars" begin
    @test thriftbytes(w -> TH.writei32!(w, Int32(0))) == UInt8[0x00]
    @test thriftbytes(w -> TH.writei32!(w, Int32(-1))) == UInt8[0x01]
    @test thriftbytes(w -> TH.writei32!(w, Int32(1))) == UInt8[0x02]
    @test thriftbytes(w -> TH.writei32!(w, Int32(300))) == UInt8[0xd8, 0x04]
    @test thriftbytes(w -> TH.writei64!(w, Int64(-2))) == UInt8[0x03]
    @test thriftbytes(w -> TH.writei16!(w, Int16(32767))) == UInt8[0xfe, 0xff, 0x03]
    @test thriftbytes(w -> TH.writedouble!(w, 1.0)) == UInt8[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f]
    @test thriftbytes(w -> TH.writei8!(w, Int8(-1))) == UInt8[0xff]
    @test thriftbytes(w -> TH.writestring!(w, "ab")) == UInt8[0x02, 0x61, 0x62]
    @test thriftbytes(w -> TH.writebool!(w, true)) == UInt8[0x01]
    @test thriftbytes(w -> TH.writebool!(w, false)) == UInt8[0x02]
    for value in (Int32(0), Int32(-1), Int32(1), typemin(Int32), typemax(Int32), Int32(300), Int32(-300))
        @test TH.readi32(TH.Reader(thriftbytes(w -> TH.writei32!(w, value)))) == value
    end
    for value in (Int64(0), Int64(-1), typemin(Int64), typemax(Int64), Int64(2)^40, -Int64(2)^40)
        @test TH.readi64(TH.Reader(thriftbytes(w -> TH.writei64!(w, value)))) == value
    end
    for value in (typemin(Int16), Int16(-1), Int16(0), typemax(Int16))
        @test TH.readi16(TH.Reader(thriftbytes(w -> TH.writei16!(w, value)))) == value
    end
    for value in (typemin(Int8), Int8(0), typemax(Int8))
        @test TH.readi8(TH.Reader(thriftbytes(w -> TH.writei8!(w, value)))) == value
    end
    for value in (0.0, -0.0, 1.5, -Inf, NaN, floatmax(Float64))
        @test isequal(TH.readdouble(TH.Reader(thriftbytes(w -> TH.writedouble!(w, value)))), value)
    end
    @test TH.readbool(TH.Reader(UInt8[0x01])) === true
    @test TH.readbool(TH.Reader(UInt8[0x02])) === false
    @test TH.readstring(TH.Reader(thriftbytes(w -> TH.writestring!(w, "héllo")))) == "héllo"
    @test TH.readbinary(TH.Reader(thriftbytes(w -> TH.writebinary!(w, UInt8[0xff, 0x00])))) == UInt8[0xff, 0x00]
    @test TH.readstring(TH.Reader(UInt8[0x00])) == ""
    @test TH.readi32(TH.Reader(UInt8[0xff, 0xff, 0xff, 0xff, 0x0f])) == typemin(Int32)
    @test TH.readi64(TH.Reader(vcat(fill(0xff, 9), UInt8[0x01]))) == typemin(Int64)
    r = TH.Reader(UInt8[0x02, 0x61, 0x62, 0x07])
    @test TH.readstring(r) == "ab"
    @test TH.consumed(r) == 3
    @test TH.remaining(r) == 1
end

@testset "exact Compact Thrift writer buffers" begin
    dynamic = UInt8[0xaa]
    writer = TH.Writer(dynamic)
    TH.writei32!(writer, Int32(300))
    @test writer.buffer === dynamic
    @test dynamic == UInt8[0xaa, 0xd8, 0x04]

    value = ThriftShiftedValue(ThriftShiftedBytes(UInt8[0x10, 0x20, 0x30], 7))
    exact = TH._encodedsize(value)
    fixed = Vector{UInt8}(undef, exact)
    @test exact == 3
    @test TH._encodefixed!(fixed, value) === fixed
    @test fixed == UInt8[0x10, 0x20, 0x30]

    counter = TH._CountingBuffer(typemax(Int64))
    @test_throws TH._WriteCountOverflow push!(counter, UInt8(0))
    @test counter.count == typemax(Int64)
    short = Vector{UInt8}(undef, 2)
    @test_throws AssertionError TH._encodefixed!(short, value)
    long = Vector{UInt8}(undef, 4)
    @test_throws AssertionError TH._encodefixed!(long, value)
end

@testset "exact Compact Thrift writer allocations" begin
    small = thriftallocationmetadata(0)
    large = thriftallocationmetadata(256)
    thriftwriterallocations(small)
    thriftwriterallocations(large)
    smallcount, smallfixed = thriftwriterallocations(small)
    largecount, largefixed = thriftwriterallocations(large)
    threshold = 1024
    @test smallcount <= threshold
    @test largecount <= threshold
    @test largecount <= smallcount + 512
    @test smallfixed <= threshold
    @test largefixed <= threshold
    @test largefixed <= smallfixed + 512
end

@testset "field headers" begin
    w = TH.Writer()
    lastid = TH.writefieldheader!(w, Int16(0), Int16(1), TH.I32)
    lastid = TH.writefieldheader!(w, lastid, Int16(16), TH.BINARY)
    lastid = TH.writefieldheader!(w, lastid, Int16(32), TH.STRUCT)
    lastid = TH.writefieldheader!(w, lastid, Int16(3), TH.BOOL_TRUE)
    lastid = TH.writefieldheader!(w, lastid, Int16(32767), TH.BINARY)
    TH.writestop!(w)
    @test w.buffer == UInt8[0x15, 0xf8, 0x0c, 0x40, 0x01, 0x06, 0x08, 0xfe, 0xff, 0x03, 0x00]
    r = TH.Reader(w.buffer)
    @test TH.readfieldheader(r, Int16(0)) == (Int16(1), TH.I32)
    @test TH.readfieldheader(r, Int16(1)) == (Int16(16), TH.BINARY)
    @test TH.readfieldheader(r, Int16(16)) == (Int16(32), TH.STRUCT)
    @test TH.readfieldheader(r, Int16(32)) == (Int16(3), TH.BOOL_TRUE)
    @test TH.readfieldheader(r, Int16(3)) == (Int16(32767), TH.BINARY)
    @test TH.readfieldheader(r, Int16(32767)) == (Int16(0), TH.STOP)
    @test TH.remaining(r) == 0
    @test r.headerpos == 11 && r.previd == 32767
    @test_throws Parquet.FormatError TH.readfieldheader(TH.Reader(UInt8[0x1d]), Int16(0))
    @test_throws Parquet.FormatError TH.readfieldheader(TH.Reader(UInt8[0x1e]), Int16(0))
    @test_throws Parquet.FormatError TH.readfieldheader(TH.Reader(UInt8[0x1f]), Int16(0))
    @test_throws Parquet.FormatError TH.readfieldheader(TH.Reader(UInt8[0x10]), Int16(0))
    @test_throws Parquet.FormatError TH.readfieldheader(TH.Reader(UInt8[0xf5]), Int16(32760))
    @test_throws Parquet.FormatError TH.readfieldheader(TH.Reader(UInt8[0x05]), Int16(0))
    @test_throws Parquet.FormatError TH.readfieldheader(TH.Reader(UInt8[]), Int16(0))
end

@testset "containers" begin
    @test thriftbytes(w -> TH.writelist!(w, Int32[1, 2])) == UInt8[0x25, 0x02, 0x04]
    @test thriftbytes(w -> TH.writelist!(w, Bool[true, false])) == UInt8[0x21, 0x01, 0x02]
    long = Int32.(1:20)
    bytes = thriftbytes(w -> TH.writelist!(w, long))
    @test bytes[1:2] == UInt8[0xf5, 0x14]
    @test TH.readlist(TH.Reader(bytes), Int32) == long
    @test TH.readlist(TH.Reader(thriftbytes(w -> TH.writelist!(w, Bool[true, false]))), Bool) == [true, false]
    @test TH.readlist(TH.Reader(thriftbytes(w -> TH.writelist!(w, ["a", "bc"]))), String) == ["a", "bc"]
    @test TH.readlist(TH.Reader(thriftbytes(w -> TH.writelist!(w, Int8[-1, 1]))), Int8) == Int8[-1, 1]
    @test TH.readlist(TH.Reader(thriftbytes(w -> TH.writelist!(w, Int16[-1, 1]))), Int16) == Int16[-1, 1]
    @test TH.readlist(TH.Reader(thriftbytes(w -> TH.writelist!(w, [UInt8[1], UInt8[]]))), Vector{UInt8}) == [UInt8[1], UInt8[]]
    nested = [Int64[1], Int64[], Int64[-5, 7]]
    @test TH.readlist(TH.Reader(thriftbytes(w -> TH.writelist!(w, nested))), Vector{Int64}) == nested
    doubles = [1.5, NaN]
    @test isequal(TH.readlist(TH.Reader(thriftbytes(w -> TH.writelist!(w, doubles))), Float64), doubles)
    r = TH.Reader(UInt8[0x25, 0x02, 0x04])
    @test TH.readlist(r, String) === nothing
    @test TH.consumed(r) == 0
    @test TH.readlist(r, Int32) == Int32[1, 2]
    @test r.depth == 0
    pairs = ["a" => Int64(1), "b" => Int64(-1)]
    bytes = thriftbytes(w -> TH.writemap!(w, pairs))
    @test bytes == UInt8[0x02, 0x86, 0x01, 0x61, 0x02, 0x01, 0x62, 0x01]
    @test TH.readmap(TH.Reader(bytes), String, Int64) == pairs
    @test thriftbytes(w -> TH.writemap!(w, Pair{String,Int64}[])) == UInt8[0x00]
    @test TH.readmap(TH.Reader(UInt8[0x00]), String, Int64) == Pair{String,Int64}[]
    r = TH.Reader(bytes)
    @test TH.readmap(r, Int32, Int64) === nothing
    @test TH.consumed(r) == 0
    listofmaps = [[Int32(1) => "x"], Pair{Int32,String}[]]
    @test TH.readlist(TH.Reader(thriftbytes(w -> TH.writelist!(w, listofmaps))), Vector{Pair{Int32,String}}) == listofmaps
    mapofbools = [true => [false]]
    @test TH.readmap(TH.Reader(thriftbytes(w -> TH.writemap!(w, mapofbools))), Bool, Vector{Bool}) == mapofbools
end

@testset "skip and raw field capture" begin
    bytes = thriftbytes() do w
        lastid = TH.writefieldheader!(w, Int16(0), Int16(1), TH.BOOL_TRUE)
        lastid = TH.writefieldheader!(w, lastid, Int16(2), TH.BYTE)
        TH.writei8!(w, Int8(7))
        lastid = TH.writefieldheader!(w, lastid, Int16(3), TH.I16)
        TH.writei16!(w, Int16(-300))
        lastid = TH.writefieldheader!(w, lastid, Int16(4), TH.I32)
        TH.writei32!(w, Int32(123456))
        lastid = TH.writefieldheader!(w, lastid, Int16(5), TH.I64)
        TH.writei64!(w, Int64(-1))
        lastid = TH.writefieldheader!(w, lastid, Int16(6), TH.DOUBLE)
        TH.writedouble!(w, 2.5)
        lastid = TH.writefieldheader!(w, lastid, Int16(7), TH.BINARY)
        TH.writebinary!(w, UInt8[1, 2, 3])
        lastid = TH.writefieldheader!(w, lastid, Int16(8), TH.LIST)
        TH.writelist!(w, Int32[1, 2, 3])
        lastid = TH.writefieldheader!(w, lastid, Int16(9), TH.SET)
        TH.writelist!(w, ["x"])
        lastid = TH.writefieldheader!(w, lastid, Int16(10), TH.MAP)
        TH.writemap!(w, [Int32(1) => UInt8[9]])
        lastid = TH.writefieldheader!(w, lastid, Int16(11), TH.STRUCT)
        TH.writefieldheader!(w, Int16(0), Int16(1), TH.BINARY)
        TH.writestring!(w, "inner")
        TH.writestop!(w)
        lastid = TH.writefieldheader!(w, lastid, Int16(12), TH.BOOL_FALSE)
        TH.writestop!(w)
    end
    r = TH.Reader(bytes)
    TH.skipstruct!(r)
    @test TH.remaining(r) == 0
    @test r.depth == 0
    r = TH.Reader(bytes)
    TH.enter!(r)
    lastid = Int16(0)
    fields = TH.RawField[]
    while true
        id, ty = TH.readfieldheader(r, lastid)
        ty == TH.STOP && break
        lastid = id
        push!(fields, TH.readrawfield(r, id, ty))
    end
    @test [f.id for f in fields] == Int16.(1:12)
    @test fields[1].bytes == UInt8[0x11] && isempty(TH.payload(fields[1]))
    @test fields[7].bytes == UInt8[0x18, 0x03, 0x01, 0x02, 0x03]
    @test TH.payload(fields[7]) == UInt8[0x03, 0x01, 0x02, 0x03]
    @test fields[12].type == TH.BOOL_FALSE && isempty(TH.payload(fields[12]))
    @test all(f -> f.headerlength == 1, fields)
    @test [f.previd for f in fields] == Int16.(0:11)
    @test fields[3] == fields[3] && hash(fields[3]) == hash(fields[3]) && fields[3] != fields[4]
    w = TH.Writer()
    lastid = Int16(0)
    for field in fields
        lastid = TH.writeraw!(w, lastid, field)
    end
    TH.writestop!(w)
    @test w.buffer == bytes
    # a raw field re-emitted after a different predecessor gets a synthesized header
    w = TH.Writer()
    @test TH.writeraw!(w, Int16(0), fields[7]) == 7
    @test w.buffer == UInt8[0x78, 0x03, 0x01, 0x02, 0x03]
    # regression: skipping a binary field must consume its length prefix (found with ARROW-GH-41317)
    kv = thriftbytes() do w
        TH.writefieldheader!(w, Int16(0), Int16(1), TH.BINARY)
        TH.writestring!(w, "boolean")
        TH.writestop!(w)
    end
    r = TH.Reader(kv)
    TH.skipstruct!(r)
    @test TH.remaining(r) == 0
end

@testset "malformed input" begin
    F = Parquet.FormatError
    @test_throws F TH.readbyte(TH.Reader(UInt8[]))
    @test_throws F TH.readi32(TH.Reader(UInt8[0x80]))
    @test_throws F TH.readi64(TH.Reader(UInt8[0x80, 0x80, 0x80]))
    @test_throws F TH.readi32(TH.Reader(UInt8[0x80, 0x80, 0x80, 0x80, 0x80, 0x00]))
    @test_throws F TH.readi32(TH.Reader(UInt8[0x80, 0x80, 0x80, 0x80, 0x10]))
    @test_throws F TH.readi64(TH.Reader(vcat(fill(0x80, 9), UInt8[0x02])))
    @test_throws F TH.readi64(TH.Reader(vcat(fill(0x80, 10), UInt8[0x00])))
    @test_throws F TH.readi16(TH.Reader(thriftbytes(w -> TH.writei32!(w, Int32(40000)))))
    @test_throws F TH.readdouble(TH.Reader(fill(0x00, 7)))
    @test_throws F TH.readbool(TH.Reader(UInt8[0x00]))
    @test_throws F TH.readbool(TH.Reader(UInt8[0x03]))
    @test_throws F TH.readlist(TH.Reader(UInt8[0x21, 0x01, 0x00]), Bool)
    @test_throws F TH.skiplist!(TH.Reader(UInt8[0x21, 0x01, 0x00]))
    @test_throws F TH.readstring(TH.Reader(UInt8[0x05, 0x61]))
    @test_throws F TH.readstring(TH.Reader(UInt8[0xff, 0xff, 0xff, 0xff, 0x0f]))
    @test_throws F TH.readlist(TH.Reader(UInt8[0xf5, 0xff, 0xff, 0xff, 0xff, 0x0f]), Int32)
    @test_throws F TH.readlist(TH.Reader(UInt8[0x2d, 0x00, 0x00]), Int32)
    @test_throws F TH.readlistheader(TH.Reader(UInt8[0x20]))
    @test_throws F TH.readmap(TH.Reader(UInt8[0x01, 0xd5, 0x00, 0x00]), Int32, Int32)
    @test_throws F TH.readmap(TH.Reader(UInt8[0x01, 0x5d, 0x00, 0x00]), Int32, Int32)
    @test_throws F TH.readmap(TH.Reader(UInt8[0x01]), Int32, Int32)
    @test_throws F TH.skipvalue!(TH.Reader(UInt8[0x00]), UInt8(13))
    @test_throws F TH.skipvalue!(TH.Reader(UInt8[0x00]), UInt8(0))
    @test_throws F TH.skipstruct!(TH.Reader(UInt8[0x15]))
    @test_throws F TH.skipstruct!(TH.Reader(UInt8[0x18, 0x05, 0x61]))
    @test_throws F TH.skipstruct!(TH.Reader(UInt8[0x17, 0x00]))
    @test_throws F TH.skipstruct!(TH.Reader(UInt8[0x1c, 0x1d, 0x00, 0x00]))
end

@testset "resource limits" begin
    L = Parquet.LimitError
    F = Parquet.FormatError
    small = Parquet.Limits(max_string_bytes=4)
    err = try
        TH.readstring(TH.Reader(UInt8[0x05, 0x61, 0x62, 0x63, 0x64, 0x65]; limits=small))
        nothing
    catch e
        e
    end
    @test err isa L && err.resource == :string_bytes && err.requested == 5 && err.maximum == 4
    @test TH.readstring(TH.Reader(UInt8[0x04, 0x61, 0x62, 0x63, 0x64]; limits=small)) == "abcd"
    @test_throws L TH.skipvalue!(TH.Reader(UInt8[0x05, 0x61, 0x62, 0x63, 0x64, 0x65]; limits=small), TH.BINARY)
    @test_throws F TH.readstring(TH.Reader(UInt8[0x80, 0x80, 0x40]))
    @test_throws L TH.readstring(TH.Reader(UInt8[0xff, 0xff, 0xff, 0xff, 0x07]))
    few = Parquet.Limits(max_container_elements=3)
    @test_throws L TH.readlist(TH.Reader(UInt8[0x45, 0x00, 0x00, 0x00, 0x00]; limits=few), Int32)
    @test_throws L TH.skiplist!(TH.Reader(UInt8[0x45, 0x00, 0x00, 0x00, 0x00]; limits=few))
    @test TH.readlist(TH.Reader(UInt8[0x35, 0x00, 0x00, 0x00]; limits=few), Int32) == Int32[0, 0, 0]
    @test_throws F TH.readlist(TH.Reader(UInt8[0xf5, 0x80, 0x80, 0x80, 0x01]), Int32)
    @test_throws L TH.readlist(TH.Reader(UInt8[0xf5, 0xff, 0xff, 0xff, 0xff, 0x07]), Int32)
    @test_throws F TH.readlist(TH.Reader(vcat(UInt8[0xf7, 0x80, 0x08], zeros(UInt8, 64))), Float64)
    @test_throws L TH.readmap(TH.Reader(UInt8[0x04, 0x55, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]; limits=few), Int32, Int32)
    @test_throws F TH.readmap(TH.Reader(UInt8[0x7f, 0x55, 0x00, 0x00]), Int32, Int32)
    shallow = Parquet.Limits(max_metadata_depth=2)
    @test TH.readlist(TH.Reader(UInt8[0x19, 0x15, 0x00]; limits=shallow), Vector{Int32}) == [Int32[0]]
    @test_throws L TH.readlist(TH.Reader(UInt8[0x19, 0x19, 0x15, 0x00]; limits=shallow), Vector{Vector{Int32}})
    err = try
        TH.skiplist!(TH.Reader(vcat(fill(0x19, 199), UInt8[0x05])))
        nothing
    catch e
        e
    end
    @test err isa L && err.resource == :metadata_depth && err.requested == 129 && err.maximum == 128
    @test_throws L TH.skipstruct!(TH.Reader(fill(0x1c, 200)))
    @test_throws L TH.skipstruct!(TH.Reader(vcat(UInt8[0x1b, 0x01, 0xcc], fill(0x1c, 200))))
    r = TH.Reader(vcat(fill(0x19, 100), UInt8[0x05]))
    TH.skiplist!(r)
    @test r.depth == 0 && TH.remaining(r) == 0

    binarybudget = Parquet._LiveByteBudget(
        Parquet.Limits(max_materialized_bytes=100))
    binaryreader = TH.Reader(vcat(UInt8[0x64], fill(UInt8(0x61), 100));
        budget=binarybudget)
    @test_throws L TH.readbinary(binaryreader)
    @test Parquet._budgetused(binarybudget) == 0

    listbudget = Parquet._LiveByteBudget(
        Parquet.Limits(max_materialized_bytes=70))
    listreader = TH.Reader(UInt8[0x25, 0x00, 0x00]; budget=listbudget)
    @test_throws L TH.readlist(listreader, Int32)
    @test Parquet._budgetused(listbudget) == 0

    decodebudget = Parquet._LiveByteBudget(Parquet.Limits())
    @test_throws F TH.decode(UInt8[0x00], Parquet.Metadata.FileMetaData;
        budget=decodebudget)
    @test Parquet._budgetused(decodebudget) == 0
end

@testset "reader ranges and byte sources" begin
    bytes = UInt8[0x15, 0x02, 0x00]
    r = TH.Reader(bytes, 2, 3)
    @test TH.readi32(r) == 1 && TH.remaining(r) == 1 && TH.consumed(r) == 1
    @test_throws BoundsError TH.Reader(bytes, 0, 3)
    @test_throws BoundsError TH.Reader(bytes, 1, 4)
    @test_throws ArgumentError TH.Reader(bytes, 3, 1)
    @test TH.remaining(TH.Reader(bytes, 2, 1)) == 0
    slice = Parquet.readrange(Parquet.source(bytes), 0, 3)
    @test TH.readfieldheader(TH.Reader(slice), Int16(0)) == (Int16(1), TH.I32)
    @test TH.readlist(TH.Reader(view(UInt8[0x00, 0x25, 0x02, 0x04], 2:4)), Int32) == Int32[1, 2]
    @test TH.readstring(TH.Reader(view(UInt8[0x00, 0x02, 0x61, 0x62], 2:4))) == "ab"
end
