using UUIDs

if !isdefined(Parquet, :JSONValue)
    Base.include(Parquet, joinpath(@__DIR__, "..", "src", "logical_binary.jl"))
end

if !@isdefined(MD)
    const MD = Parquet.Metadata
end
if !@isdefined(TH)
    const TH = Parquet.Thrift
end

function binarylogicalelement(name, physical; logical=nothing, converted=nothing,
    width=nothing, repetition=MD.FieldRepetitionType.OPTIONAL)
    return MD.SchemaElement(name=name, type_=physical,
        repetition_type=repetition, logicalType=logical, converted_type=converted,
        type_length=width === nothing ? nothing : Int32(width))
end

function littleuint32(value::UInt32)
    return UInt8[UInt8(value & 0xff), UInt8((value >> 8) & 0xff),
        UInt8((value >> 16) & 0xff), UInt8(value >> 24)]
end

@testset "binary logical annotation validation" begin
    enum = binarylogicalelement("enum", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(ENUM=MD.EnumType()))
    json = binarylogicalelement("json", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(JSON=MD.JsonType()))
    bson = binarylogicalelement("bson", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(BSON=MD.BsonType()))
    uuid = binarylogicalelement("uuid", MD.Type.FIXED_LEN_BYTE_ARRAY; width=16,
        logical=MD.LogicalType(UUID=MD.UUIDType()))
    float16 = binarylogicalelement("half", MD.Type.FIXED_LEN_BYTE_ARRAY; width=2,
        logical=MD.LogicalType(FLOAT16=MD.Float16Type()))
    unknown = binarylogicalelement("null", MD.Type.INT64;
        logical=MD.LogicalType(UNKNOWN=MD.NullType()))
    interval = binarylogicalelement("interval", MD.Type.FIXED_LEN_BYTE_ARRAY;
        width=12, converted=MD.ConvertedType.INTERVAL)
    @test Parquet._binarylogicalkind(enum) === :enum
    @test Parquet._binarylogicalkind(json) === :json
    @test Parquet._binarylogicalkind(bson) === :bson
    @test Parquet._binarylogicalkind(uuid) === :uuid
    @test Parquet._binarylogicalkind(float16) === :float16
    @test Parquet._binarylogicalkind(unknown) === :unknown
    @test Parquet._binarylogicalkind(interval) === :interval
    @test Parquet._binarylogicaleltype(enum, Vector{UInt8}) === String
    @test Parquet._binarylogicaleltype(json, Vector{UInt8}) === Parquet.JSONValue
    @test Parquet._binarylogicaleltype(bson, Vector{UInt8}) === Parquet.BSONValue
    @test Parquet._binarylogicaleltype(uuid, Vector{UInt8}) === UUID
    @test Parquet._binarylogicaleltype(float16, Vector{UInt8}) === Float16
    @test Parquet._binarylogicaleltype(unknown, Int64) === Missing
    @test Parquet._binarylogicaleltype(interval, Vector{UInt8}) === Parquet.Interval

    node = Parquet.SchemaNode(uuid, ["uuid"], Int16(1), Int16(0), Int32(1),
        Parquet.SchemaNode[])
    @test Parquet._binarylogicalkind(node) === :uuid
    @test Parquet._binarylogicaleltype(node, Vector{UInt8}) === UUID

    legacy = (
        (MD.ConvertedType.ENUM, MD.Type.BYTE_ARRAY, nothing, :enum),
        (MD.ConvertedType.JSON, MD.Type.BYTE_ARRAY, nothing, :json),
        (MD.ConvertedType.BSON, MD.Type.BYTE_ARRAY, nothing, :bson),
        (MD.ConvertedType.INTERVAL, MD.Type.FIXED_LEN_BYTE_ARRAY, 12, :interval),
    )
    for (converted, physical, width, expected) in legacy
        element = binarylogicalelement("legacy", physical; converted=converted,
            width=width)
        @test Parquet._binarylogicalkind(element) === expected
    end

    modernwins = binarylogicalelement("uuid", MD.Type.FIXED_LEN_BYTE_ARRAY;
        width=16, logical=MD.LogicalType(UUID=MD.UUIDType()),
        converted=MD.ConvertedType.ENUM)
    @test Parquet._binarylogicalkind(modernwins) === :uuid
    stringwins = binarylogicalelement("text", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(STRING=MD.StringType()),
        converted=MD.ConvertedType.INTERVAL)
    @test Parquet._binarylogicalkind(stringwins) === nothing
    opaque = MD.LogicalType(unknown_fields=(TH.RawField(100, TH.STRUCT, UInt8[0x00]),))
    opaquemodern = binarylogicalelement("opaque", MD.Type.BYTE_ARRAY;
        logical=opaque, converted=MD.ConvertedType.JSON)
    @test Parquet._binarylogicalkind(opaquemodern) === nothing
    @test Parquet._binarylogicalkind(binarylogicalelement("plain", MD.Type.INT32)) === nothing

    badlogical = (
        binarylogicalelement("enum", MD.Type.INT32;
            logical=MD.LogicalType(ENUM=MD.EnumType())),
        binarylogicalelement("json", MD.Type.INT32;
            logical=MD.LogicalType(JSON=MD.JsonType())),
        binarylogicalelement("bson", MD.Type.INT64;
            logical=MD.LogicalType(BSON=MD.BsonType())),
        binarylogicalelement("uuid", MD.Type.BYTE_ARRAY;
            logical=MD.LogicalType(UUID=MD.UUIDType())),
        binarylogicalelement("uuid", MD.Type.FIXED_LEN_BYTE_ARRAY; width=15,
            logical=MD.LogicalType(UUID=MD.UUIDType())),
        binarylogicalelement("uuid", MD.Type.FIXED_LEN_BYTE_ARRAY;
            logical=MD.LogicalType(UUID=MD.UUIDType())),
        binarylogicalelement("half", MD.Type.FLOAT;
            logical=MD.LogicalType(FLOAT16=MD.Float16Type())),
        binarylogicalelement("half", MD.Type.FIXED_LEN_BYTE_ARRAY; width=4,
            logical=MD.LogicalType(FLOAT16=MD.Float16Type())),
        binarylogicalelement("interval", MD.Type.BYTE_ARRAY;
            converted=MD.ConvertedType.INTERVAL),
        binarylogicalelement("interval", MD.Type.FIXED_LEN_BYTE_ARRAY; width=8,
            converted=MD.ConvertedType.INTERVAL),
        binarylogicalelement("unknown", MD.Type.INT32;
            logical=MD.LogicalType(UNKNOWN=MD.NullType()),
            repetition=MD.FieldRepetitionType.REQUIRED),
        MD.SchemaElement(name="group", num_children=Int32(0),
            repetition_type=MD.FieldRepetitionType.OPTIONAL,
            logicalType=MD.LogicalType(UNKNOWN=MD.NullType())),
    )
    for element in badlogical
        @test_throws Parquet.FormatError Parquet._binarylogicalkind(element)
    end
end

@testset "UUID byte order and inverse" begin
    element = binarylogicalelement("uuid", MD.Type.FIXED_LEN_BYTE_ARRAY; width=16,
        logical=MD.LogicalType(UUID=MD.UUIDType()))
    bytes = hex2bytes("00112233445566778899aabbccddeeff")
    expected = UUID("00112233-4455-6677-8899-aabbccddeeff")
    @test Parquet._binarylogicalvalue(element, bytes) == expected
    @test Parquet._binaryphysicalvalue(element, expected) == bytes
    values = Union{Missing,Vector{UInt8}}[bytes, missing,
        hex2bytes("ffffffffffffffffffffffffffffffff")]
    decoded = Parquet._binarylogicalvalues(element, values)
    @test decoded isa Vector{Union{Missing,UUID}}
    @test isequal(decoded, Union{Missing,UUID}[expected, missing,
        UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")])
    encoded = Parquet._binaryphysicalvalues(element, decoded)
    @test encoded isa Vector{Union{Missing,Vector{UInt8}}}
    @test isequal(encoded, values)
    @test Parquet._binarylogicalvalue(element, missing) === missing
    @test Parquet._binaryphysicalvalue(element, missing) === missing
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(element, bytes[1:15])
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(element, bytes[1:1])
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(element, "uuid")
    @test_throws ArgumentError Parquet._binaryphysicalvalue(element, string(expected))
end

@testset "FLOAT16 little-endian inverse" begin
    element = binarylogicalelement("half", MD.Type.FIXED_LEN_BYTE_ARRAY; width=2,
        logical=MD.LogicalType(FLOAT16=MD.Float16Type()))
    patterns = UInt16[0x0000, 0x8000, 0x3c00, 0x7c00, 0xfc00, 0x7e01, 0x0001]
    physical = [UInt8[UInt8(bits & 0xff), UInt8(bits >> 8)] for bits in patterns]
    decoded = Parquet._binarylogicalvalues(element, physical)
    @test reinterpret(UInt16, decoded) == patterns
    @test Parquet._binaryphysicalvalues(element, decoded) == physical
    optional = Union{Missing,Vector{UInt8}}[physical[2], missing, physical[6]]
    optionallogical = Parquet._binarylogicalvalues(element, optional)
    @test optionallogical isa Vector{Union{Missing,Float16}}
    @test reinterpret(UInt16, collect(skipmissing(optionallogical))) == patterns[[2, 6]]
    @test isequal(Parquet._binaryphysicalvalues(element, optionallogical), optional)
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(element, UInt8[0x00])
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(
        element, UInt8[0x00, 0x00, 0x00])
    @test_throws ArgumentError Parquet._binaryphysicalvalue(element, Float32(1))
end

@testset "ENUM UTF-8 conversion" begin
    modern = binarylogicalelement("enum", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(ENUM=MD.EnumType()))
    legacy = binarylogicalelement("enum", MD.Type.BYTE_ARRAY;
        converted=MD.ConvertedType.ENUM)
    bytes = [collect(codeunits("alpha")), UInt8[0xce, 0xb2]]
    for element in (modern, legacy)
        @test Parquet._binarylogicalvalues(element, bytes) == ["alpha", "β"]
        @test Parquet._binaryphysicalvalues(element, ["alpha", "β"]) == bytes
    end
    source = copy(bytes[1])
    decoded = Parquet._binarylogicalvalue(modern, source)
    source[1] = 0x7a
    @test decoded == "alpha"
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(modern, UInt8[0xff])
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(modern, Int32(1))
    @test_throws ArgumentError Parquet._binaryphysicalvalue(modern, UInt8[0x61])
end

@testset "tagged JSON and BSON bytes" begin
    json = binarylogicalelement("json", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(JSON=MD.JsonType()))
    bson = binarylogicalelement("bson", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(BSON=MD.BsonType()))
    jsonbytes = collect(codeunits("{\"answer\":42}"))
    source = copy(jsonbytes)
    taggedjson = Parquet._binarylogicalvalue(json, source)
    @test taggedjson isa Parquet.JSONValue
    @test taggedjson.bytes == jsonbytes
    source[1] = 0x00
    @test taggedjson.bytes == jsonbytes
    encodedjson = Parquet._binaryphysicalvalue(json, taggedjson)
    encodedjson[1] = 0x00
    @test taggedjson.bytes == jsonbytes
    copiedjson = copy(taggedjson)
    @test copiedjson == taggedjson && isequal(copiedjson, taggedjson)
    @test hash(copiedjson) == hash(taggedjson)
    originaljsonhash = hash(copiedjson)
    @test_throws CanonicalIndexError setindex!(copiedjson.bytes, 0x00, 1)
    @test hash(copiedjson) == originaljsonhash
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(json,
        collect(codeunits("syntactically not JSON")))
    @test_throws ArgumentError Parquet.JSONValue(
        collect(codeunits("syntactically not JSON")))
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(json, UInt8[0xff])
    @test_throws ArgumentError Parquet.JSONValue(UInt8[0xff])
    @test_throws ArgumentError Parquet._binaryphysicalvalue(json, jsonbytes)

    bsonbytes = UInt8[0x07, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00]
    taggedbson = Parquet._binarylogicalvalue(bson, bsonbytes)
    @test taggedbson isa Parquet.BSONValue
    @test taggedbson.bytes == bsonbytes
    bsonbytes[1] = 0x00
    @test taggedbson.bytes[1] == 0x07
    encodedbson = Parquet._binaryphysicalvalue(bson, taggedbson)
    encodedbson[1] = 0x00
    @test taggedbson.bytes[1] == 0x07
    copiedbson = copy(taggedbson)
    @test copiedbson == taggedbson && hash(copiedbson) == hash(taggedbson)
    originalbsonhash = hash(copiedbson)
    @test_throws CanonicalIndexError setindex!(copiedbson.bytes, 0x00, 1)
    @test hash(copiedbson) == originalbsonhash
    @test_throws ArgumentError Parquet._binaryphysicalvalue(bson, taggedbson.bytes)

    optionaljson = Union{Missing,Vector{UInt8}}[jsonbytes, missing]
    logicaljson = Parquet._binarylogicalvalues(json, optionaljson)
    @test logicaljson isa Vector{Union{Missing,Parquet.JSONValue}}
    @test isequal(Parquet._binaryphysicalvalues(json, logicaljson), optionaljson)
    optionalbson = Union{Missing,Vector{UInt8}}[collect(taggedbson.bytes), missing]
    logicalbson = Parquet._binarylogicalvalues(bson, optionalbson)
    @test logicalbson isa Vector{Union{Missing,Parquet.BSONValue}}
    @test isequal(Parquet._binaryphysicalvalues(bson, logicalbson), optionalbson)
end

@testset "legacy INTERVAL little-endian inverse" begin
    element = binarylogicalelement("interval", MD.Type.FIXED_LEN_BYTE_ARRAY;
        width=12, converted=MD.ConvertedType.INTERVAL)
    expected = Parquet.Interval(UInt32(1), UInt32(0x01020304), typemax(UInt32))
    bytes = vcat(littleuint32(expected.months), littleuint32(expected.days),
        littleuint32(expected.milliseconds))
    @test Parquet._binarylogicalvalue(element, bytes) == expected
    @test Parquet._binaryphysicalvalue(element, expected) == bytes
    @test isequal(copy(expected), expected)
    @test hash(copy(expected)) == hash(expected)
    values = Union{Missing,Vector{UInt8}}[bytes, missing, zeros(UInt8, 12)]
    decoded = Parquet._binarylogicalvalues(element, values)
    @test decoded isa Vector{Union{Missing,Parquet.Interval}}
    @test isequal(decoded, Union{Missing,Parquet.Interval}[
        expected, missing, Parquet.Interval(0, 0, 0)])
    @test isequal(Parquet._binaryphysicalvalues(element, decoded), values)
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(element, bytes[1:11])
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(element, bytes[1:1])
    @test_throws ArgumentError Parquet._binaryphysicalvalue(element, (1, 2, 3))
    @test_throws ArgumentError Parquet.Interval(-1, 0, 0)
    @test_throws ArgumentError Parquet.Interval(0, typemax(UInt32) + UInt64(1), 0)
end

@testset "UNKNOWN always-null contract" begin
    element = binarylogicalelement("null", MD.Type.INT32;
        logical=MD.LogicalType(UNKNOWN=MD.NullType()))
    @test Parquet._binarylogicalvalue(element, missing) === missing
    @test Parquet._binaryphysicalvalue(element, missing) === missing
    decoded = Parquet._binarylogicalvalues(element, Missing[missing, missing])
    encoded = Parquet._binaryphysicalvalues(element, decoded)
    @test decoded isa Vector{Missing} && all(ismissing, decoded)
    @test encoded isa Vector{Missing} && all(ismissing, encoded)
    @test_throws Parquet.FormatError Parquet._binarylogicalvalue(element, Int32(0))
    @test_throws Parquet.FormatError Parquet._binarylogicalvalues(
        element, Union{Missing,Int32}[missing, 0])
    @test_throws ArgumentError Parquet._binaryphysicalvalue(element, nothing)
    @test_throws ArgumentError Parquet._binaryphysicalvalues(element, Any[missing, 0])
end

@testset "binary logical limits and fallthrough" begin
    json = binarylogicalelement("json", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(JSON=MD.JsonType()))
    bson = binarylogicalelement("bson", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(BSON=MD.BsonType()))
    enum = binarylogicalelement("enum", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(ENUM=MD.EnumType()))
    tinycontainer = Parquet.Limits(max_container_elements=1)
    tinybytes = Parquet.Limits(max_string_bytes=1)
    for element in (json, bson, enum)
        @test_throws Parquet.LimitError Parquet._binarylogicalvalues(
            element, [UInt8[0x61], UInt8[0x62]]; limits=tinycontainer)
    end
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        json, UInt8[0x61, 0x62]; limits=tinybytes)
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        bson, UInt8[0x61, 0x62]; limits=tinybytes)
    @test_throws Parquet.LimitError Parquet._binarylogicalvalue(
        enum, UInt8[0x61, 0x62]; limits=tinybytes)
    @test_throws Parquet.LimitError Parquet._binaryphysicalvalue(
        json, Parquet.JSONValue(UInt8[0x22, 0x61, 0x22]); limits=tinybytes)
    @test_throws Parquet.LimitError Parquet._binaryphysicalvalue(
        bson, Parquet.BSONValue(UInt8[0x05, 0x00, 0x00, 0x00, 0x00]);
        limits=tinybytes)
    plain = binarylogicalelement("plain", MD.Type.BYTE_ARRAY)
    values = [UInt8[0x61]]
    @test Parquet._binarylogicalkind(plain) === nothing
    @test Parquet._binarylogicaleltype(plain, Vector{UInt8}) === nothing
    @test Parquet._binarylogicalvalue(plain, values[1]) === nothing
    @test Parquet._binaryphysicalvalue(plain, values[1]) === nothing
    @test Parquet._binarylogicalvalues(plain, values) === nothing
    @test Parquet._binaryphysicalvalues(plain, values) === nothing
end
