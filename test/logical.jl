using Dates

if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end

function logicaltestelement(name, physical; logical=nothing, converted=nothing)
    return MD.SchemaElement(name=name, type_=physical,
        repetition_type=MD.FieldRepetitionType.OPTIONAL,
        logicalType=logical, converted_type=converted)
end

@testset "logical annotation precedence and validation" begin
    stringtype = MD.LogicalType(STRING=MD.StringType())
    datetype = MD.LogicalType(DATE=MD.DateType())
    modernstring = logicaltestelement("value", MD.Type.BYTE_ARRAY;
        logical=stringtype, converted=MD.ConvertedType.DATE)
    moderndate = logicaltestelement("value", MD.Type.INT32;
        logical=datetype, converted=MD.ConvertedType.UTF8)
    @test Parquet._logicalkind(modernstring) === :string
    @test Parquet._logicalkind(moderndate) === :date
    @test Parquet._logicaleltype(modernstring, Vector{UInt8}) === Parquet.DataStrings.DataString
    @test Parquet._logicaleltype(moderndate, Int32) === Date

    node = Parquet.SchemaNode(moderndate, ["value"], Int16(1), Int16(0),
        Int32(1), Parquet.SchemaNode[])
    @test Parquet._logicalkind(node) === :date
    @test Parquet._logicaleltype(node, Int32) === Date

    legacystring = logicaltestelement("value", MD.Type.BYTE_ARRAY;
        converted=MD.ConvertedType.UTF8)
    legacydate = logicaltestelement("value", MD.Type.INT32;
        converted=MD.ConvertedType.DATE)
    @test Parquet._logicalkind(legacystring) === :string
    @test Parquet._logicalkind(legacydate) === :date

    unknown = MD.LogicalType(unknown_fields=(TH.RawField(2555, TH.STRUCT, UInt8[0x00]),))
    unknownmodern = logicaltestelement("value", MD.Type.BYTE_ARRAY;
        logical=unknown, converted=MD.ConvertedType.UTF8)
    unsupportedmodern = logicaltestelement("value", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(VARIANT=MD.VariantType(specification_version=Int8(1))),
        converted=MD.ConvertedType.UTF8)
    physical = [UInt8[0x61], UInt8[0x62]]
    @test Parquet._logicalkind(unknownmodern) === nothing
    @test Parquet._logicalkind(unsupportedmodern) === nothing
    @test Parquet._logicalvalues(unknownmodern, physical) === physical
    @test Parquet._logicalvalues(unsupportedmodern, physical) === physical
    @test Parquet._physicalvalues(unknownmodern, physical) === physical

    plain = logicaltestelement("value", MD.Type.INT64)
    unknownlegacy = logicaltestelement("value", MD.Type.INT64;
        converted=MD.ConvertedType.T(999))
    integers = Int64[1, 2]
    @test Parquet._logicalvalues(plain, integers) === integers
    @test Parquet._logicalvalues(unknownlegacy, integers) === integers

    @test_throws Parquet.FormatError Parquet._logicalkind(
        logicaltestelement("bad", MD.Type.INT32; logical=stringtype))
    @test_throws Parquet.FormatError Parquet._logicalkind(
        logicaltestelement("bad", MD.Type.INT64; logical=datetype))
    @test_throws Parquet.FormatError Parquet._logicalkind(
        logicaltestelement("bad", MD.Type.INT32; converted=MD.ConvertedType.UTF8))
    @test_throws Parquet.FormatError Parquet._logicalkind(
        logicaltestelement("bad", MD.Type.INT64; converted=MD.ConvertedType.DATE))
end

@testset "DATE scalar conversion" begin
    epoch = Date(1970, 1, 1)
    @test Parquet._fromparquetdate(Int32(-1)) == Date(1969, 12, 31)
    @test Parquet._fromparquetdate(Int32(0)) == epoch
    @test Parquet._fromparquetdate(Int32(11016)) == Date(2000, 2, 29)
    @test Parquet._toparquetdate(Date(1969, 12, 31)) == Int32(-1)
    @test Parquet._toparquetdate(epoch) == Int32(0)
    @test Parquet._toparquetdate(Date(2000, 2, 29)) == Int32(11016)

    for days in (typemin(Int32), typemax(Int32))
        @test Parquet._toparquetdate(Parquet._fromparquetdate(days)) == days
    end
    epochday = Dates.value(epoch)
    below = Date(Dates.UTD(epochday + Int64(typemin(Int32)) - 1))
    above = Date(Dates.UTD(epochday + Int64(typemax(Int32)) + 1))
    @test_throws ArgumentError Parquet._toparquetdate(below)
    @test_throws ArgumentError Parquet._toparquetdate(above)
    @test_throws ArgumentError Parquet._toparquetdate(Date(Dates.UTD(typemin(Int64))))
end

@testset "logical vector conversion" begin
    datetype = logicaltestelement("date", MD.Type.INT32;
        logical=MD.LogicalType(DATE=MD.DateType()), converted=MD.ConvertedType.DATE)
    physicaldates = Int32[-1, 0, 11016]
    dates = Date[Date(1969, 12, 31), Date(1970, 1, 1), Date(2000, 2, 29)]
    @test Parquet._logicalvalues(datetype, physicaldates) == dates
    @test Parquet._physicalvalues(datetype, dates) == physicaldates

    optionalphysical = Union{Missing,Int32}[missing, -1, 0, 11016]
    optionallogical = Union{Missing,Date}[
        missing, Date(1969, 12, 31), Date(1970, 1, 1), Date(2000, 2, 29)]
    decoded = Parquet._logicalvalues(datetype, optionalphysical)
    encoded = Parquet._physicalvalues(datetype, optionallogical)
    @test decoded isa Vector{Union{Missing,Date}}
    @test encoded isa Vector{Union{Missing,Int32}}
    @test isequal(decoded, optionallogical)
    @test isequal(encoded, optionalphysical)
    @test Parquet._logicalvalue(datetype, missing) === missing
    @test Parquet._physicalvalue(datetype, missing) === missing
    @test_throws Parquet.FormatError Parquet._logicalvalue(datetype, Int64(0))
    @test_throws ArgumentError Parquet._physicalvalue(datetype, Int32(0))

    stringtype = logicaltestelement("text", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(STRING=MD.StringType()), converted=MD.ConvertedType.UTF8)
    first = UInt8[0x61, 0x6c, 0x70, 0x68, 0x61]
    physicalstrings = [first, UInt8[0xce, 0xb2]]
    strings = Parquet._logicalvalues(stringtype, physicalstrings)
    @test strings == ["alpha", "β"]
    @test first == UInt8[0x61, 0x6c, 0x70, 0x68, 0x61]
    @test Parquet._physicalvalues(stringtype, strings) == physicalstrings

    optionalbytes = Union{Missing,Vector{UInt8}}[UInt8[0xce, 0xba], missing]
    optionalstrings = Parquet._logicalvalues(stringtype, optionalbytes)
    @test optionalstrings isa Vector{Union{Missing,Parquet.DataStrings.DataString}}
    @test isequal(optionalstrings, Union{Missing,String}["κ", missing])
    @test isequal(Parquet._physicalvalues(stringtype, optionalstrings), optionalbytes)

    @test_throws Parquet.FormatError Parquet._logicalvalues(stringtype, [UInt8[0xff]])
    invalid = String(copy(UInt8[0xff]))
    @test !isvalid(invalid)
    @test_throws ArgumentError Parquet._physicalvalues(stringtype, [invalid])
    @test_throws Parquet.FormatError Parquet._logicalvalue(stringtype, Int32(1))
    @test_throws ArgumentError Parquet._physicalvalue(stringtype, UInt8[0x61])
end

@testset "logical conversion limits" begin
    datetype = logicaltestelement("date", MD.Type.INT32;
        logical=MD.LogicalType(DATE=MD.DateType()))
    stringtype = logicaltestelement("text", MD.Type.BYTE_ARRAY;
        logical=MD.LogicalType(STRING=MD.StringType()))
    tinycontainer = Parquet.Limits(max_container_elements=1)
    tinystring = Parquet.Limits(max_string_bytes=1)
    @test_throws Parquet.LimitError Parquet._logicalvalues(
        datetype, Int32[0, 1]; limits=tinycontainer)
    @test_throws Parquet.LimitError Parquet._physicalvalues(
        datetype, Date[Date(1970, 1, 1), Date(1970, 1, 2)]; limits=tinycontainer)
    @test_throws Parquet.LimitError Parquet._logicalvalues(
        stringtype, [UInt8[0x61, 0x62]]; limits=tinystring)
    @test_throws Parquet.LimitError Parquet._physicalvalues(
        stringtype, ["ab"]; limits=tinystring)
end
