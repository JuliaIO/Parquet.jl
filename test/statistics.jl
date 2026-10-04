using SHA

const STMD = Parquet.Metadata
const STTH = Parquet.Thrift

struct STBlockingOrders <: AbstractVector{STMD.ColumnOrder}
    values::Vector{STMD.ColumnOrder}
    entered::Channel{Nothing}
    release::Channel{Nothing}
end

function Base.size(orders::STBlockingOrders)
    return size(orders.values)
end

function Base.getindex(orders::STBlockingOrders, index::Int)
    put!(orders.entered, nothing)
    take!(orders.release)
    return orders.values[index]
end

function st_leaf(type; name="value", width=nothing, logical=nothing,
    converted=nothing, precision=nothing, scale=nothing)
    return STMD.SchemaElement(type_=type, type_length=width,
        repetition_type=STMD.FieldRepetitionType.OPTIONAL, name=name,
        logicalType=logical, converted_type=converted, precision=precision,
        scale=scale)
end

function st_schema(leaves::Vector{STMD.SchemaElement})
    root = STMD.SchemaElement(repetition_type=STMD.FieldRepetitionType.REQUIRED,
        name="schema", num_children=Int32(length(leaves)))
    return Parquet.Schema(vcat(STMD.SchemaElement[root], leaves))
end

function st_metadata(leaf::STMD.SchemaElement, statistics=nothing;
    total::Int64=4, path=String[leaf.name])
    return STMD.ColumnMetaData(type_=leaf.type_, encodings=[STMD.Encoding.PLAIN],
        path_in_schema=path, codec=STMD.CompressionCodec.UNCOMPRESSED,
        num_values=total, total_uncompressed_size=Int64(0),
        total_compressed_size=Int64(0), data_page_offset=Int64(0),
        statistics=statistics)
end

function st_type_order()
    return STMD.ColumnOrder(TYPE_ORDER=STMD.TypeDefinedOrder())
end

function st_ieee_order()
    return STMD.ColumnOrder(IEEE_754_TOTAL_ORDER=STMD.IEEE754TotalOrder())
end

function st_unknown_order()
    raw = STTH.RawField(77, STTH.STRUCT, UInt8[0x00])
    return STMD.ColumnOrder(unknown_fields=(raw,))
end

function st_le16(bits::UInt16)
    return UInt8[UInt8(bits & 0xff), UInt8(bits >> 8)]
end

function st_le32(bits::UInt32)
    return UInt8[UInt8(bits & 0xff), UInt8((bits >> 8) & 0xff),
        UInt8((bits >> 16) & 0xff), UInt8(bits >> 24)]
end

function st_le64(bits::UInt64)
    return UInt8[UInt8(bits & 0xff), UInt8((bits >> 8) & 0xff),
        UInt8((bits >> 16) & 0xff), UInt8((bits >> 24) & 0xff),
        UInt8((bits >> 32) & 0xff), UInt8((bits >> 40) & 0xff),
        UInt8((bits >> 48) & 0xff), UInt8(bits >> 56)]
end

function st_i32(value::Integer)
    return st_le32(reinterpret(UInt32, Int32(value)))
end

function st_i64(value::Integer)
    return st_le64(reinterpret(UInt64, Int64(value)))
end

function st_f32(value::Float32)
    return st_le32(reinterpret(UInt32, value))
end

function st_f64(value::Float64)
    return st_le64(reinterpret(UInt64, value))
end

function st_f16bits(bits::Integer)
    return st_le16(UInt16(bits))
end

function st_facts(leaf::STMD.SchemaElement, statistics=nothing;
    order=st_type_order(), created_by="Parquet.jl version 1.0.0",
    total::Int64=4, limits=Parquet.Limits(), budget=nothing)
    schema = st_schema(STMD.SchemaElement[leaf])
    orders = order === nothing ? nothing : STMD.ColumnOrder[order]
    metadata = st_metadata(leaf, statistics; total=total)
    budget === nothing && return Parquet._statisticsfacts(schema, 1, created_by,
        orders, metadata; limits=limits)
    return Parquet._statisticsfacts(schema, 1, created_by, orders, metadata;
        limits=limits, budget=budget)
end

function st_modern(lower, upper; nulls=nothing, nans=nothing, distinct=nothing,
    lower_exact=nothing, upper_exact=nothing)
    return STMD.Statistics(min_value=lower, max_value=upper,
        null_count=nulls, nan_count=nans, distinct_count=distinct,
        is_min_value_exact=lower_exact, is_max_value_exact=upper_exact)
end

function st_deprecated(lower, upper; nulls=nothing, nans=nothing,
    distinct=nothing)
    return STMD.Statistics(min=lower, max=upper, null_count=nulls,
        nan_count=nans, distinct_count=distinct)
end

function st_decimaltype(precision::Integer)
    return STMD.LogicalType(DECIMAL=STMD.DecimalType(scale=Int32(0),
        precision=Int32(precision)))
end

@testset "reader statistics physical and logical orders" begin
    physical = (
        (st_leaf(STMD.Type.BOOLEAN), :boolean),
        (st_leaf(STMD.Type.INT32), :signed),
        (st_leaf(STMD.Type.INT64), :signed),
        (st_leaf(STMD.Type.INT96), :undefined),
        (st_leaf(STMD.Type.FLOAT), :floating),
        (st_leaf(STMD.Type.DOUBLE), :floating),
        (st_leaf(STMD.Type.BYTE_ARRAY), :unsigned_bytes),
        (st_leaf(STMD.Type.FIXED_LEN_BYTE_ARRAY; width=Int32(3)), :unsigned_bytes),
    )
    for (leaf, comparison) in physical
        facts = st_facts(leaf)
        @test facts.order.comparison == comparison
    end

    signed = STMD.LogicalType(INTEGER=STMD.IntType(bitWidth=Int8(8),
        isSigned=true))
    unsigned = STMD.LogicalType(INTEGER=STMD.IntType(bitWidth=Int8(32),
        isSigned=false))
    stringtype = STMD.LogicalType(STRING=STMD.StringType())
    enumtype = STMD.LogicalType(ENUM=STMD.EnumType())
    uuidtype = STMD.LogicalType(UUID=STMD.UUIDType())
    date = STMD.LogicalType(DATE=STMD.DateType())
    millis = STMD.TimeUnit(MILLIS=STMD.MilliSeconds())
    time = STMD.LogicalType(TIME=STMD.TimeType(isAdjustedToUTC=false, unit=millis))
    timestamp = STMD.LogicalType(TIMESTAMP=STMD.TimestampType(
        isAdjustedToUTC=false, unit=millis))
    float16 = STMD.LogicalType(FLOAT16=STMD.Float16Type())
    logical = (
        (st_leaf(STMD.Type.INT32; logical=signed), :signed),
        (st_leaf(STMD.Type.INT32; logical=unsigned), :unsigned),
        (st_leaf(STMD.Type.BYTE_ARRAY; logical=stringtype), :unsigned_bytes),
        (st_leaf(STMD.Type.BYTE_ARRAY; logical=enumtype), :unsigned_bytes),
        (st_leaf(STMD.Type.FIXED_LEN_BYTE_ARRAY; width=Int32(16), logical=uuidtype),
            :unsigned_bytes),
        (st_leaf(STMD.Type.INT32; logical=date), :signed),
        (st_leaf(STMD.Type.INT32; logical=time), :signed),
        (st_leaf(STMD.Type.INT64; logical=timestamp), :signed),
        (st_leaf(STMD.Type.FIXED_LEN_BYTE_ARRAY; width=Int32(2), logical=float16),
            :floating),
    )
    for (leaf, comparison) in logical
        @test st_facts(leaf).order.comparison == comparison
    end

    undefined = (
        st_leaf(STMD.Type.FIXED_LEN_BYTE_ARRAY; width=Int32(12),
            converted=STMD.ConvertedType.INTERVAL),
        st_leaf(STMD.Type.INT32; logical=STMD.LogicalType(UNKNOWN=STMD.NullType())),
        st_leaf(STMD.Type.BYTE_ARRAY;
            logical=STMD.LogicalType(VARIANT=STMD.VariantType())),
        st_leaf(STMD.Type.BYTE_ARRAY;
            logical=STMD.LogicalType(GEOMETRY=STMD.GeometryType())),
        st_leaf(STMD.Type.BYTE_ARRAY;
            logical=STMD.LogicalType(GEOGRAPHY=STMD.GeographyType())),
    )
    for leaf in undefined
        @test st_facts(leaf).order.comparison == :undefined
        ignored = st_facts(leaf, st_modern(fill(UInt8(0),
            something(Parquet._statisticplainwidth(leaf), 1)), fill(UInt8(0),
            something(Parquet._statisticplainwidth(leaf), 1))))
        @test ignored.lower.state == :unknown
        @test ignored.lower.reason == :undefined_order
    end

    modernwins = st_leaf(STMD.Type.BYTE_ARRAY; logical=stringtype,
        converted=STMD.ConvertedType.INTERVAL)
    @test st_facts(modernwins).order.comparison == :unsigned_bytes
    unsupported = st_leaf(STMD.Type.BYTE_ARRAY;
        logical=STMD.LogicalType(VARIANT=STMD.VariantType()))
    @test st_facts(unsupported,
        st_modern(UInt8[0x01], UInt8[0x02])).lower.state == :unknown

    futurelogical = STMD.LogicalType(unknown_fields=(
        STTH.RawField(91, STTH.STRUCT, UInt8[0x00]),))
    futurefloat = st_leaf(STMD.Type.FLOAT; logical=futurelogical)
    futurestats = st_modern(st_f32(-1.0f0), st_f32(1.0f0);
        nulls=Int64(0), nans=Int64(0))
    futuretype = st_facts(futurefloat, futurestats)
    @test futuretype.order.comparison == :undefined
    @test futuretype.lower.reason == :undefined_order
    @test futuretype.nan_count.value == 0
    futureieee = st_facts(futurefloat, futurestats; order=st_ieee_order())
    @test futureieee.order.comparison == :ieee_total_order
    @test futureieee.lower.state == :known
    @test futureieee.upper.state == :known
end

@testset "reader statistics families, identity, and exactness" begin
    leaf = st_leaf(STMD.Type.INT32)
    lower = st_i32(-3)
    upper = st_i32(8)
    deprecated_lower = st_i32(-100)
    statistics = STMD.Statistics(min=deprecated_lower, max=st_i32(100),
        min_value=lower, is_min_value_exact=true)
    facts = st_facts(leaf, statistics)
    @test facts.family == :modern
    @test facts.lower.state == :known
    @test facts.lower.raw === lower
    @test facts.lower.exactness == :exact
    @test facts.upper.state == :absent
    @test facts.upper.raw === nothing

    facts = st_facts(leaf, st_modern(lower, upper; lower_exact=false,
        upper_exact=nothing))
    @test facts.lower.exactness == :inexact
    @test facts.upper.exactness == :unknown

    facts = st_facts(leaf, st_deprecated(lower, upper))
    @test facts.family == :deprecated
    @test facts.lower.state == :known
    @test facts.lower.exactness == :unknown
    @test st_facts(leaf, st_deprecated(lower, upper); order=nothing).lower.state ==
        :known

    unsigned = STMD.LogicalType(INTEGER=STMD.IntType(bitWidth=Int8(32),
        isSigned=false))
    unsignedleaf = st_leaf(STMD.Type.INT32; logical=unsigned)
    facts = st_facts(unsignedleaf, st_deprecated(st_le32(0x00000001),
        st_le32(0xffffffff)))
    @test facts.lower.state == :unknown
    @test facts.lower.reason == :deprecated_order_mismatch

    contradictory = st_facts(leaf, st_modern(st_i32(9), st_i32(2)))
    @test contradictory.lower.state == :unknown
    @test contradictory.upper.state == :unknown
    @test contradictory.lower.reason == :contradictory_bounds
    flagswithoutbounds = st_facts(leaf,
        STMD.Statistics(is_min_value_exact=true, is_max_value_exact=false))
    @test flagswithoutbounds.lower.state == :absent
    @test flagswithoutbounds.upper.state == :absent
end

@testset "reader statistics count validation" begin
    leaf = st_leaf(STMD.Type.FLOAT)
    facts = st_facts(leaf, st_modern(nothing, nothing; nulls=Int64(0),
        nans=Int64(0), distinct=Int64(0)))
    @test facts.null_count == Parquet._StatisticCountFact(:known, 0)
    @test facts.nan_count == Parquet._StatisticCountFact(:known, 0)
    @test facts.distinct_count == Parquet._StatisticCountFact(:known, 0)

    facts = st_facts(leaf, STMD.Statistics())
    @test facts.null_count.state == :absent
    @test facts.nan_count.state == :absent
    @test facts.distinct_count.state == :absent

    for statistics in (
        STMD.Statistics(null_count=Int64(-1)),
        STMD.Statistics(null_count=Int64(5)),
        STMD.Statistics(nan_count=Int64(-1)),
        STMD.Statistics(nan_count=Int64(5)),
        STMD.Statistics(distinct_count=Int64(-1)),
        STMD.Statistics(distinct_count=Int64(5)),
        STMD.Statistics(null_count=Int64(3), nan_count=Int64(2)),
        STMD.Statistics(null_count=Int64(2), distinct_count=Int64(3)),
    )
        @test_throws Parquet.FormatError st_facts(leaf, statistics)
    end
    @test_throws Parquet.FormatError st_facts(st_leaf(STMD.Type.INT32),
        STMD.Statistics(nan_count=Int64(0)))
    @test_throws Parquet.FormatError st_facts(leaf,
        STMD.Statistics(null_count=typemax(Int64), nan_count=typemax(Int64));
        total=typemax(Int64))

    allnull = st_facts(st_leaf(STMD.Type.INT32),
        st_modern(st_i32(1), st_i32(2); nulls=Int64(4)))
    @test allnull.lower.state == :unknown
    @test allnull.lower.reason == :no_non_null

    nested = st_facts(st_leaf(STMD.Type.INT32),
        STMD.Statistics(null_count=Int64(3)); total=Int64(7))
    @test nested.null_count.value == 3
    empty = st_facts(st_leaf(STMD.Type.INT32),
        st_modern(st_i32(1), st_i32(2)); total=Int64(0))
    @test empty.lower.reason == :no_non_null
end

@testset "reader statistics column orders and entry precedence" begin
    leaves = STMD.SchemaElement[st_leaf(STMD.Type.INT32; name="a"),
        st_leaf(STMD.Type.FLOAT; name="b")]
    schema = st_schema(leaves)
    metadata = st_metadata(leaves[1], nothing; path=["a"])
    @test_throws Parquet.ArgumentError Parquet._statisticsfacts(schema, 0,
        nothing, nothing, metadata)
    @test_throws Parquet.ArgumentError Parquet._statisticsfacts(schema, 3,
        nothing, nothing, metadata)
    @test_throws Parquet.FormatError Parquet._statisticsfacts(schema, 1,
        nothing, STMD.ColumnOrder[st_type_order()], metadata)
    @test_throws Parquet.FormatError Parquet._statisticsfacts(schema, 1,
        nothing, STMD.ColumnOrder[st_type_order(), st_type_order(), st_type_order()],
        metadata)
    @test_throws ArgumentError Parquet._statisticsfacts(schema, 1,
        nothing, STMD.ColumnOrder[st_type_order(), st_ieee_order()], metadata;
        limits=Parquet.Limits(max_statistics_value_bytes=-1))
    @test_throws Parquet.FormatError Parquet._statisticsfacts(schema, 1,
        nothing, nothing, st_metadata(leaves[2], nothing; path=["a"]))
    @test_throws Parquet.FormatError Parquet._statisticsfacts(schema, 1,
        nothing, nothing, st_metadata(leaves[1], nothing; path=["wrong"]))
    @test_throws Parquet.FormatError Parquet._statisticsfacts(schema, 1,
        nothing, nothing, st_metadata(leaves[1], nothing; total=Int64(-1), path=["a"]))

    entered = Channel{Nothing}(1)
    release = Channel{Nothing}(1)
    blocking = STBlockingOrders(STMD.ColumnOrder[st_type_order()], entered, release)
    single = st_schema(STMD.SchemaElement[leaves[1]])
    budget = Parquet._LiveByteBudget(
        Parquet.Limits(max_materialized_bytes=Int64(1024)))
    task = errormonitor(Threads.@spawn begin
        try
            return Parquet._statisticsfacts(single, 1, nothing, blocking,
                st_metadata(leaves[1], nothing; path=["wrong"]); budget=budget)
        catch err
            return err
        end
    end)
    take!(entered)
    Parquet._reserve!(budget, 100)
    put!(release, nothing)
    @test fetch(task) isa Parquet.FormatError
    @test Parquet._budgetused(budget) == 100
    Parquet._release!(budget, 100)

    badorders = STMD.ColumnOrder[st_type_order(), st_ieee_order()]
    @test Parquet._statisticsfacts(schema, 1, nothing, badorders, metadata).order.declared ==
        :type_order
    illegal = STMD.ColumnOrder[st_ieee_order(), st_ieee_order()]
    @test_throws Parquet.FormatError Parquet._statisticsfacts(schema, 2,
        nothing, illegal, st_metadata(leaves[2], nothing; path=["b"]))

    empty = STMD.ColumnOrder()
    @test_throws Parquet.FormatError st_facts(leaves[1], nothing; order=empty)
    unknown = st_facts(leaves[1], st_modern(st_i32(1), st_i32(2));
        order=st_unknown_order())
    @test unknown.order.declared == :unknown
    @test unknown.lower.state == :unknown
    @test unknown.null_count.state == :absent

    missing = st_facts(leaves[1], st_modern(st_i32(1), st_i32(2)); order=nothing)
    @test missing.order.declared == :absent
    @test missing.lower.reason == :missing_order

    ieeeinteger = st_facts(leaves[2], nothing; order=st_ieee_order())
    @test ieeeinteger.order.comparison == :ieee_total_order
end

@testset "reader statistics widths, limits, and semantic validation" begin
    fixed = st_leaf(STMD.Type.FIXED_LEN_BYTE_ARRAY; width=Int32(4))
    wrong = st_modern(UInt8[0x01, 0x02, 0x03], UInt8[0x01, 0x02, 0x03, 0x04])
    @test_throws Parquet.FormatError st_facts(fixed, wrong;
        limits=Parquet.Limits(max_statistics_value_bytes=Int64(0)))

    variable = st_leaf(STMD.Type.BYTE_ARRAY;
        logical=STMD.LogicalType(STRING=STMD.StringType()))
    invalidutf8 = UInt8[0xff]
    exact = st_facts(variable, st_modern(invalidutf8, invalidutf8);
        limits=Parquet.Limits(max_statistics_value_bytes=Int64(1)))
    @test exact.lower.state == :unknown
    @test exact.lower.reason == :invalid_value
    over = st_facts(variable, st_modern(invalidutf8, invalidutf8);
        limits=Parquet.Limits(max_statistics_value_bytes=Int64(0)))
    @test over.lower.state == :unknown
    @test over.lower.reason == :over_limit
    @test over.lower.raw === invalidutf8
    overmissing = st_facts(variable, st_modern(invalidutf8, invalidutf8);
        order=nothing, limits=Parquet.Limits(max_statistics_value_bytes=Int64(0)))
    @test overmissing.lower.reason == :over_limit
    @test_throws ArgumentError st_facts(variable, nothing;
        limits=Parquet.Limits(max_statistics_value_bytes=Int64(-1)))

    rawleaf = st_leaf(STMD.Type.BYTE_ARRAY)
    raw = UInt8[0x00, 0xff, 0x80]
    rawfacts = st_facts(rawleaf, st_modern(raw, raw))
    @test rawfacts.lower.state == :known
    @test rawfacts.lower.raw === raw
    emptyraw = UInt8[]
    @test st_facts(rawleaf, st_modern(emptyraw, emptyraw);
        limits=Parquet.Limits(max_statistics_value_bytes=Int64(0))).lower.state ==
        :known
    exactraw = UInt8[0x80]
    @test st_facts(rawleaf, st_modern(exactraw, exactraw);
        limits=Parquet.Limits(max_statistics_value_bytes=Int64(1))).lower.state ==
        :known

    boolean = st_leaf(STMD.Type.BOOLEAN)
    @test st_facts(boolean, st_modern(UInt8[0x00], UInt8[0x01])).lower.state == :known
    @test st_facts(boolean, st_modern(UInt8[0x02], UInt8[0x02])).lower.state == :unknown
    @test_throws Parquet.FormatError st_facts(boolean,
        st_modern(UInt8[], UInt8[0x01]))

    signed8 = st_leaf(STMD.Type.INT32; logical=STMD.LogicalType(
        INTEGER=STMD.IntType(bitWidth=Int8(8), isSigned=true)))
    unsigned8 = st_leaf(STMD.Type.INT32; logical=STMD.LogicalType(
        INTEGER=STMD.IntType(bitWidth=Int8(8), isSigned=false)))
    @test st_facts(signed8, st_modern(st_i32(-128), st_i32(127))).lower.state == :known
    @test st_facts(signed8, st_modern(st_i32(-129), st_i32(127))).lower.state == :unknown
    @test st_facts(unsigned8, st_modern(st_le32(0x00000000),
        st_le32(0x000000ff))).upper.state == :known
    @test st_facts(unsigned8, st_modern(st_le32(0x00000100),
        st_le32(0x00000100))).lower.state == :unknown

    millis = STMD.TimeUnit(MILLIS=STMD.MilliSeconds())
    timeleaf = st_leaf(STMD.Type.INT32; logical=STMD.LogicalType(
        TIME=STMD.TimeType(isAdjustedToUTC=false, unit=millis)))
    @test st_facts(timeleaf, st_modern(st_i32(0), st_i32(86_399_999))).upper.state ==
        :known
    @test st_facts(timeleaf, st_modern(st_i32(-1), st_i32(86_400_000))).lower.state ==
        :unknown

    jsonleaf = st_leaf(STMD.Type.BYTE_ARRAY;
        logical=STMD.LogicalType(JSON=STMD.JsonType()))
    bsonleaf = st_leaf(STMD.Type.BYTE_ARRAY;
        logical=STMD.LogicalType(BSON=STMD.BsonType()))
    validjsonlower = Vector{UInt8}(codeunits("{\"a\":1}"))
    validjsonupper = Vector{UInt8}(codeunits("{\"a\":2}"))
    validjson = st_modern(validjsonlower, validjsonupper)
    @test st_facts(jsonleaf, validjson).lower.state == :known
    @test st_facts(jsonleaf, st_modern(UInt8[0x7b], UInt8[0x7b])).lower.state == :unknown
    tinybudget = Parquet._LiveByteBudget(
        Parquet.Limits(max_materialized_bytes=Int64(127)))
    @test_throws Parquet.LimitError st_facts(jsonleaf, validjson; budget=tinybudget)
    @test Parquet._budgetused(tinybudget) == 0
    exactbudget = Parquet._LiveByteBudget(
        Parquet.Limits(max_materialized_bytes=Int64(128)))
    @test st_facts(jsonleaf, validjson; budget=exactbudget).lower.state == :known
    @test Parquet._budgetused(exactbudget) == 0
    emptybson = UInt8[0x05, 0x00, 0x00, 0x00, 0x00]
    @test st_facts(bsonleaf, st_modern(emptybson, emptybson)).lower.state == :known
    @test st_facts(bsonleaf, st_modern(UInt8[0x00], UInt8[0x00])).lower.state == :unknown
end

@testset "reader statistics decimal orders and precision" begin
    int32leaf = st_leaf(STMD.Type.INT32; logical=st_decimaltype(3))
    int64leaf = st_leaf(STMD.Type.INT64; logical=st_decimaltype(18))
    byteleaf = st_leaf(STMD.Type.BYTE_ARRAY; logical=st_decimaltype(5))
    fixedleaf = st_leaf(STMD.Type.FIXED_LEN_BYTE_ARRAY; width=Int32(4),
        logical=st_decimaltype(9))

    @test st_facts(int32leaf, st_modern(st_i32(-999), st_i32(999))).lower.state ==
        :known
    @test st_facts(int32leaf, st_modern(st_i32(-1000), st_i32(1000))).lower.state ==
        :unknown
    @test st_facts(int64leaf, st_modern(st_i64(-999_999_999_999_999_999),
        st_i64(999_999_999_999_999_999))).upper.state == :known
    @test st_facts(int64leaf, st_modern(st_i64(typemin(Int64)),
        st_i64(typemin(Int64)))).lower.state == :unknown

    negative = UInt8[0xff, 0x7f]
    positive = UInt8[0x00, 0x80]
    bytefacts = st_facts(byteleaf, st_modern(negative, positive))
    @test bytefacts.lower.state == :known
    @test bytefacts.upper.state == :known
    @test Parquet._comparestatisticvalues(byteleaf, negative, positive, :decimal) == -1
    @test Parquet._comparestatisticvalues(byteleaf, UInt8[0xff], UInt8[0xff, 0xff],
        :decimal) == 0
    @test Parquet._comparestatisticvalues(byteleaf, UInt8[0xff, 0x7f], UInt8[0x80],
        :decimal) == -1

    @test st_facts(byteleaf, st_modern(UInt8[], UInt8[])).lower.state == :unknown
    @test st_facts(byteleaf, st_modern(UInt8[0x0f, 0x42, 0x40],
        UInt8[0x0f, 0x42, 0x40])).lower.state == :unknown
    @test st_facts(fixedleaf, st_modern(UInt8[0xff, 0xff, 0xfc, 0x18],
        UInt8[0x00, 0x00, 0x03, 0xe8])).lower.state == :known

    budget = Parquet._LiveByteBudget(Parquet.Limits(max_materialized_bytes=Int64(67)))
    @test_throws Parquet.LimitError st_facts(byteleaf,
        st_modern(UInt8[0x01, 0x02, 0x03, 0x04], UInt8[0x05]); budget=budget)
    @test Parquet._budgetused(budget) == 0
    retained = Parquet._LiveByteBudget(
        Parquet.Limits(max_materialized_bytes=Int64(74)))
    Parquet._reserve!(retained, 7)
    @test_throws Parquet.LimitError st_facts(byteleaf,
        st_modern(UInt8[0x01, 0x02, 0x03, 0x04], UInt8[0x05]); budget=retained)
    @test Parquet._budgetused(retained) == 7
    Parquet._release!(retained, 7)
end

@testset "reader statistics floating policies" begin
    floatleaf = st_leaf(STMD.Type.FLOAT)
    doubleleaf = st_leaf(STMD.Type.DOUBLE)
    float16leaf = st_leaf(STMD.Type.FIXED_LEN_BYTE_ARRAY; width=Int32(2),
        logical=STMD.LogicalType(FLOAT16=STMD.Float16Type()))

    for (leaf, lower, upper) in (
        (floatleaf, st_f32(-Inf32), st_f32(Inf32)),
        (doubleleaf, st_f64(-Inf), st_f64(Inf)),
        (float16leaf, st_f16bits(0xfc00), st_f16bits(0x7c00)),
    )
        facts = st_facts(leaf, st_modern(lower, upper); order=st_ieee_order())
        @test facts.lower.state == :known
        @test facts.upper.state == :known
        @test facts.order.comparison == :ieee_total_order
    end

    positivezero = st_f32(0.0f0)
    negativezero = st_f32(-0.0f0)
    typezeros = st_facts(floatleaf, st_modern(positivezero, negativezero;
        lower_exact=true, upper_exact=true))
    @test typezeros.lower.adjustment == :negative_zero
    @test typezeros.upper.adjustment == :positive_zero
    @test typezeros.lower.exactness == :inexact
    @test typezeros.upper.exactness == :inexact
    @test typezeros.lower.raw === positivezero
    @test typezeros.upper.raw === negativezero

    ieeezeros = st_facts(floatleaf, st_modern(negativezero, positivezero;
        nulls=Int64(0), nans=Int64(0)); order=st_ieee_order())
    @test ieeezeros.lower.state == :known
    @test ieeezeros.upper.state == :known
    @test Parquet._comparestatisticvalues(floatleaf, negativezero, positivezero,
        :ieee_total_order) == -1

    quietnan = st_le32(0x7fc00001)
    signalingnan = st_le32(0x7f800001)
    @test Parquet._comparestatisticvalues(floatleaf, quietnan, st_f32(1.0f0),
        :floating) === nothing
    @test Parquet._comparestatisticvalues(floatleaf, st_f32(1.0f0), quietnan,
        :floating) === nothing
    insufficient = st_facts(floatleaf, st_modern(quietnan, st_f32(3.0f0)))
    @test insufficient.lower.state == :unknown
    @test insufficient.upper.state == :known
    @test insufficient.lower.reason == :nan_type_order

    allnantype = st_facts(floatleaf, st_modern(quietnan, signalingnan;
        nulls=Int64(0), nans=Int64(4)))
    @test allnantype.lower.reason == :all_nan_type_order
    @test allnantype.upper.reason == :all_nan_type_order

    allnanieee = st_facts(floatleaf, st_modern(signalingnan, quietnan;
        nulls=Int64(0), nans=Int64(4)); order=st_ieee_order())
    @test allnanieee.lower.state == :known
    @test allnanieee.upper.state == :known
    @test Parquet._comparestatisticvalues(floatleaf, signalingnan, quietnan,
        :ieee_total_order) < 0

    wrongallnan = st_facts(floatleaf, st_modern(st_f32(1.0f0), quietnan;
        nulls=Int64(0), nans=Int64(4)); order=st_ieee_order())
    @test wrongallnan.lower.reason == :ieee_bound_kind
    @test wrongallnan.upper.reason == :ieee_bound_kind
    mixedcontradiction = st_facts(floatleaf, st_modern(st_f32(1.0f0), quietnan;
        nulls=Int64(0), nans=Int64(1)); order=st_ieee_order())
    @test mixedcontradiction.lower.reason == :ieee_bound_kind
    @test mixedcontradiction.upper.reason == :ieee_bound_kind
    missingcounts = st_facts(floatleaf, st_modern(quietnan, st_f32(2.0f0));
        order=st_ieee_order())
    @test missingcounts.lower.reason == :unproven_ieee_nan
    @test missingcounts.upper.state == :known

    empty = st_facts(floatleaf, st_modern(st_f32(1.0f0), st_f32(2.0f0);
        nulls=Int64(4), nans=Int64(0)); order=st_ieee_order())
    @test empty.lower.reason == :no_non_null

    deprecated = st_facts(floatleaf,
        st_deprecated(st_f32(-1.0f0), st_f32(1.0f0));
        created_by="parquet-cpp version 1.2.9")
    @test deprecated.lower.state == :known
    @test deprecated.upper.state == :known
    @test deprecated.trust.state == :trusted

    seen = falses(65_536)
    ordered = Vector{UInt8}(undef, 2 * 65_536)
    unique = true
    roundtrip = true
    nancount = 0
    zerocount = 0
    for rawbits in UInt32(0):UInt32(0xffff)
        bits = UInt16(rawbits)
        raw = st_le16(bits)
        roundtrip &= Parquet._statisticfloatbits(float16leaf, raw) == bits
        key = Parquet._statisticieeekey(bits)
        position = Int(key) + 1
        unique &= !seen[position]
        seen[position] = true
        ordered[2 * position - 1] = raw[1]
        ordered[2 * position] = raw[2]
        nancount += Parquet._statisticisnan(bits)
        zerocount += Parquet._statisticiszero(bits)
    end
    @test roundtrip
    @test unique
    @test all(seen)
    @test nancount == 2046
    @test zerocount == 2
    @test bytes2hex(SHA.sha256(ordered)) ==
        "61619b1a4260ee4cff6d462d21cab5a049198a27b3a9ae3e8ecfe246efc1d4b6"
end

@testset "reader statistics producer trust" begin
    decimal = STMD.LogicalType(DECIMAL=STMD.DecimalType(scale=Int32(0),
        precision=Int32(3)))
    signedbinary = st_leaf(STMD.Type.BYTE_ARRAY; logical=decimal)
    decimalstats = st_modern(UInt8[0xff], UInt8[0x01])
    for createdby in (nothing, "", "not a parsed created-by",
        "parquet-mr", "parquet-mr version 1.7.9",
        "parquet-mr version 1.7",
        "parquet-mr version 2147483648.0.0",
        "parquet-mr version 1.8.0-rc1",
        "parquet-mr version 1.8.1-2147483648",
        "parquet-mr version 1.8.1-alpha.2147483648",
        "parquet-mr version 1.5.0-cdh5.4.9",
        "parquet-mr version 1.5.0-.",
        "parquet-mr version 1.5.0-..",
        "parquet-mr version 1.5.0-cdh5.5.",
        "parquet-mr version 1.5.0-cdh5.5..",
        "parquet-mr version 1.5.0-cdh5.2147483648.0",
        "parquet-mr version 1.5.0-cdh10.0.0",
        "parquet-mr version 1.8.0 (not-build metadata)",
        "unrelated version 0.1.0 (not-build metadata)")
        facts = st_facts(signedbinary, decimalstats; created_by=createdby)
        @test facts.trust.state == :untrusted
        @test facts.trust.reason == :parquet_251
        @test facts.null_count.state == :absent
    end
    for createdby in ("parquet-mr version 1.8.0",
        "parquet-mr version 1.8.0 (build abc123)",
        "parquet-mr version 1.8.1-2147483647",
        "parquet-mr version 1.8.1+2147483648",
        "parquet-mr version 1.5.0-cdh5.5.0",
        "parquet-mr version 1.5.0-cdh5.5.0.",
        "parquet-mr version 1.5.0-cdh5.5.0..",
        "parquet-mr version 1.5.0-cdh5.5.0-SNAPSHOT",
        "parquet-mr version 1.5.0-cdh5.2147483648x.0",
        "parquet-mr version 1.5.0-cdh5.-1.0",
        "parquet-mr version 1.5.0-cdh5. 5.0",
        "parquet-mr version 1.5.0-cdh5.\u0665.0",
        "parquet-mr version 1.5.0-cdh5.6.1",
        "unrelated version 0.1.0")
        @test st_facts(signedbinary, decimalstats;
            created_by=createdby).trust.state == :trusted
    end

    rawleaf = st_leaf(STMD.Type.BYTE_ARRAY)
    distinct = st_modern(UInt8[0x01], UInt8[0x02])
    equal = st_modern(UInt8[0x01], UInt8[0x01])
    nobounds = st_facts(rawleaf, nothing; created_by=nothing)
    @test nobounds.trust.state == :trusted
    @test nobounds.trust.reason == :no_bounds
    bare = st_facts(rawleaf, distinct; created_by="parquet-cpp")
    @test bare.trust.state == :untrusted
    @test bare.trust.reason == :parquet_251
    @test st_facts(rawleaf, equal;
        created_by="parquet-cpp").trust.reason == :parquet_251
    @test st_facts(rawleaf, st_deprecated(UInt8[0x01], UInt8[0x02]);
        created_by="parquet-cpp").trust.reason == :parquet_251
    for createdby in ("parquet-cpp version 1.2.9",
        "parquet-cpp version 1.3.0-rc1",
        "parquet-cpp    version1.2.9",
        "parquet-cpp\tversion\t1.2.9",
        " \tparquet-cpp\nversion\n1.2.9 \r")
        facts = st_facts(rawleaf, distinct; created_by=createdby)
        @test facts.trust.state == :untrusted
        @test facts.trust.reason == :parquet_cpp_pre_1_3
    end
    @test st_facts(rawleaf, distinct;
        created_by="parquet-cpp version 1.3.0").trust.state == :trusted
    @test st_facts(rawleaf, distinct;
        created_by=" \tparquet-cpp    version1.3.0 \n").trust.state == :trusted
    @test st_facts(rawleaf, distinct;
        created_by="parquet-cpp-arrow version 1.3.0").trust.state == :trusted
    @test st_facts(rawleaf, distinct;
        created_by="parquet-cpp-arrow version 1.2.0").trust.state == :trusted
    @test st_facts(rawleaf, distinct;
        created_by="parquet-cpp version 1.3.0rc1").trust.state == :untrusted
    @test st_facts(rawleaf, equal;
        created_by="parquet-cpp version 1.2.9").trust.reason ==
        :affected_equal_bounds
    oversizedequal = fill(UInt8(0x01), 2)
    oversizedfacts = st_facts(rawleaf,
        st_modern(oversizedequal, oversizedequal);
        created_by="parquet-cpp version 1.2.9",
        limits=Parquet.Limits(max_statistics_value_bytes=Int64(1)))
    @test oversizedfacts.trust.reason == :parquet_cpp_pre_1_3
    @test oversizedfacts.lower.reason == :over_limit

    for createdby in ("parquet-mr version 1.9.9",
        "parquet-mr version 1.10.0-rc1",
        "parquet-mr    version1.9.9",
        "parquet-mr\tversion\t1.9.9",
        " \tparquet-mr\nversion\n1.9.9 \r")
        facts = st_facts(rawleaf, distinct; created_by=createdby)
        @test facts.trust.state == :untrusted
        @test facts.trust.reason == :parquet_mr_pre_1_10
    end
    @test st_facts(rawleaf, distinct;
        created_by="parquet-mr version 1.10.0").trust.state == :trusted
    @test st_facts(rawleaf, distinct;
        created_by=" \tparquet-mr\tversion\t1.10.0 \n").trust.state == :trusted
    @test st_facts(rawleaf, equal;
        created_by="parquet-mr version 1.9.0").trust.reason ==
        :affected_equal_bounds

    for createdby in ("foo\nbar version 1.0.0",
        "parquet-mr\nextra version 1.7.9",
        "parquet-cpp\nextra version 1.2.9")
        @test !Parquet._statisticsjavaproducer(createdby).parsed
        facts = st_facts(rawleaf, distinct; created_by=createdby)
        @test facts.trust.state == :untrusted
        @test facts.trust.reason == :parquet_251
    end
    for terminator in ("\r", "\r\n", "\u0085", "\u2028", "\u2029")
        createdby = "foo" * terminator * "bar version 1.0.0"
        @test !Parquet._statisticsjavaproducer(createdby).parsed
        facts = st_facts(rawleaf, distinct; created_by=createdby)
        @test facts.trust.state == :untrusted
        @test facts.trust.reason == :parquet_251
    end
    for (createdby, reason) in (("parquet-mr\nversion\n1.9.9",
            :parquet_mr_pre_1_10),
        ("parquet-cpp\nversion\n1.2.9", :parquet_cpp_pre_1_3))
        @test Parquet._statisticsjavaproducer(createdby).parsed
        facts = st_facts(rawleaf, distinct; created_by=createdby)
        @test facts.trust.state == :untrusted
        @test facts.trust.reason == reason
    end
    for separator in ("\r", "\r\n", "\v", "\f")
        createdby = "parquet-mr" * separator * "version 1.9.9"
        @test Parquet._statisticsjavaproducer(createdby).parsed
        facts = st_facts(rawleaf, distinct; created_by=createdby)
        @test facts.trust.state == :untrusted
        @test facts.trust.reason == :parquet_mr_pre_1_10
    end
    for separator in ("\u0085", "\u2028", "\u2029")
        createdby = "parquet-mr" * separator * "version 1.8.0"
        @test !Parquet._statisticsjavaproducer(createdby).parsed
        facts = st_facts(rawleaf, distinct; created_by=createdby)
        @test facts.trust.state == :untrusted
        @test facts.trust.reason == :parquet_251
    end
    for terminator in ("\n", "\r", "\r\n", "\u0085", "\u2028", "\u2029")
        mrcreatedby = "parquet-mr version 1.10.0+foo" * terminator * "bar"
        @test !Parquet._statisticsjavaproducer(mrcreatedby).version.present
        @test st_facts(rawleaf, distinct;
            created_by=mrcreatedby).trust.reason == :parquet_251
        cppcreatedby = "parquet-cpp version 1.3.0+foo" * terminator * "bar"
        @test !Parquet._statisticsjavaproducer(cppcreatedby).version.present
        @test st_facts(rawleaf, distinct;
            created_by=cppcreatedby).trust.reason == :parquet_cpp_pre_1_3
    end
    for separator in ("\v", "\f")
        for createdby in ("parquet-mr version 1.10.0+foo" * separator * "bar",
            "parquet-cpp version 1.3.0+foo" * separator * "bar")
            @test Parquet._statisticsjavaproducer(createdby).version.present
            @test st_facts(rawleaf, distinct;
                created_by=createdby).trust.state == :trusted
        end
    end
    backtracked = Parquet._statisticsjavaproducer(
        "foo version (bad) bar version 1.0.0")
    @test backtracked.parsed
    @test backtracked.application == :other
    for createdby in ("parquet-mr version (bad) x version 1.7.9",
        "parquet-cpp version (bad) x version 1.2.9")
        producer = Parquet._statisticsjavaproducer(createdby)
        @test producer.parsed
        @test producer.application == :other
        @test st_facts(rawleaf, distinct;
            created_by=createdby).trust.state == :trusted
    end
    backtrackinghostile = "parquet-mr" * repeat(" version (bad)", 8_192) *
        " version 1.0.0"
    @test Parquet._statisticsjavaproducer(backtrackinghostile).parsed
    hostile = "parquet-cpp" * repeat(" ", 32_768) * "not-version"
    @test !Parquet._statisticsjavaproducer(hostile).parsed

    signedleaf = st_leaf(STMD.Type.INT32)
    @test st_facts(signedleaf, st_modern(st_i32(1), st_i32(2));
        created_by=nothing).trust.state == :trusted
    unsigned = STMD.LogicalType(INTEGER=STMD.IntType(bitWidth=Int8(32),
        isSigned=false))
    unsignedleaf = st_leaf(STMD.Type.INT32; logical=unsigned)
    for createdby in ("parquet-cpp", "parquet-cpp version 1.2",
        "parquet-cpp version 2147483648.0.0",
        "parquet-cpp version 1.3.1-2147483648", "parquet-mr",
        "parquet-mr version 1.9", "parquet-mr version 1.10.1-2147483648")
        @test st_facts(unsignedleaf, st_modern(st_le32(0x00000001),
            st_le32(0x00000002)); created_by=createdby).trust.state == :untrusted
    end
    for createdby in ("parquet-cpp version 1.3.1-2147483647",
        "parquet-cpp version 1.3.1-2147483648x",
        "parquet-cpp version 1.3.1-2147483648)",
        "parquet-mr version 1.10.1-2147483647",
        "parquet-mr version 1.10.1-2147483648x",
        "parquet-mr version 1.10.1-2147483648)")
        @test st_facts(unsignedleaf, st_modern(st_le32(0x00000001),
            st_le32(0x00000002)); created_by=createdby).trust.state == :trusted
    end
    ieeeold = st_facts(st_leaf(STMD.Type.FLOAT),
        st_modern(st_f32(-1.0f0), st_f32(1.0f0)); order=st_ieee_order(),
        created_by="parquet-cpp version 1.2.9")
    @test ieeeold.trust.state == :untrusted
    @test ieeeold.trust.reason == :parquet_cpp_pre_1_3
    counted = st_facts(rawleaf, st_modern(UInt8[0x01], UInt8[0x02];
        nulls=Int64(1)); created_by="parquet-cpp")
    @test counted.lower.reason == :parquet_251
    @test counted.null_count.value == 1
    deprecated = st_facts(rawleaf, st_deprecated(UInt8[0x01], UInt8[0x02]);
        created_by="parquet-cpp version 1.2.9")
    @test deprecated.trust.reason == :parquet_cpp_pre_1_3
    @test deprecated.comparison == :undefined
    @test deprecated.lower.reason == :deprecated_order_mismatch

    for createdby in ("parquet-cpp version 1.2.9",
            "parquet-mr version 1.9.9")
        modern = st_facts(rawleaf, distinct; created_by=createdby)
        @test modern.comparison == :unsigned_bytes
        @test modern.lower.reason in (
            :parquet_cpp_pre_1_3, :parquet_mr_pre_1_10)
        deprecated = st_facts(rawleaf,
            st_deprecated(UInt8[0x01], UInt8[0x02]); created_by=createdby)
        @test deprecated.comparison == :undefined
        @test deprecated.lower.reason == :deprecated_order_mismatch
    end
    empty = st_modern(UInt8[0x01], UInt8[0x02]; nulls=Int64(4))
    for createdby in ("parquet-cpp version 1.2.9",
            "parquet-mr version 1.9.9"), order in (nothing, st_unknown_order())
        facts = st_facts(rawleaf, empty; created_by=createdby, order=order)
        @test facts.occupancy == :no_non_null
        @test facts.trust.state == :untrusted
        @test facts.lower.reason == (order === nothing ? :missing_order :
            :unknown_order)
    end
end
