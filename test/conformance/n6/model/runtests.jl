using SHA
using Test
using TOML

include("N6StatisticsModel.jl")
const Model = N6StatisticsModel

function le32(bits::UInt32)
    return UInt8[bits & 0xff, (bits >> 8) & 0xff, (bits >> 16) & 0xff,
        (bits >> 24) & 0xff]
end

function le16(bits::UInt16)
    return UInt8[bits & 0xff, (bits >> 8) & 0xff]
end

function be16(value::Int16)
    bits = reinterpret(UInt16, value)
    return UInt8[bits >> 8, bits & 0xff]
end

function bsonbinary(subtype::UInt8)
    return UInt8[0x0e, 0x00, 0x00, 0x00, 0x05, 0x62, 0x00,
        0x01, 0x00, 0x00, 0x00, subtype, 0x61, 0x00]
end

function bsonregex(options::AbstractString)
    optionbytes = Vector{UInt8}(codeunits(options))
    length = 11 + Base.length(optionbytes)
    return vcat(le32(UInt32(length)), UInt8[0x0b, 0x72, 0x00, 0x61, 0x00],
        optionbytes, UInt8[0x00, 0x00])
end

@testset "N6 independent statistics model" begin
    @testset "signed PLAIN bound decoding" begin
        leaf = Model.LeafSpec(Model.PHYSICAL_INT32)
        decoded = Model.decode_bound(UInt8[0xf9, 0xff, 0xff, 0xff], leaf,
            Model.ModelLimits())
        @test decoded.state == Model.BOUND_KNOWN
        @test decoded.value == Model.SignedValue(Int128(-7))
    end

    @testset "physical and logical bound decoding" begin
        @test Model.decode_bound(UInt8[0x01],
            Model.LeafSpec(Model.PHYSICAL_BOOLEAN), Model.ModelLimits()).value ==
            Model.BooleanValue(true)
        @test_throws Model.ModelFormatError Model.decode_bound(UInt8[0x01, 0x00],
            Model.LeafSpec(Model.PHYSICAL_BOOLEAN),
            Model.ModelLimits(max_statistics_value_bytes=0))

        unsigned = Model.LeafSpec(Model.PHYSICAL_INT32;
            logical=Model.LOGICAL_UNSIGNED_INTEGER, bit_width=32)
        @test Model.decode_bound(fill(0xff, 4), unsigned,
            Model.ModelLimits()).value == Model.UnsignedValue(typemax(UInt32))
        signed8 = Model.LeafSpec(Model.PHYSICAL_INT32;
            logical=Model.LOGICAL_SIGNED_INTEGER, bit_width=8)
        @test Model.decode_bound(UInt8[0x7f, 0x00, 0x00, 0x00], signed8,
            Model.ModelLimits()).state == Model.BOUND_KNOWN
        @test Model.decode_bound(UInt8[0x80, 0x00, 0x00, 0x00], signed8,
            Model.ModelLimits()).state == Model.BOUND_UNKNOWN

        uuid = Model.LeafSpec(Model.PHYSICAL_FIXED_LEN_BYTE_ARRAY;
            logical=Model.LOGICAL_UUID, type_length=16)
        @test_throws Model.ModelFormatError Model.decode_bound(fill(0x00, 15), uuid,
            Model.ModelLimits(max_statistics_value_bytes=1))
        millis = Model.LeafSpec(Model.PHYSICAL_INT32;
            logical=Model.LOGICAL_TIME, time_unit=Model.TIME_MILLIS)
        @test Model.decode_bound(UInt8[0xff, 0x5b, 0x26, 0x05], millis,
            Model.ModelLimits()).state == Model.BOUND_KNOWN
        @test Model.decode_bound(UInt8[0x00, 0x5c, 0x26, 0x05], millis,
            Model.ModelLimits()).state == Model.BOUND_UNKNOWN

        stringleaf = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY;
            logical=Model.LOGICAL_STRING)
        over = Model.decode_bound(UInt8[0xff, 0xff], stringleaf,
            Model.ModelLimits(max_statistics_value_bytes=1))
        @test (over.state, over.reason, over.semantic_checked) ==
            (Model.BOUND_UNKNOWN, :over_limit, false)
        invalid = Model.decode_bound(UInt8[0xff], stringleaf,
            Model.ModelLimits(max_statistics_value_bytes=1))
        @test (invalid.state, invalid.reason, invalid.semantic_checked) ==
            (Model.BOUND_UNKNOWN, :invalid_logical, true)

        jsonleaf = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY;
            logical=Model.LOGICAL_JSON)
        validjson = Vector{UInt8}(codeunits("{\"a\":[1,true,null]}"))
        @test Model.decode_bound(validjson, jsonleaf,
            Model.ModelLimits()).state == Model.BOUND_KNOWN
        @test Model.decode_bound(Vector{UInt8}(codeunits("{\"a\":}")), jsonleaf,
            Model.ModelLimits()).state == Model.BOUND_UNKNOWN

        bsonleaf = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY;
            logical=Model.LOGICAL_BSON)
        validbson = UInt8[0x0c, 0x00, 0x00, 0x00, 0x10, 0x78, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x00]
        @test Model.decode_bound(validbson, bsonleaf,
            Model.ModelLimits()).state == Model.BOUND_KNOWN
        invalidbson = copy(validbson)
        invalidbson[1] = 0x0d
        @test Model.decode_bound(invalidbson, bsonleaf,
            Model.ModelLimits()).state == Model.BOUND_UNKNOWN
        validarray = UInt8[0x11, 0x00, 0x00, 0x00, 0x04, 0x61, 0x00,
            0x09, 0x00, 0x00, 0x00, 0x08, 0x30, 0x00, 0x01, 0x00, 0x00]
        @test Model.decode_bound(validarray, bsonleaf,
            Model.ModelLimits()).state == Model.BOUND_KNOWN
        invalidarray = copy(validarray)
        invalidarray[14] = 0x78
        @test Model.decode_bound(invalidarray, bsonleaf,
            Model.ModelLimits()).state == Model.BOUND_UNKNOWN
        validoldbinary = UInt8[0x14, 0x00, 0x00, 0x00, 0x05, 0x62, 0x00,
            0x07, 0x00, 0x00, 0x00, 0x02, 0x03, 0x00, 0x00, 0x00,
            0x61, 0x62, 0x63, 0x00]
        @test Model.decode_bound(validoldbinary, bsonleaf,
            Model.ModelLimits()).state == Model.BOUND_KNOWN
        invalidoldbinary = copy(validoldbinary)
        invalidoldbinary[13] = 0x04
        @test Model.decode_bound(invalidoldbinary, bsonleaf,
            Model.ModelLimits()).state == Model.BOUND_UNKNOWN
        for subtype in UInt8[0x00, 0x09, 0x80]
            @test Model.decode_bound(bsonbinary(subtype), bsonleaf,
                Model.ModelLimits()).state == Model.BOUND_KNOWN
        end
        for subtype in UInt8[0x0a, 0x7f]
            @test Model.decode_bound(bsonbinary(subtype), bsonleaf,
                Model.ModelLimits()).state == Model.BOUND_UNKNOWN
        end
        @test Model.decode_bound(bsonregex("imsux"), bsonleaf,
            Model.ModelLimits()).state == Model.BOUND_KNOWN
        for options in ("mi", "ii", "z")
            @test Model.decode_bound(bsonregex(options), bsonleaf,
                Model.ModelLimits()).state == Model.BOUND_UNKNOWN
        end
    end

    @testset "decimal and byte comparisons" begin
        decimal = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY;
            logical=Model.LOGICAL_DECIMAL, precision=3)
        minus129 = Model.decode_bound(UInt8[0xff, 0x7f], decimal,
            Model.ModelLimits()).value
        minus1 = Model.decode_bound(UInt8[0xff], decimal,
            Model.ModelLimits()).value
        plus1wide = Model.decode_bound(UInt8[0x00, 0x01], decimal,
            Model.ModelLimits()).value
        plus1 = Model.decode_bound(UInt8[0x01], decimal,
            Model.ModelLimits()).value
        @test Model.compare_values(minus129, minus1, Model.COMPARATOR_DECIMAL) == -1
        @test Model.compare_values(plus1wide, plus1, Model.COMPARATOR_DECIMAL) == 0

        precision2 = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY;
            logical=Model.LOGICAL_DECIMAL, precision=2)
        @test Model.decode_bound(UInt8[0x63], precision2,
            Model.ModelLimits()).state == Model.BOUND_KNOWN
        @test Model.decode_bound(UInt8[0x64], precision2,
            Model.ModelLimits()).state == Model.BOUND_UNKNOWN

        rawleaf = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY)
        left = Model.decode_bound(UInt8[0x61, 0x00], rawleaf,
            Model.ModelLimits()).value
        right = Model.decode_bound(UInt8[0x61, 0x01], rawleaf,
            Model.ModelLimits()).value
        @test Model.compare_values(left, right, Model.COMPARATOR_UNSIGNED_BYTES) == -1
        @test Model.decode_bound(UInt8[0xff], rawleaf,
            Model.ModelLimits()).state == Model.BOUND_KNOWN

        decimal16 = Model.LeafSpec(Model.PHYSICAL_FIXED_LEN_BYTE_ARRAY;
            logical=Model.LOGICAL_DECIMAL, type_length=2, precision=5)
        limits = Model.ModelLimits()
        @test all(begin
            leftvalue = Model.decode_bound(be16(Int16(value)), decimal16,
                limits).value
            rightvalue = Model.decode_bound(be16(Int16(value + 1)), decimal16,
                limits).value
            Model.compare_values(leftvalue, rightvalue,
                Model.COMPARATOR_DECIMAL) == -1
        end for value in Int32(typemin(Int16)):(Int32(typemax(Int16)) - 1))
    end

    @testset "raw-bit IEEE total order" begin
        patterns = UInt16[0xfe01, 0xfc01, 0xfc00, 0x8000, 0x0000, 0x7c00,
            0x7c01, 0x7e01]
        values = [Model.FloatValue(UInt8(16), UInt64(bits)) for bits in patterns]
        @test all(Model.compare_values(values[index], values[index + 1],
            Model.COMPARATOR_IEEE_FLOAT) == -1 for index in 1:(length(values) - 1))
        @test Model.compare_values(values[4], values[5],
            Model.COMPARATOR_TYPE_FLOAT) == 0
        @test length(unique(Model.ieee_total_key(value) for value in values)) ==
            length(values)
        samples = (
            (UInt8(32), UInt64[0xffffffff, 0xff800000, 0x80000001,
                0x80000000, 0x00000000, 0x00000001, 0x7f800000,
                0x7fffffff]),
            (UInt8(64), UInt64[0xffffffffffffffff, 0xfff0000000000000,
                0x8000000000000001, 0x8000000000000000,
                0x0000000000000000, 0x0000000000000001,
                0x7ff0000000000000, 0x7fffffffffffffff]),
        )
        for (width, bits) in samples
            ordered = [Model.FloatValue(width, value) for value in bits]
            @test all(Model.compare_values(ordered[index], ordered[index + 1],
                Model.COMPARATOR_IEEE_FLOAT) == -1 for
                index in 1:(length(ordered) - 1))
            @test Model.float_isnan(first(ordered))
            @test Model.float_isnan(last(ordered))
        end
    end

    @testset "statistics value limit" begin
        rawleaf = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY)
        exact = Model.decode_bound(UInt8[1, 2, 3, 4], rawleaf,
            Model.ModelLimits(max_statistics_value_bytes=4))
        @test exact.state == Model.BOUND_KNOWN
        @test Model.decode_bound(UInt8[1, 2, 3, 4, 5], rawleaf,
            Model.ModelLimits(max_statistics_value_bytes=4)).state ==
            Model.BOUND_UNKNOWN
        @test Model.writer_bounds_allowed(UInt8[1, 2, 3, 4], UInt8[5],
            Model.ModelLimits(max_statistics_value_bytes=4))
        @test !Model.writer_bounds_allowed(UInt8[1, 2, 3, 4, 5], UInt8[5],
            Model.ModelLimits(max_statistics_value_bytes=4))
        zerolimits = Model.ModelLimits(max_statistics_value_bytes=0)
        @test Model.decode_bound(UInt8[], rawleaf, zerolimits).state ==
            Model.BOUND_KNOWN
        overzero = Model.decode_bound(UInt8[0xff], rawleaf, zerolimits)
        @test (overzero.state, overzero.reason, overzero.semantic_checked) ==
            (Model.BOUND_UNKNOWN, :over_limit, false)
        @test Model.writer_bounds_allowed(UInt8[], UInt8[], zerolimits)
        @test !Model.writer_bounds_allowed(UInt8[0x00], UInt8[], zerolimits)
        @test_throws ArgumentError Model.writer_bounds_allowed(UInt8[], UInt8[],
            Model.ModelLimits(max_statistics_value_bytes=-1))
        @test_throws ArgumentError Model.producer_decision(
            "parquet-cpp version 1.2.9", rawleaf,
            Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN,
            UInt8[], UInt8[], Model.ModelLimits(max_statistics_value_bytes=-1))
        for createdby in ("parquet-cpp version 1.2.9",
                "parquet-mr version 1.9.9"), equal in (false, true)
            lower = fill(UInt8(0x61), 5)
            upper = equal ? copy(lower) : fill(UInt8(0x62), 5)
            result = Model.interpret_statistics(rawleaf, Int64(1),
                Model.RawStatistics(modern_lower=lower, modern_upper=upper),
                Model.DeclaredOrder[Model.ORDER_TYPE]; created_by=createdby,
                limits=Model.ModelLimits(max_statistics_value_bytes=4))
            @test result.trust.state == Model.TRUST_UNTRUSTED
            @test result.trust.reason == :legacy_wrong_order
            @test result.lower.reason == :over_limit
            @test result.upper.reason == :over_limit
        end
        malformed = Model.RawStatistics(modern_lower=UInt8[0x00],
            null_count=Int64(-1))
        @test_throws ArgumentError Model.interpret_statistics(rawleaf, Int64(-1),
            malformed, Model.DeclaredOrder[];
            limits=Model.ModelLimits(max_statistics_value_bytes=-1))
    end

    @testset "count state machine" begin
        floatleaf = Model.LeafSpec(Model.PHYSICAL_FLOAT)
        stats = Model.RawStatistics(null_count=Int64(2), nan_count=Int64(3),
            distinct_count=Int64(4))
        result = Model.interpret_statistics(floatleaf, Int64(10), stats,
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test result.occupancy == Model.OCCUPANCY_HAS_NON_NAN
        @test (result.null_count.known, result.null_count.value) == (true, 2)
        @test (result.nan_count.known, result.nan_count.value) == (true, 3)
        @test (result.distinct_count.known, result.distinct_count.value) == (true, 4)

        @test_throws Model.ModelFormatError Model.interpret_statistics(floatleaf,
            Int64(4), Model.RawStatistics(null_count=Int64(2), nan_count=Int64(3)),
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test_throws Model.ModelFormatError Model.interpret_statistics(
            Model.LeafSpec(Model.PHYSICAL_INT32), Int64(4),
            Model.RawStatistics(nan_count=Int64(0)),
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test_throws Model.ModelFormatError Model.interpret_statistics(floatleaf,
            Int64(4), Model.RawStatistics(null_count=Int64(2),
                distinct_count=Int64(3)), Model.DeclaredOrder[Model.ORDER_TYPE])
        @test_throws Model.ModelFormatError Model.interpret_statistics(floatleaf,
            Int64(4), Model.RawStatistics(null_count=Int64(-1)),
            Model.DeclaredOrder[Model.ORDER_TYPE])

        zero = Model.interpret_statistics(floatleaf, Int64(0),
            Model.RawStatistics(null_count=Int64(0), nan_count=Int64(0),
                distinct_count=Int64(0)),
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test zero.occupancy == Model.OCCUPANCY_EMPTY
        @test all(fact -> fact.known && iszero(fact.value),
            (zero.null_count, zero.nan_count, zero.distinct_count))
        missing = Model.interpret_statistics(floatleaf, Int64(0),
            Model.RawStatistics(), Model.DeclaredOrder[Model.ORDER_TYPE])
        @test all(fact -> !fact.known,
            (missing.null_count, missing.nan_count, missing.distinct_count))
        @test missing.occupancy == Model.OCCUPANCY_EMPTY

        intleaf = Model.LeafSpec(Model.PHYSICAL_INT32)
        emptyfamilies = (
            Model.RawStatistics(modern_lower=le32(UInt32(1)),
                modern_upper=le32(UInt32(2))),
            Model.RawStatistics(deprecated_lower=le32(UInt32(1)),
                deprecated_upper=le32(UInt32(2))),
        )
        for emptystats in emptyfamilies
            emptyresult = Model.interpret_statistics(intleaf, Int64(0),
                emptystats, Model.DeclaredOrder[Model.ORDER_TYPE])
            @test emptyresult.occupancy == Model.OCCUPANCY_EMPTY
            @test (emptyresult.lower.state, emptyresult.lower.reason) ==
                (Model.BOUND_UNKNOWN, :no_non_null_values)
            @test (emptyresult.upper.state, emptyresult.upper.reason) ==
                (Model.BOUND_UNKNOWN, :no_non_null_values)
        end
        largest = Model.interpret_statistics(floatleaf, typemax(Int64),
            Model.RawStatistics(null_count=typemax(Int64), nan_count=Int64(0)),
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test largest.occupancy == Model.OCCUPANCY_EMPTY
    end

    @testset "atomic bound-family selection" begin
        intleaf = Model.LeafSpec(Model.PHYSICAL_INT32)
        modernlower = le32(UInt32(1))
        deprecatedupper = le32(UInt32(9))
        stats = Model.RawStatistics(modern_lower=modernlower,
            deprecated_upper=deprecatedupper)
        result = Model.interpret_statistics(intleaf, Int64(1), stats,
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test result.family == Model.FAMILY_MODERN
        @test result.lower.state == Model.BOUND_KNOWN
        @test result.upper.state == Model.BOUND_ABSENT

        deprecated = Model.RawStatistics(deprecated_lower=le32(UInt32(1)),
            deprecated_upper=le32(UInt32(9)), lower_exact=true,
            upper_exact=true)
        result = Model.interpret_statistics(intleaf, Int64(1), deprecated, nothing)
        @test result.family == Model.FAMILY_DEPRECATED
        @test result.lower.state == Model.BOUND_KNOWN
        @test result.upper.state == Model.BOUND_KNOWN
        @test result.lower.exactness == Model.EXACTNESS_UNKNOWN
        @test result.upper.exactness == Model.EXACTNESS_UNKNOWN

        unsigned = Model.LeafSpec(Model.PHYSICAL_INT32;
            logical=Model.LOGICAL_UNSIGNED_INTEGER, bit_width=32)
        result = Model.interpret_statistics(unsigned, Int64(1), deprecated, nothing)
        @test result.lower.state == Model.BOUND_UNKNOWN
        @test result.upper.state == Model.BOUND_UNKNOWN

        @test_throws Model.ModelFormatError Model.interpret_statistics(intleaf,
            Int64(1), stats, Model.DeclaredOrder[])
        @test_throws Model.ModelFormatError Model.interpret_statistics(intleaf,
            Int64(1), stats,
            Model.DeclaredOrder[Model.ORDER_TYPE, Model.ORDER_TYPE])
        missingorder = Model.interpret_statistics(intleaf, Int64(1), stats, nothing)
        @test missingorder.lower.reason == :missing_column_orders
        future = Model.interpret_statistics(intleaf, Int64(1), stats,
            Model.DeclaredOrder[Model.ORDER_FUTURE])
        @test future.lower.reason == :unknown_column_order
        @test_throws Model.ModelFormatError Model.interpret_statistics(intleaf,
            Int64(1), stats, Model.DeclaredOrder[Model.ORDER_IEEE])

        contradictory = Model.RawStatistics(modern_lower=le32(UInt32(9)),
            modern_upper=le32(UInt32(1)))
        result = Model.interpret_statistics(intleaf, Int64(1), contradictory,
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test result.lower.reason == :contradictory_bounds
        @test result.upper.reason == :contradictory_bounds

        int96 = Model.LeafSpec(Model.PHYSICAL_INT96)
        undefined = Model.interpret_statistics(int96, Int64(1),
            Model.RawStatistics(modern_lower=fill(0x00, 12)),
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test undefined.comparator == Model.COMPARATOR_UNDEFINED
        @test undefined.lower.reason == :undefined_type_order

        binary = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY)
        emptystats = Model.RawStatistics(modern_lower=UInt8[0x61],
            modern_upper=UInt8[0x62], null_count=Int64(4))
        for createdby in ("parquet-cpp version 1.2.9",
                "parquet-mr version 1.9.9"), modelorders in (
                nothing, Model.DeclaredOrder[Model.ORDER_FUTURE])
            result = Model.interpret_statistics(binary, Int64(4), emptystats,
                modelorders; created_by=createdby)
            expected = modelorders === nothing ? :missing_column_orders :
                :unknown_column_order
            @test result.occupancy == Model.OCCUPANCY_EMPTY
            @test result.trust.state == Model.TRUST_UNTRUSTED
            @test result.trust.reason == :legacy_wrong_order
            @test result.lower.reason == expected
            @test result.upper.reason == expected
        end

        limited = Model.interpret_statistics(
            Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY), Int64(1),
            Model.RawStatistics(modern_lower=UInt8[0x61, 0x62],
                modern_upper=UInt8[0x7a], lower_exact=true,
                upper_exact=false), Model.DeclaredOrder[Model.ORDER_TYPE];
            created_by="impala version 1.0.0",
            limits=Model.ModelLimits(max_statistics_value_bytes=1))
        @test (limited.lower.state, limited.lower.reason,
            limited.lower.exactness) ==
            (Model.BOUND_UNKNOWN, :over_limit, Model.EXACTNESS_EXACT)
        @test (limited.upper.state, limited.upper.exactness) ==
            (Model.BOUND_KNOWN, Model.EXACTNESS_INEXACT)
    end

    @testset "TYPE_ORDER floating compatibility" begin
        floatleaf = Model.LeafSpec(Model.PHYSICAL_FLOAT)
        zeros = Model.RawStatistics(modern_lower=le32(UInt32(0x00000000)),
            modern_upper=le32(UInt32(0x80000000)),
            lower_exact=true, upper_exact=true, null_count=Int64(0),
            nan_count=Int64(0))
        result = Model.interpret_statistics(floatleaf, Int64(2), zeros,
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test result.lower.value == Model.FloatValue(UInt8(32), UInt64(0x80000000))
        @test result.upper.value == Model.FloatValue(UInt8(32), UInt64(0x00000000))
        @test result.lower.exactness == Model.EXACTNESS_INEXACT
        @test result.upper.exactness == Model.EXACTNESS_INEXACT

        nan = le32(UInt32(0x7fc00001))
        finite = le32(UInt32(0x3f800000))
        partial = Model.RawStatistics(modern_lower=nan, modern_upper=finite)
        result = Model.interpret_statistics(floatleaf, Int64(2), partial,
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test result.lower.reason == :nan_type_order
        @test result.upper.state == Model.BOUND_KNOWN

        allnan = Model.RawStatistics(modern_lower=nan, modern_upper=nan,
            null_count=Int64(0), nan_count=Int64(2))
        result = Model.interpret_statistics(floatleaf, Int64(2), allnan,
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test result.lower.reason == :all_nan_type_order
        @test result.upper.reason == :all_nan_type_order

        allnull = Model.RawStatistics(modern_lower=finite, modern_upper=finite,
            null_count=Int64(2), nan_count=Int64(0))
        result = Model.interpret_statistics(floatleaf, Int64(2), allnull,
            Model.DeclaredOrder[Model.ORDER_TYPE])
        @test result.lower.reason == :no_non_null_values
        @test result.upper.reason == :no_non_null_values
    end

    @testset "IEEE count-bound states" begin
        floatleaf = Model.LeafSpec(Model.PHYSICAL_FLOAT)
        signaling = le32(UInt32(0x7f800001))
        quiet = le32(UInt32(0x7fc00001))
        finite = le32(UInt32(0x3f800000))
        allnan = Model.RawStatistics(modern_lower=signaling,
            modern_upper=quiet, null_count=Int64(0), nan_count=Int64(2))
        result = Model.interpret_statistics(floatleaf, Int64(2), allnan,
            Model.DeclaredOrder[Model.ORDER_IEEE])
        @test result.lower.state == Model.BOUND_KNOWN
        @test result.upper.state == Model.BOUND_KNOWN
        @test result.comparator == Model.COMPARATOR_IEEE_FLOAT

        badallnan = Model.RawStatistics(modern_lower=finite,
            modern_upper=quiet, null_count=Int64(0), nan_count=Int64(2))
        result = Model.interpret_statistics(floatleaf, Int64(2), badallnan,
            Model.DeclaredOrder[Model.ORDER_IEEE])
        @test result.lower.reason == :ieee_bound_kind_contradiction
        @test result.upper.reason == :ieee_bound_kind_contradiction

        mixed = Model.RawStatistics(modern_lower=finite,
            modern_upper=le32(UInt32(0x40000000)), null_count=Int64(0),
            nan_count=Int64(1))
        result = Model.interpret_statistics(floatleaf, Int64(3), mixed,
            Model.DeclaredOrder[Model.ORDER_IEEE])
        @test result.lower.state == Model.BOUND_KNOWN
        @test result.upper.state == Model.BOUND_KNOWN

        badmixed = Model.RawStatistics(modern_lower=finite, modern_upper=quiet,
            null_count=Int64(0), nan_count=Int64(1))
        result = Model.interpret_statistics(floatleaf, Int64(3), badmixed,
            Model.DeclaredOrder[Model.ORDER_IEEE])
        @test result.lower.reason == :ieee_bound_kind_contradiction
        @test result.upper.reason == :ieee_bound_kind_contradiction

        unknown = Model.RawStatistics(modern_lower=quiet, modern_upper=finite)
        result = Model.interpret_statistics(floatleaf, Int64(3), unknown,
            Model.DeclaredOrder[Model.ORDER_IEEE])
        @test result.lower.reason == :unproven_ieee_nan
        @test result.upper.state == Model.BOUND_KNOWN

        onefinite = Model.RawStatistics(modern_lower=finite,
            null_count=Int64(0), nan_count=Int64(2))
        result = Model.interpret_statistics(floatleaf, Int64(2), onefinite,
            Model.DeclaredOrder[Model.ORDER_IEEE])
        @test result.lower.reason == :ieee_bound_kind_contradiction
        @test result.upper.state == Model.BOUND_ABSENT
    end

    @testset "producer-version policy" begin
        binary = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY)
        lower = UInt8[0x61]
        upper = UInt8[0x62]
        for application in ("parquet-cpp", "parquet-mr", "impala")
            @test !Model.parse_created_by(application).parsed
        end
        for application in ("parquet-cpp", "parquet-mr")
            arrow = Model.parse_arrow_created_by(application)
            @test arrow.parsed
            @test arrow.application == application
            @test arrow.version === nothing
        end
        @test !Model.parse_arrow_created_by("impala").parsed
        @test Model.parse_created_by(
            "parquet-mr version 1.8.0").parsed
        backtracked = Model.parse_created_by(
            "foo version (bad) bar version 1.0.0")
        @test backtracked.parsed
        @test backtracked.application == "foo version (bad) bar"
        for created_by in ("parquet-mr version (bad) x version 1.7.9",
                "parquet-cpp version (bad) x version 1.2.9")
            producer = Model.parse_created_by(created_by)
            @test producer.parsed
            @test producer.application in (
                "parquet-mr version (bad) x", "parquet-cpp version (bad) x")
            @test Model.producer_decision(created_by, binary,
                Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN,
                lower, upper).state == Model.TRUST_TRUSTED
        end
        p251affected = Union{Nothing,String}[nothing, "", " version 1.0.0",
            "parquet-mr", "parquet-mr version 1", "parquet-mr version 1.2",
            "parquet-mr version 1.7.9",
            "parquet-mr version 1.8.0-rc1",
            "parquet-mr version 1.5.0-cdh5.4.9",
            "parquet-mr version 1.5.0-.",
            "parquet-mr version 1.5.0-..",
            "parquet-mr version 1.5.0-cdh5.5.",
            "parquet-mr version 1.5.0-cdh5.5.."]
        for created_by in p251affected
            decision = Model.producer_decision(created_by, binary,
                Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN, lower, upper)
            @test decision.state == Model.TRUST_UNTRUSTED
            @test decision.reason == :parquet_251
        end
        for created_by in ("impala", "garbage!")
            decision = Model.producer_decision(created_by, binary,
                Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN,
                lower, upper)
            @test decision.state == Model.TRUST_UNTRUSTED
            @test decision.reason == :parquet_251
        end
        @test Model.producer_decision("parquet-cpp", binary,
            Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN,
            lower, lower).reason == :parquet_251
        for created_by in ("parquet-mr version 1.8.0",
                "parquet-mr version 1.5.0-cdh5.5.0")
            decision = Model.producer_decision(created_by, binary,
                Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN, lower, upper)
            @test decision.state == Model.TRUST_UNTRUSTED
            @test decision.reason == :legacy_wrong_order
        end
        for (created_by, expected) in (
                ("parquet-mr version 1.5.0-cdh5.5.0", Model.TRUST_TRUSTED),
                ("parquet-mr version 1.5.0-cdh5.5.0.", Model.TRUST_TRUSTED),
                ("parquet-mr version 1.5.0-cdh5.5.0..", Model.TRUST_TRUSTED),
                ("parquet-mr version 1.5.0-cdh5.5.0+build.7",
                    Model.TRUST_TRUSTED),
                ("parquet-mr\tversion\t1.8.0", Model.TRUST_TRUSTED),
                ("parquet-mr version 1.5.0-cdh5.4.9", Model.TRUST_UNTRUSTED),
                ("parquet-mr version 1.5.0", Model.TRUST_UNTRUSTED),
                ("parquet-mr version 1.5.0zz-cdh5.5.0",
                    Model.TRUST_UNTRUSTED),
                ("parquet-mr version 1.5.0-cdh5.2147483648x.0",
                    Model.TRUST_TRUSTED),
                ("parquet-mr version 1.5.0-cdh5.-1.0",
                    Model.TRUST_TRUSTED),
                ("parquet-mr version 1.5.0-cdh5. 5.0",
                    Model.TRUST_TRUSTED),
                ("parquet-mr version 1.5.0-cdh5.\u0665.0",
                    Model.TRUST_TRUSTED),
                ("parquet-mr version 2147483648.0.0",
                    Model.TRUST_UNTRUSTED))
            decision = Model.producer_decision(created_by, binary,
                Model.COMPARATOR_SIGNED, Model.FAMILY_MODERN, lower, upper)
            @test decision.state == expected
        end
        @test Model.producer_decision("impala version 1.0.0", binary,
            Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN, lower,
            upper).state == Model.TRUST_TRUSTED
        for created_by in ("parquet-cpp version 1.3.1-2147483648x",
                "parquet-cpp version 1.3.1-2147483648)",
                "parquet-mr version 1.10.1-2147483648x",
                "parquet-mr version 1.10.1-2147483648)")
            @test Model.producer_decision(created_by, binary,
                Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN,
                lower, upper).state == Model.TRUST_TRUSTED
        end
        for terminator in ("\n", "\r", "\r\n", "\u0085", "\u2028", "\u2029")
            for (application, version, reason) in (
                    ("parquet-mr", "1.10.0", :parquet_251),
                    ("parquet-cpp", "1.3.0", :legacy_wrong_order))
                created_by = application * " version " * version * "+foo" *
                    terminator * "bar"
                @test Model.parse_created_by(created_by).version === nothing
                @test Model.producer_decision(created_by, binary,
                    Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN,
                    lower, upper).reason == reason
            end
        end
        for separator in ("\v", "\f")
            for created_by in ("parquet-mr version 1.10.0+foo" * separator * "bar",
                    "parquet-cpp version 1.3.0+foo" * separator * "bar")
                @test Model.parse_created_by(created_by).version !== nothing
                @test Model.producer_decision(created_by, binary,
                    Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN,
                    lower, upper).state == Model.TRUST_TRUSTED
            end
        end
        equal = UInt8[0x61]
        @test Model.producer_decision("parquet-mr version 1.7.9", binary,
            Model.COMPARATOR_UNSIGNED_BYTES, Model.FAMILY_MODERN, equal,
            equal).reason == :parquet_251

        for terminator in ("\n", "\r", "\r\n", "\u0085", "\u2028", "\u2029")
            created_by = "foo" * terminator * "bar version 1.0.0"
            @test !Model.parse_created_by(created_by).parsed
            decision = Model.producer_decision(created_by, binary,
                Model.COMPARATOR_SIGNED, Model.FAMILY_MODERN, lower, upper)
            @test decision.state == Model.TRUST_UNTRUSTED
            @test decision.reason == :parquet_251
        end
        for separator in ("\n", "\r", "\r\n", "\v", "\f")
            created_by = "parquet-mr" * separator * "version 1.8.0"
            parsed = Model.parse_created_by(created_by)
            @test parsed.parsed
            @test parsed.application == "parquet-mr"
            decision = Model.producer_decision(created_by, binary,
                Model.COMPARATOR_SIGNED, Model.FAMILY_MODERN, lower, upper)
            @test decision.state == Model.TRUST_TRUSTED
            @test decision.reason == :trusted
        end
        for separator in ("\u0085", "\u2028", "\u2029")
            created_by = "parquet-mr" * separator * "version 1.8.0"
            @test !Model.parse_created_by(created_by).parsed
            decision = Model.producer_decision(created_by, binary,
                Model.COMPARATOR_SIGNED, Model.FAMILY_MODERN, lower, upper)
            @test decision.state == Model.TRUST_UNTRUSTED
            @test decision.reason == :parquet_251
        end

        oldcases = [
            ("parquet-cpp", true),
            ("parquet-cpp version 1", true),
            ("parquet-cpp version 1.2", true),
            ("parquet-cpp version 1.2.9", true),
            ("parquet-cpp version 1.3", true),
            ("parquet-cpp version", true),
            ("parquet-cpp    version1.2.9", true),
            ("parquet-cpp version 1.3.0-rc1", true),
            ("parquet-cpp version 1.3.0", false),
            ("parquet-mr", true),
            ("parquet-mr version 1", true),
            ("parquet-mr version 1.2", true),
            ("parquet-mr version 1.9.9", true),
            ("parquet-mr version 1.10", true),
            ("parquet-mr version 1.10.0-rc1", true),
            ("parquet-mr version 1.10.0", false),
        ]
        intleaf = Model.LeafSpec(Model.PHYSICAL_INT32;
            logical=Model.LOGICAL_UNSIGNED_INTEGER, bit_width=32)
        for (created_by, affected) in oldcases
            decision = Model.producer_decision(created_by, intleaf,
                Model.COMPARATOR_UNSIGNED, Model.FAMILY_MODERN, le32(UInt32(1)),
                le32(UInt32(2)))
            @test (decision.state == Model.TRUST_UNTRUSTED) == affected
        end
        @test Model.producer_decision("parquet-cpp version 1.2.9", intleaf,
            Model.COMPARATOR_UNSIGNED, Model.FAMILY_DEPRECATED, equal,
            equal).state == Model.TRUST_TRUSTED
        floatleaf = Model.LeafSpec(Model.PHYSICAL_FLOAT)
        @test Model.producer_decision("parquet-mr version 1.9.9", floatleaf,
            Model.COMPARATOR_IEEE_FLOAT, Model.FAMILY_MODERN,
            le32(UInt32(0)), le32(UInt32(1))).state == Model.TRUST_UNTRUSTED
        @test Model.producer_decision("parquet-mr version 1.9.9", floatleaf,
            Model.COMPARATOR_TYPE_FLOAT, Model.FAMILY_MODERN,
            le32(UInt32(0)), le32(UInt32(1))).state == Model.TRUST_TRUSTED

        rejected = Model.interpret_statistics(binary, Int64(3),
            Model.RawStatistics(modern_lower=lower, modern_upper=upper,
                null_count=Int64(1), distinct_count=Int64(2)),
            Model.DeclaredOrder[Model.ORDER_TYPE];
            created_by="parquet-mr version 1.7.9")
        @test rejected.lower.reason == :parquet_251
        @test rejected.upper.reason == :parquet_251
        @test (rejected.null_count.known, rejected.null_count.value) == (true, 1)
        @test (rejected.distinct_count.known, rejected.distinct_count.value) ==
            (true, 2)
    end

    @testset "exhaustive Float16 total order" begin
        patterns = collect(UInt16(0):typemax(UInt16))
        sort!(patterns; lt=(left, right) -> Model.compare_values(
            Model.FloatValue(UInt8(16), UInt64(left)),
            Model.FloatValue(UInt8(16), UInt64(right)),
            Model.COMPARATOR_IEEE_FLOAT) < 0)
        @test length(patterns) == 65_536
        @test first(patterns) == UInt16(0xffff)
        @test last(patterns) == UInt16(0x7fff)
        @test all(Model.compare_values(
            Model.FloatValue(UInt8(16), UInt64(patterns[index])),
            Model.FloatValue(UInt8(16), UInt64(patterns[index + 1])),
            Model.COMPARATOR_IEEE_FLOAT) == -1 for index in 1:65_535)
        keys = [Model.ieee_total_key(Model.FloatValue(UInt8(16), UInt64(bits)))
            for bits in patterns]
        @test length(unique(keys)) == 65_536
        @test count(bits -> Model.float_isnan(
            Model.FloatValue(UInt8(16), UInt64(bits))), patterns) == 2_046
        orderedbytes = Vector{UInt8}(undef, 2 * length(patterns))
        for (index, bits) in enumerate(patterns)
            orderedbytes[2 * index - 1] = UInt8(bits & 0xff)
            orderedbytes[2 * index] = UInt8(bits >> 8)
        end
        @test bytes2hex(sha256(orderedbytes)) ==
            "61619b1a4260ee4cff6d462d21cab5a049198a27b3a9ae3e8ecfe246efc1d4b6"
    end

    @testset "independent extrema summary" begin
        float16 = Model.LeafSpec(Model.PHYSICAL_FIXED_LEN_BYTE_ARRAY;
            logical=Model.LOGICAL_FLOAT16, type_length=2)
        mixed = Vector{UInt8}[
            le16(UInt16(0x7e01)),
            le16(UInt16(0xbc00)),
            le16(UInt16(0x4000)),
            le16(UInt16(0x7c01)),
        ]
        summary = Model.summarize_raw_values(float16, mixed,
            Model.COMPARATOR_IEEE_FLOAT)
        @test summary.nan_count == 2
        @test summary.lower == Model.FloatValue(UInt8(16), UInt64(0xbc00))
        @test summary.upper == Model.FloatValue(UInt8(16), UInt64(0x4000))
        @test Model.contains_value(summary,
            Model.FloatValue(UInt8(16), UInt64(0x3c00)),
            Model.COMPARATOR_IEEE_FLOAT)

        allnan = Vector{UInt8}[
            le16(UInt16(0x7e02)),
            le16(UInt16(0x7c01)),
            le16(UInt16(0xfe03)),
        ]
        summary = Model.summarize_raw_values(float16, allnan,
            Model.COMPARATOR_IEEE_FLOAT)
        @test summary.nan_count == 3
        @test summary.lower == Model.FloatValue(UInt8(16), UInt64(0xfe03))
        @test summary.upper == Model.FloatValue(UInt8(16), UInt64(0x7e02))
        @test all(value -> Model.contains_value(summary, value,
            Model.COMPARATOR_IEEE_FLOAT), (
                Model.FloatValue(UInt8(16), UInt64(0xfe03)),
                Model.FloatValue(UInt8(16), UInt64(0x7c01)),
                Model.FloatValue(UInt8(16), UInt64(0x7e02))))
        @test !Model.contains_value(summary,
            Model.FloatValue(UInt8(16), UInt64(0xffff)),
            Model.COMPARATOR_IEEE_FLOAT)
        @test !Model.contains_value(Model.summarize_raw_values(float16, mixed,
            Model.COMPARATOR_IEEE_FLOAT),
            Model.FloatValue(UInt8(16), UInt64(0x7e01)),
            Model.COMPARATOR_IEEE_FLOAT)

        typesummary = Model.summarize_raw_values(float16, mixed,
            Model.COMPARATOR_TYPE_FLOAT)
        @test typesummary.nan_count == 2
        @test typesummary.lower == Model.FloatValue(UInt8(16), UInt64(0xbc00))
        @test typesummary.upper == Model.FloatValue(UInt8(16), UInt64(0x4000))
        typeallnan = Model.summarize_raw_values(float16, allnan,
            Model.COMPARATOR_TYPE_FLOAT)
        @test typeallnan.lower === nothing
        @test typeallnan.upper === nothing

        pluszero = le16(UInt16(0x0000))
        minuszero = le16(UInt16(0x8000))
        plussummary = Model.summarize_raw_values(float16,
            Vector{UInt8}[pluszero], Model.COMPARATOR_IEEE_FLOAT)
        @test plussummary.lower == plussummary.upper ==
            Model.FloatValue(UInt8(16), UInt64(0x0000))
        minussummary = Model.summarize_raw_values(float16,
            Vector{UInt8}[minuszero], Model.COMPARATOR_IEEE_FLOAT)
        @test minussummary.lower == minussummary.upper ==
            Model.FloatValue(UInt8(16), UInt64(0x8000))
        bothsummary = Model.summarize_raw_values(float16,
            Vector{UInt8}[pluszero, minuszero], Model.COMPARATOR_IEEE_FLOAT)
        @test bothsummary.lower == Model.FloatValue(UInt8(16), UInt64(0x8000))
        @test bothsummary.upper == Model.FloatValue(UInt8(16), UInt64(0x0000))

        rawvalues = Vector{UInt8}[UInt8[0x61], UInt8[0x7a]]
        bytesummary = Model.summarize_raw_values(
            Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY), rawvalues,
            Model.COMPARATOR_UNSIGNED_BYTES)
        rawvalues[1][1] = 0xff
        rawvalues[2][1] = 0x00
        @test bytesummary.lower isa Model.ByteValue
        @test bytesummary.upper isa Model.ByteValue
        @test bytesummary.lower.value == UInt8[0x61]
        @test bytesummary.upper.value == UInt8[0x7a]
        @test bytesummary.lower.value !== rawvalues[1]
        @test bytesummary.upper.value !== rawvalues[2]
    end

    @testset "producer policy cross-product" begin
        unsigned = Model.LeafSpec(Model.PHYSICAL_INT32;
            logical=Model.LOGICAL_UNSIGNED_INTEGER, bit_width=32)
        families = (Model.FAMILY_MODERN, Model.FAMILY_DEPRECATED)
        comparators = (Model.COMPARATOR_SIGNED, Model.COMPARATOR_UNSIGNED,
            Model.COMPARATOR_IEEE_FLOAT)
        cutoffs = (
            ("parquet-cpp version 1.3.0-rc1", true),
            ("parquet-cpp version 1.3.0", false),
            ("parquet-mr version 1.10.0-rc1", true),
            ("parquet-mr version 1.10.0", false),
        )
        for (created_by, affected) in cutoffs, family in families,
                comparator in comparators, equal in (false, true)
            lower = le32(UInt32(1))
            upper = equal ? copy(lower) : le32(UInt32(2))
            decision = Model.producer_decision(created_by, unsigned, comparator,
                family, lower, upper)
            nonsigned = comparator != Model.COMPARATOR_SIGNED
            expected = affected && nonsigned && !equal
            @test (decision.state == Model.TRUST_UNTRUSTED) == expected
        end

        binary = Model.LeafSpec(Model.PHYSICAL_BYTE_ARRAY)
        versions = (
            ("parquet-mr version 1.7.9", :parquet_251),
            ("parquet-mr version 1.8.0", :legacy_wrong_order),
            ("parquet-mr version 1.5.0-cdh5.5.0", :legacy_wrong_order),
            ("parquet-mr version 1.10.0", :trusted),
        )
        for (created_by, distinct_reason) in versions, family in families,
                equal in (false, true)
            lower = UInt8[0x61]
            upper = equal ? copy(lower) : UInt8[0x62]
            decision = Model.producer_decision(created_by, binary,
                Model.COMPARATOR_UNSIGNED_BYTES, family, lower, upper)
            expected = equal && distinct_reason == :legacy_wrong_order ? :trusted :
                distinct_reason
            @test decision.reason == expected
        end
    end

    @testset "atomic family presence cross-product" begin
        leaf = Model.LeafSpec(Model.PHYSICAL_INT32)
        deprecatedlower = le32(UInt32(3))
        deprecatedupper = le32(UInt32(7))
        for haslower in (false, true), hasupper in (false, true)
            stats = Model.RawStatistics(
                modern_lower=haslower ? le32(UInt32(4)) : nothing,
                modern_upper=hasupper ? le32(UInt32(6)) : nothing,
                deprecated_lower=deprecatedlower,
                deprecated_upper=deprecatedupper)
            result = Model.interpret_statistics(leaf, Int64(1), stats,
                Model.DeclaredOrder[Model.ORDER_TYPE])
            expectedfamily = haslower || hasupper ? Model.FAMILY_MODERN :
                Model.FAMILY_DEPRECATED
            @test result.family == expectedfamily
            if expectedfamily == Model.FAMILY_MODERN
                @test (result.lower.state == Model.BOUND_ABSENT) == !haslower
                @test (result.upper.state == Model.BOUND_ABSENT) == !hasupper
            else
                @test result.lower.value == Model.SignedValue(Int128(3))
                @test result.upper.value == Model.SignedValue(Int128(7))
            end
        end
    end

    @testset "frozen manifest and forbidden dependencies" begin
        manifest = TOML.parsefile(joinpath(@__DIR__, "cases.toml"))
        @test manifest["schema_version"] == 1
        @test manifest["authority_plan_sha256"] ==
            "15adf34af765a3300d8a49ced73532ed02b7e4edee5764453c58e53fcf12c304"
        @test manifest["parquet_format_commit"] ==
            "c47e2a66e88943fc46fde1b028a9432f14fdf5c0"
        @test manifest["arrow_statistics_policy_commit"] ==
            "515410b2a14ac766258e00b07eab9e5ee2692a62"
        @test manifest["parquet_java_tag"] == "apache-parquet-1.17.1"
        @test manifest["parquet_java_commit"] ==
            "78a8d3230eb4769db93de5f2f2e18363c04cae81"
        @test manifest["parquet_java_embedded_format"] == "2.12"
        @test manifest["module"] == "N6StatisticsModel.jl"
        @test manifest["runner"] == "runtests.jl"
        @test manifest["production_dependency_allowed"] == false
        @test manifest["float16"]["pattern_count"] == 65_536
        @test manifest["float16"]["nan_pattern_count"] == 2_046
        @test manifest["float16"]["ordered_little_endian_sha256"] ==
            "61619b1a4260ee4cff6d462d21cab5a049198a27b3a9ae3e8ecfe246efc1d4b6"
        @test manifest["limits"]["default_statistics_value_bytes"] == 4096
        @test manifest["limits"]["equality_succeeds"] == true
        @test manifest["limits"]["fixed_structure_precedes_limit"] == true
        @test manifest["limits"]["variable_limit_precedes_semantics"] == true
        @test length(manifest["case_groups"]) == 13
        caseids = [group["id"] for group in manifest["case_groups"]]
        @test length(unique(caseids)) == length(caseids)
        for group in manifest["case_groups"]
            @test Set(keys(group)) == Set(["id", "requirements",
                "capabilities", "digest_contract", "expected_sha256"])
            @test group["id"] isa String
            @test !isempty(group["requirements"])
            @test all(value -> value isa String, group["requirements"])
            @test group["capabilities"] == sort(group["capabilities"])
            @test length(group["capabilities"]) ==
                length(unique(group["capabilities"]))
            @test group["digest_contract"] ==
                "n6-capability-result-sha256-v1"
            @test Set(keys(group["expected_sha256"])) ==
                Set(group["capabilities"])
            @test all(value -> occursin(r"^[0-9a-f]{64}$", value),
                values(group["expected_sha256"]))
        end

        package_name = string("Par", "quet")
        forbidden = String[
            string("using ", package_name),
            string("import ", package_name),
            string(package_name, "."),
            string("include(\"", "..", "/"),
            string("include(\"", "src", "/"),
            string("src", "/", "statistics.jl"),
        ]
        juliafiles = sort(filter(path -> endswith(path, ".jl"), readdir(@__DIR__;
            join=true)))
        @test basename.(juliafiles) == ["N6StatisticsModel.jl", "runtests.jl"]
        for path in juliafiles
            source = read(path, String)
            @test all(needle -> !occursin(needle, source), forbidden)
        end
        @test !isdefined(Main, Symbol(package_name))
    end
end
