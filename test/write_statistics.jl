using Dates
using SHA
using UUIDs
import Tables

const WSMD = Parquet.Metadata

function wsmetadata(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    return try
        Parquet.Thrift.decode(file.footer.bytes, WSMD.FileMetaData)
    finally
        close(file)
    end
end

function wsstats(metadata::WSMD.FileMetaData, group::Int, column::Int)
    return metadata.row_groups[group].columns[column].meta_data.statistics
end

function wspages(bytes::Vector{UInt8}, column::Int)
    file = Parquet.File(bytes)
    return try
        metadata = Parquet.Thrift.decode(file.footer.bytes, WSMD.FileMetaData)
        chunk = metadata.row_groups[1].columns[column].meta_data
        start, stop = Parquet._chunkrange(chunk, file.footer.offset)
        headers = WSMD.PageHeader[]
        position = start
        while position < stop
            frame = Parquet.readpage(file.source, position, stop, Parquet.Limits())
            push!(headers, frame.header)
            position = Parquet.pageend(frame)
        end
        headers
    finally
        close(file)
    end
end

function wscolumnindex(metadata::WSMD.FileMetaData, name::String)
    for (index, leaf) in enumerate(metadata.schema[2:end])
        leaf.name == name && return index
    end
    throw(ArgumentError("writer statistics test column $name is absent"))
end

function wsorderkind(order::WSMD.ColumnOrder)
    order.TYPE_ORDER !== nothing && return :type
    order.IEEE_754_TOTAL_ORDER !== nothing && return :ieee
    return :unknown
end

function wsle16(bytes::AbstractVector{UInt8})
    length(bytes) == 2 || throw(ArgumentError("expected two bytes"))
    return UInt16(bytes[1]) | (UInt16(bytes[2]) << 8)
end

function wsle32(bytes::AbstractVector{UInt8})
    length(bytes) == 4 || throw(ArgumentError("expected four bytes"))
    return UInt32(bytes[1]) | (UInt32(bytes[2]) << 8) |
        (UInt32(bytes[3]) << 16) | (UInt32(bytes[4]) << 24)
end

function wsle64(bytes::AbstractVector{UInt8})
    length(bytes) == 8 || throw(ArgumentError("expected eight bytes"))
    value = UInt64(0)
    for index in 8:-1:1
        value = (value << 8) | UInt64(bytes[index])
    end
    return value
end

function wsfloatvalue(::Type{Float16}, bits::UInt16)
    return reinterpret(Float16, bits)
end

function wsfloatvalue(::Type{Float32}, bits::UInt32)
    return reinterpret(Float32, bits)
end

function wsfloatvalue(::Type{Float64}, bits::UInt64)
    return reinterpret(Float64, bits)
end

function wsfloatbits(::Type{Float16}, bytes::AbstractVector{UInt8})
    return wsle16(bytes)
end

function wsfloatbits(::Type{Float32}, bytes::AbstractVector{UInt8})
    return wsle32(bytes)
end

function wsfloatbits(::Type{Float64}, bytes::AbstractVector{UInt8})
    return wsle64(bytes)
end

function wsieeekey(bits::T) where {T<:Union{UInt16,UInt32,UInt64}}
    sign = one(T) << (8 * sizeof(T) - 1)
    return iszero(bits & sign) ? bits | sign : ~bits
end

function wsassertmodern(statistics::WSMD.Statistics)
    @test statistics.min === nothing
    @test statistics.max === nothing
    if statistics.min_value === nothing
        @test statistics.max_value === nothing
        @test statistics.is_min_value_exact === nothing
        @test statistics.is_max_value_exact === nothing
    else
        @test statistics.max_value !== nothing
        @test statistics.is_min_value_exact === true
        @test statistics.is_max_value_exact === true
    end
    return
end

mutable struct WSCountingVector{T} <: AbstractVector{T}
    values::Vector{T}
    reads::Int
end

function Base.IndexStyle(::Type{<:WSCountingVector})
    return IndexLinear()
end

function Base.size(values::WSCountingVector)
    return size(values.values)
end

function Base.getindex(values::WSCountingVector, index::Int)
    values.reads += 1
    return values.values[index]
end

mutable struct WSCallbackTable
    calls::Int
    values::Vector{Int32}
end

function Tables.istable(::Type{WSCallbackTable})
    return true
end

function Tables.columnaccess(::Type{WSCallbackTable})
    return true
end

function Tables.columns(table::WSCallbackTable)
    table.calls += 1
    return (value=table.values,)
end

@testset "writer statistics column orders and logical families" begin
    input = (
        flag=Bool[true, false],
        i32=Int32[-2, 3],
        i64=Int64[-9, 11],
        u32=UInt32[typemax(UInt32), 0],
        u64=UInt64[typemax(UInt64), 0],
        f32=Float32[-0.0, 0.0],
        f64=Float64[-Inf, Inf],
        text=["a\0b", "a\0c"],
        raw=Vector{UInt8}[UInt8[0xff], UInt8[0x00, 0xff]],
        fixed=NTuple{3,UInt8}[(0x01, 0x00, 0xff), (0x01, 0x01, 0x00)],
        date=Date[Date(1969, 12, 31), Date(2000, 2, 29)],
        time=Time[Time(0), Time(23, 59, 59, 999, 999, 999)],
        timestamp=Parquet.Timestamp{:nanos}[
            Parquet.Timestamp(-1, :nanos, false),
            Parquet.Timestamp(1, :nanos, false),
        ],
        decimal32=Parquet.Decimal[
            Parquet.Decimal(-12, 1), Parquet.Decimal(34, 1)],
        decimal64=Parquet.Decimal[
            Parquet.Decimal(-12_345_678_901, 1),
            Parquet.Decimal(12_345_678_902, 1),
        ],
        decimalfixed=Parquet.Decimal[
            Parquet.Decimal(big"-12345678901234567890", 1),
            Parquet.Decimal(big"12345678901234567891", 1),
        ],
        uuid=UUID[
            UUID("00112233-4455-6677-8899-aabbccddeeff"),
            UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        ],
        f16=Float16[-0.0, 0.0],
        json=Parquet.JSONValue[
            Parquet.JSONValue(codeunits("{\"a\":1}")),
            Parquet.JSONValue(codeunits("[1,null,3]")),
        ],
        bson=Parquet.BSONValue[
            Parquet.BSONValue(hex2bytes("0c0000001061000100000000")),
            Parquet.BSONValue(hex2bytes("090000000a00ff0000")),
        ],
        interval=Parquet.Interval[
            Parquet.Interval(0, 1, 2), Parquet.Interval(3, 4, 5)],
        unknown=Missing[missing, missing],
    )
    metadata = wsmetadata(Parquet._encodefile(input; statistics=true))
    @test metadata.column_orders !== nothing
    @test length(metadata.column_orders) == length(metadata.schema) - 1
    floating = Set(["f32", "f64", "f16"])
    for (index, element) in enumerate(metadata.schema[2:end])
        expected = element.name in floating ? :ieee : :type
        @test wsorderkind(metadata.column_orders[index]) == expected
        statistics = wsstats(metadata, 1, index)
        @test statistics !== nothing
        wsassertmodern(statistics)
        @test statistics.null_count == (element.name == "unknown" ? 2 : 0)
        if element.name in floating
            @test statistics.nan_count == 0
        else
            @test statistics.nan_count === nothing
        end
        if element.name in ("interval", "unknown")
            @test statistics.min_value === nothing
            @test statistics.max_value === nothing
        else
            @test statistics.min_value !== nothing
            @test statistics.max_value !== nothing
        end
        chunk = metadata.row_groups[1].columns[index]
        @test chunk.column_index_offset === nothing
        @test chunk.column_index_length === nothing
    end
    unsigned32 = wsstats(metadata, 1, wscolumnindex(metadata, "u32"))
    @test wsle32(unsigned32.min_value) == UInt32(0)
    @test wsle32(unsigned32.max_value) == typemax(UInt32)
    unsigned64 = wsstats(metadata, 1, wscolumnindex(metadata, "u64"))
    @test wsle64(unsigned64.min_value) == UInt64(0)
    @test wsle64(unsigned64.max_value) == typemax(UInt64)
    signed = wsstats(metadata, 1, wscolumnindex(metadata, "i32"))
    @test reinterpret(Int32, wsle32(signed.min_value)) == Int32(-2)
    @test reinterpret(Int32, wsle32(signed.max_value)) == Int32(3)
    text = wsstats(metadata, 1, wscolumnindex(metadata, "text"))
    @test text.min_value == collect(codeunits("a\0b"))
    @test text.max_value == collect(codeunits("a\0c"))
    raw = wsstats(metadata, 1, wscolumnindex(metadata, "raw"))
    @test raw.min_value == UInt8[0x00, 0xff]
    @test raw.max_value == UInt8[0xff]
    fixed = wsstats(metadata, 1, wscolumnindex(metadata, "fixed"))
    @test fixed.min_value == UInt8[0x01, 0x00, 0xff]
    @test fixed.max_value == UInt8[0x01, 0x01, 0x00]
end

@testset "writer IEEE extrema, NaN counts, and signed zero" begin
    formats = (
        (Float16, UInt16, UInt16(0x8000), UInt16(0x0000),
            UInt16(0x7e11), UInt16(0xfe22)),
        (Float32, UInt32, UInt32(0x80000000), UInt32(0x00000000),
            UInt32(0x7fc00011), UInt32(0xffc00022)),
        (Float64, UInt64, UInt64(0x8000000000000000),
            UInt64(0x0000000000000000), UInt64(0x7ff8000000000011),
            UInt64(0xfff8000000000022)),
    )
    for (T, _, negativezero, positivezero, positivenan, negativenan) in formats
        mixed = Union{Missing,T}[
            wsfloatvalue(T, negativezero), wsfloatvalue(T, positivenan),
            missing, wsfloatvalue(T, positivezero),
        ]
        metadata = wsmetadata(Parquet._encodefile((value=mixed,);
            statistics=true))
        statistics = wsstats(metadata, 1, 1)
        @test wsorderkind(only(metadata.column_orders)) == :ieee
        @test statistics.null_count == 1
        @test statistics.nan_count == 1
        @test wsfloatbits(T, statistics.min_value) == negativezero
        @test wsfloatbits(T, statistics.max_value) == positivezero
        allnanbits = (negativenan, positivenan)
        allnan = T[wsfloatvalue(T, bits) for bits in allnanbits]
        allnanmetadata = wsmetadata(Parquet._encodefile((value=allnan,);
            statistics=true))
        allnanstatistics = wsstats(allnanmetadata, 1, 1)
        expected = sort(collect(allnanbits); by=wsieeekey)
        @test allnanstatistics.nan_count == 2
        @test wsfloatbits(T, allnanstatistics.min_value) == first(expected)
        @test wsfloatbits(T, allnanstatistics.max_value) == last(expected)
        for bits in (positivezero, negativezero)
            zero = wsfloatvalue(T, bits)
            zerostatistics = wsstats(wsmetadata(Parquet._encodefile(
                (value=T[zero],); statistics=true)), 1, 1)
            @test zerostatistics.nan_count == 0
            @test wsfloatbits(T, zerostatistics.min_value) == bits
            @test wsfloatbits(T, zerostatistics.max_value) == bits
        end
    end
    allhalves = reinterpret(Float16, collect(UInt16(0):typemax(UInt16)))
    statistics = wsstats(wsmetadata(Parquet._encodefile(
        (value=allhalves,); statistics=true)), 1, 1)
    @test statistics.nan_count == 2046
    @test wsle16(statistics.min_value) == UInt16(0xfc00)
    @test wsle16(statistics.max_value) == UInt16(0x7c00)
end

@testset "writer statistics nested row-group slices" begin
    E = Union{Missing,Int32}
    rows = Union{Missing,Vector{E}}[
        E[Int32(1), missing],
        missing,
        E[],
        E[Int32(5), Int32(-2)],
        E[Int32(7)],
    ]
    metadata = wsmetadata(Parquet._encodefile((items=rows,);
        rowgroupsize=2, statistics=true))
    @test length(metadata.row_groups) == 3
    expected = ((2, Int32(1), Int32(1)),
        (1, Int32(-2), Int32(5)), (0, Int32(7), Int32(7)))
    for (group, (nulls, lower, upper)) in enumerate(expected)
        statistics = wsstats(metadata, group, 1)
        @test statistics.null_count == nulls
        @test reinterpret(Int32, wsle32(statistics.min_value)) == lower
        @test reinterpret(Int32, wsle32(statistics.max_value)) == upper
    end
end

@testset "writer statistics empty and all-null states" begin
    empty = wsmetadata(Parquet._encodefile((integer=Int32[],
        double=Float64[], half=Float16[]);
        statistics=true))
    @test isempty(empty.row_groups)
    @test length(empty.column_orders) == 3
    @test wsorderkind.(empty.column_orders) == [:type, :ieee, :ieee]
    allnull = Union{Missing,Int32}[missing, missing, missing]
    statistics = wsstats(wsmetadata(Parquet._encodefile((value=allnull,);
        statistics=true)), 1, 1)
    @test statistics.null_count == 3
    @test statistics.nan_count === nothing
    @test statistics.min_value === nothing
    @test statistics.max_value === nothing
    wsassertmodern(statistics)
end

@testset "writer statistics value limit" begin
    values = (value=Int32[-1, 2],)
    exact = wsstats(wsmetadata(Parquet._encodefile(values;
        statistics=true,
        limits=Parquet.Limits(max_statistics_value_bytes=4))), 1, 1)
    @test exact.min_value !== nothing
    @test exact.max_value !== nothing
    over = wsstats(wsmetadata(Parquet._encodefile(values;
        statistics=true,
        limits=Parquet.Limits(max_statistics_value_bytes=3))), 1, 1)
    @test over.null_count == 0
    @test over.min_value === nothing
    @test over.max_value === nothing
    variable = (value=Vector{UInt8}[UInt8[0x01, 0x02],
        UInt8[0x01, 0x02, 0x03]],)
    variableexact = wsstats(wsmetadata(Parquet._encodefile(variable;
        statistics=true,
        limits=Parquet.Limits(max_statistics_value_bytes=3))), 1, 1)
    @test variableexact.min_value == UInt8[0x01, 0x02]
    @test variableexact.max_value == UInt8[0x01, 0x02, 0x03]
    variableover = wsstats(wsmetadata(Parquet._encodefile(variable;
        statistics=true,
        limits=Parquet.Limits(max_statistics_value_bytes=2))), 1, 1)
    @test variableover.null_count == 0
    @test variableover.min_value === nothing
    @test variableover.max_value === nothing
    emptybound = wsstats(wsmetadata(Parquet._encodefile(
        (value=Vector{UInt8}[UInt8[]],); statistics=true,
        limits=Parquet.Limits(max_statistics_value_bytes=0))), 1, 1)
    @test emptybound.min_value == UInt8[]
    @test emptybound.max_value == UInt8[]
end

@testset "writer statistics opt-out and page boundary" begin
    input = (id=Int32[1, 2, 3],
        label=Union{Missing,String}["a", missing, "b"])
    disabled = Parquet._encodefile(input; statistics=false)
    @test bytes2hex(sha256(disabled)) ==
        "bd0f5655e9f9aca2a1aa5f1d2721d9ea8f5ca3c9a2714385cca49edfcb7c0256"
    metadata = wsmetadata(disabled)
    @test metadata.column_orders === nothing
    @test all(chunk -> chunk.meta_data.statistics === nothing,
        metadata.row_groups[1].columns)
    @test Parquet._encodefile(input) == Parquet._encodefile(input;
        statistics=true)
    without = WSCountingVector(Int32[1, 2, 3], 0)
    with = WSCountingVector(Int32[1, 2, 3], 0)
    Parquet._encodefile((value=without,); statistics=false)
    Parquet._encodefile((value=with,); statistics=true)
    @test with.reads == without.reads
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile((value=Int32[1, 2, 3],);
            pageversion=pageversion, pagesize=4, statistics=true)
        headers = wspages(bytes, 1)
        @test length(headers) > 1
        for header in headers
            statistics = pageversion === :v1 ?
                header.data_page_header.statistics :
                header.data_page_header_v2.statistics
            @test statistics === nothing
        end
    end
end

@testset "writer statistics validation and destination atomicity" begin
    limits = Parquet.Limits(max_statistics_value_bytes=-1)
    table = WSCallbackTable(0, Int32[1])
    io = IOBuffer()
    Base.write(io, codeunits("unchanged"))
    @test_throws ArgumentError Parquet.write(io, table; limits=limits)
    @test table.calls == 0
    @test String(take!(io)) == "unchanged"
    table = WSCallbackTable(0, Int32[1])
    mktempdir() do directory
        path = joinpath(directory, "existing.parquet")
        write(path, "unchanged")
        @test_throws ArgumentError Parquet.write(path, table; limits=limits)
        @test table.calls == 0
        @test read(path, String) == "unchanged"
        absent = joinpath(directory, "absent.parquet")
        @test_throws ArgumentError Parquet.write(absent, table; limits=limits)
        @test !ispath(absent)
    end
    @test_throws ArgumentError Parquet._encodefile((value=Int32[1],);
        statistics=false, limits=limits)
end

@testset "writer statistics live-budget rollback" begin
    malformed = WSMD.SchemaElement(
        type_=WSMD.Type.FIXED_LEN_BYTE_ARRAY,
        type_length=Int32(2),
        repetition_type=WSMD.FieldRepetitionType.REQUIRED,
        name="value",
    )
    column = Parquet.WriteColumn("value", Vector{UInt8}[UInt8[0x01]],
        WSMD.Type.FIXED_LEN_BYTE_ARRAY, Int32(2), false, nothing, nothing,
        String["value"], nothing, nothing, Int16(0), Int16(0), 1,
        WSMD.SchemaElement[])
    leaf = Parquet.WriteLeafPlan(Int32(1), String["value"], column)
    budget = Parquet._LiveByteBudget(Parquet.Limits())
    before = Parquet._budgetused(budget)
    @test_throws ArgumentError Parquet._writecolumnstatistics(
        leaf, malformed, Int64(4096), budget)
    @test Parquet._budgetused(budget) == before
    validcolumn = Parquet._writecolumn("value", Int32[1, 2])
    validelement = only(validcolumn.schema)
    validleaf = Parquet.WriteLeafPlan(Int32(1), String["value"], validcolumn)
    arraycharge = Parquet._materializedarraybytes(UInt8, 4)
    tight = Parquet._LiveByteBudget(Parquet.Limits(
        max_materialized_bytes=3 * arraycharge))
    @test_throws Parquet.LimitError Parquet._writecolumnstatistics(
        validleaf, validelement, Int64(4096), tight)
    @test Parquet._budgetused(tight) == 0
    success = Parquet._LiveByteBudget(Parquet.Limits())
    statistics = Parquet._writecolumnstatistics(
        validleaf, validelement, Int64(4096), success)
    expected = 2 * Parquet._materializedarraybytes(UInt8, 4) +
        Parquet._MATERIALIZED_OBJECT_BYTES
    @test statistics.null_count == 0
    @test Parquet._budgetused(success) == expected
    Parquet._release!(success, expected)
    @test Parquet._budgetused(success) == 0
    elements = WSMD.SchemaElement[
        WSMD.SchemaElement(name="schema", num_children=Int32(1)),
        validelement,
    ]
    schema = Parquet.Schema(elements)
    orderarray = Parquet._materializedarraybytes(WSMD.ColumnOrder, 1)
    orderbudget = Parquet._LiveByteBudget(Parquet.Limits(
        max_materialized_bytes=orderarray))
    @test_throws Parquet.LimitError Parquet._writecolumnorders(schema, orderbudget)
    @test Parquet._budgetused(orderbudget) == 0
end

@testset "writer fixed statistics allocation stays value-independent" begin
    function allocation(values::Vector{Int32})
        column = Parquet._writecolumn("value", values)
        leaf = Parquet.WriteLeafPlan(Int32(1), String["value"], column)
        budget = Parquet._LiveByteBudget(Parquet.Limits())
        return @allocated Parquet._writecolumnstatistics(
            leaf, only(column.schema), Int64(4096), budget)
    end
    allocation(Int32[1])
    allocation(fill(Int32(1), 100_000))
    small = allocation(Int32[1])
    large = allocation(fill(Int32(1), 100_000))
    @test large <= small + 512
end
