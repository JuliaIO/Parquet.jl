using Dates
using Tables
import Random

const PDMD = Parquet.Metadata

pdbytes(table; kw...) = take!(Parquet.write(IOBuffer(), table; kw...))

# The file `bytes` with its footer metadata replaced by `f(metadata)`.
function pdrefooter(f, bytes::Vector{UInt8})
    n = length(bytes)
    start = n - 8 - Int(Parquet._readu32le(bytes, n - 7))
    metadata = Parquet.Thrift.decode(view(bytes, (start + 1):(n - 8)), PDMD.FileMetaData)
    footer = Parquet.Thrift.encode(f(metadata))
    return vcat(bytes[1:start], footer, reinterpret(UInt8, [htol(UInt32(length(footer)))]),
        b"PAR1")
end

# `bytes` with the statistics of column `column` in every row group replaced by
# `f(statistics, rowgroup)`, and the file's `created_by` and column orders replaced when
# given.
function pdstatistics(f, bytes::Vector{UInt8}, column::Int; created_by=missing,
        column_orders=missing)
    return pdrefooter(bytes) do metadata
        groups = map(enumerate(metadata.row_groups)) do (index, group)
            columns = copy(group.columns)
            chunk = columns[column]
            md = chunk.meta_data
            columns[column] = Parquet._withfields(chunk; meta_data=Parquet._withfields(md;
                statistics=f(md.statistics, index)))
            return Parquet._withfields(group; columns=columns)
        end
        fields = (row_groups=groups,
            (created_by === missing ? () : (created_by=created_by,))...,
            (column_orders === missing ? () : (column_orders=column_orders,))...)
        return Parquet._withfields(metadata; fields...)
    end
end

# Row groups (index, rows that can match) the filter of a read keeps.
function pdparts(bytes, filter)
    file = Parquet.File(bytes; filter=filter)
    try
        parts = Parquet._filterparts(file, eachindex(file.plan.metadata.row_groups),
            file.plan.bound.filter, Parquet._ByteBudget(file.plan.limits))
        return [(part.group, Parquet._rowcount(part.rows)) for part in parts]
    finally
        close(file)
    end
end

pdgroups(bytes, filter) = first.(pdparts(bytes, filter))

# A read with `scan` equals the whole file read and then scanned, with 1 and 3 tasks.
function pdcheck(bytes, scan::Tables.Scan)
    expected = Tables.scan(Tables.columns(Parquet.Table(bytes)), scan)
    for ntasks in (1, 3)
        got = Parquet.Table(bytes; scan=scan, ntasks=ntasks)
        @test Tables.columnnames(got) == Tables.columnnames(expected)
        for name in Tables.columnnames(expected)
            @test isequal(collect(Tables.getcolumn(got, name)),
                collect(Tables.getcolumn(expected, name)))
        end
    end
    return
end

pdcheck(bytes; kw...) = pdcheck(bytes, Tables.Scan(; kw...))

@testset "pushdown: three-valued summaries" begin
    bits(values) = reduce(|, (value === true ? Parquet._CAN_TRUE : value === false ?
        Parquet._CAN_FALSE : Parquet._CAN_MISSING for value in values); init=0x00)
    results = (true, false, missing)
    subsets = [[results[i] for i in 1:3 if mask >> (i - 1) & 1 == 1] for mask in 1:7]
    for a in subsets
        @test Parquet._notcan(bits(a)) == bits([!x for x in a])
        for b in subsets
            @test Parquet._andcan(bits(a), bits(b)) == bits([x & y for x in a for y in b])
            @test Parquet._orcan(bits(a), bits(b)) == bits([x | y for x in a for y in b])
        end
    end
end

@testset "pushdown: row groups and pages by statistics" begin
    n = 10_000
    table = (id=collect(1:n), s=[string("k", lpad(i, 5, '0')) for i in 1:n],
        f=[isodd(i) ? missing : Float64(i) for i in 1:n],
        long=["a long string value number $i" for i in 1:n])
    for pageindex in (true, false), dictionary in (true, false)
        bytes = pdbytes(table; rowgroupsize=2_500, pagesize=512, pageindex=pageindex,
            dictionary=dictionary)
        parts = pdparts(bytes, Parquet.col(:id) < 300)
        @test first.(parts) == [1]
        # With a page index only the pages that can hold `id < 300` are candidates.
        @test pageindex ? 299 <= parts[1][2] < 2_500 : parts[1][2] == 2_500
        @test pdgroups(bytes, Parquet.colcmp(==, Parquet.col(:id), 7_777)) == [4]
        @test pdgroups(bytes, Parquet.colcmp(>=, Parquet.col(:s), "k07500")) == [3, 4]
        @test pdgroups(bytes, startswith(Parquet.col(:s), "k024")) == [1]
        @test pdgroups(bytes, Parquet.colin(Parquet.col(:id), (5, 9_999))) == [1, 4]
        @test pdgroups(bytes, (Parquet.col(:id) < 10) | (Parquet.col(:id) > 9_990)) == [1, 4]
        @test pdgroups(bytes, !Parquet.colcmp(>=, Parquet.col(:id), 100)) == [1]
        @test pdgroups(bytes, Parquet.colcmp(>, Parquet.col(:f), 9_000.0)) == [4]
        @test isempty(pdgroups(bytes, Parquet.colcmp(>, Parquet.col(:id), n)))
        @test isempty(pdgroups(bytes, Parquet.isnull(Parquet.col(:id))))
        @test length(pdgroups(bytes, Parquet.isnull(Parquet.col(:f)))) == 4
        pdcheck(bytes; filter=Parquet.col(:id) < 300)
        pdcheck(bytes; filter=Parquet.colcmp(==, Parquet.col(:id), 7_777),
            select=(:long, :s))
        pdcheck(bytes; filter=startswith(Parquet.col(:s), "k024") &
            !Parquet.isnull(Parquet.col(:f)), limit=7, offset=3)
        pdcheck(bytes; filter=Parquet.colcmp(>, Parquet.col(:f), 9_000.0), select=(:long,))
        pdcheck(bytes; filter=Parquet.isnull(Parquet.col(:f)), limit=2_600)
    end
end

@testset "pushdown: ecosystem types prune and filter" begin
    n = 4_000
    base = DateTime(2025, 1, 1)
    local_ = [Parquet.Durations.Timestamp{Microsecond}(base + Second(37 * i)) for i in 1:n]
    zoned = [Parquet.Durations.ZonedTimestamp{Millisecond,:UTC}(
        Parquet.Durations.Timestamp{Millisecond}(base + Second(11 * i)), Dates.UTC) for i in 1:n]
    D = Parquet.DataDecimals.Decimal
    table = (day=[Date(2020, 1, 1) + Day(i ÷ 10) for i in 1:n], ts=local_, zts=zoned,
        d9=[D{9,2,Int32}(reinterpret, Int32(i - 2_000)) for i in 1:n],
        d18=[D{18,3,Int64}(reinterpret, Int64(i) * 1_234_567 - 10^9) for i in 1:n],
        d30=[D{30,4,Int128}(reinterpret, Int128(i) * 10^12 - 3 * 10^15) for i in 1:n],
        u32=UInt32[0xffff_ffff - UInt32(i) for i in 1:n], u8=UInt8[i % 256 for i in 1:n])
    bytes = pdbytes(table; rowgroupsize=1_000, pagesize=256)
    cutoff = base + Second(37 * 3_100)
    # A DateTime and a Timestamp of any unit filter and prune a Timestamp column.
    for value in (cutoff, Parquet.Durations.Timestamp{Microsecond}(cutoff),
            Parquet.Durations.Timestamp{Nanosecond}(cutoff))
        @test pdgroups(bytes, Parquet.colcmp(>=, Parquet.col(:ts), value)) == [4]
        pdcheck(bytes; filter=Parquet.colcmp(>=, Parquet.col(:ts), value))
    end
    zcut = zoned[2_500]
    @test pdgroups(bytes, Parquet.colcmp(<, Parquet.col(:zts), zcut)) == [1, 2, 3]
    pdcheck(bytes; filter=Parquet.colcmp(<, Parquet.col(:zts), zcut))
    @test pdgroups(bytes, Parquet.colcmp(<, Parquet.col(:day), Date(2020, 2, 1))) == [1]
    pdcheck(bytes; filter=Parquet.colcmp(<, Parquet.col(:day), Date(2020, 2, 1)))
    # Decimals on INT32, INT64, and FIXED_LEN_BYTE_ARRAY, against integers and decimals.
    @test pdgroups(bytes, Parquet.colcmp(<, Parquet.col(:d9), -15)) == [1]
    @test pdgroups(bytes, Parquet.colcmp(>, Parquet.col(:d9), D{9,2,Int32}(reinterpret,
        Int32(1_500)))) == [4]
    @test pdgroups(bytes, Parquet.colcmp(<=, Parquet.col(:d18), -998_000)) == [1]
    @test pdgroups(bytes, Parquet.colcmp(>, Parquet.col(:d30), 100_000_000)) == [4]
    pdcheck(bytes; filter=Parquet.colcmp(<, Parquet.col(:d9), -15))
    pdcheck(bytes; filter=Parquet.colcmp(>, Parquet.col(:d30), 100_000_000))
    # Unsigned columns order their statistics as unsigned values.
    @test pdgroups(bytes, Parquet.colcmp(>, Parquet.col(:u32), 0xffff_ffff - 500)) == [1]
    @test pdgroups(bytes, Parquet.colcmp(<, Parquet.col(:u32), 0xffff_ffff - 3_500)) == [4]
    pdcheck(bytes; filter=Parquet.colcmp(>, Parquet.col(:u32), 0xffff_ffff - 500))
    pdcheck(bytes; filter=Parquet.colcmp(==, Parquet.col(:u8), 200))
end

@testset "pushdown: statistics trust rules" begin
    le64(x) = collect(reinterpret(UInt8, [htol(x)]))
    le32(x) = collect(reinterpret(UInt8, [htol(x)]))
    rows(bytes, filter) = Tables.rowcount(Parquet.Table(bytes; filter=filter))

    # Unsigned values with legacy (signed-order) min/max: the bounds are not trusted.
    bytes = pdbytes((x=UInt32[10, 3_000_000_000],); rowgroupsize=2)
    legacy = pdstatistics(bytes, 1) do statistics, _
        PDMD.Statistics(min=le32(UInt32(3_000_000_000)), max=le32(UInt32(10)))
    end
    @test rows(legacy, Parquet.colcmp(==, Parquet.col(:x), 10)) == 1
    @test pdgroups(legacy, Parquet.colcmp(==, Parquet.col(:x), 10)) == [1]

    # Modern min/max take precedence over legacy ones that would rule the row out.
    bytes = pdbytes((x=[5, 6],))
    both = pdstatistics(bytes, 1) do statistics, _
        Parquet._withfields(statistics; min=le64(100), max=le64(200))
    end
    @test rows(both, Parquet.colcmp(==, Parquet.col(:x), 5)) == 1

    # Without column orders, modern statistics have no defined order.
    noorders = pdstatistics((statistics, _) -> PDMD.Statistics(min_value=le64(100),
        max_value=le64(200)), bytes, 1; column_orders=nothing)
    @test rows(noorders, Parquet.colcmp(==, Parquet.col(:x), 5)) == 1

    # NaN values lie outside the bounds: `!=` keeps them, and NaN bounds are ignored.
    bytes = pdbytes((x=[1.0, NaN],))
    @test rows(bytes, Parquet.colcmp(!=, Parquet.col(:x), 1.0)) == 1
    nanbounds = pdstatistics(bytes, 1) do statistics, _
        Parquet._withfields(statistics; min_value=le64(reinterpret(UInt64, NaN)),
            max_value=le64(reinterpret(UInt64, NaN)), nan_count=nothing)
    end
    @test rows(nanbounds, Parquet.colcmp(<, Parquet.col(:x), 5.0)) == 1
    @test rows(bytes, Parquet.colcmp(==, Parquet.col(:x), NaN)) == 0
    @test rows(bytes, Parquet.colin(Parquet.col(:x), Set([NaN]))) == 1

    # Binary statistics from parquet-mr before 1.8.0 (PARQUET-251) and old sort orders
    # (parquet-cpp before 1.3.0, parquet-mr before 1.10) are not trusted.
    bytes = pdbytes((s=["b", "c"],))
    wrong(statistics, _) = Parquet._withfields(statistics; min_value=Vector{UInt8}("x"),
        max_value=Vector{UInt8}("y"))
    for (producer, trusted) in (("parquet-mr version 1.7.0 (build abc)", false),
            ("parquet-cpp version 1.2.0", false), ("parquet-mr version 1.9.0", false),
            ("parquet-mr version 1.12.3 (build abc)", true),
            ("parquet-cpp-arrow version 25.0.1", true))
        file = pdstatistics(wrong, bytes, 1; created_by=producer)
        # Trusted bounds, though wrong here, rule the row out; untrusted ones do not.
        @test rows(file, Parquet.colcmp(==, Parquet.col(:s), "b")) == (trusted ? 0 : 1)
    end

    # Truncated (inexact) string bounds still bound the values.
    bytes = pdbytes((s=["aa", "abz"],))
    truncated = pdstatistics(bytes, 1) do statistics, _
        Parquet._withfields(statistics; min_value=Vector{UInt8}("aa"),
            max_value=Vector{UInt8}("ac"), is_max_value_exact=false)
    end
    @test rows(truncated, Parquet.colcmp(==, Parquet.col(:s), "abz")) == 1
    @test rows(truncated, Parquet.colcmp(>, Parquet.col(:s), "ab")) == 1
    @test rows(truncated, startswith(Parquet.col(:s), "ab")) == 1

    # Bounds longer than `max_statistics_value_bytes` are not used.
    long = repeat("z", 100)
    bytes = pdbytes((s=[long * "1", long * "2"],))
    big = pdstatistics(bytes, 1) do statistics, _
        Parquet._withfields(statistics; min_value=Vector{UInt8}(long * "5"),
            max_value=Vector{UInt8}(long * "6"))
    end
    @test rows(big, Parquet.colcmp(==, Parquet.col(:s), long * "1")) == 0
    limits = Parquet.Limits(max_statistics_value_bytes=50)
    @test Tables.rowcount(Parquet.Table(big; filter=Parquet.colcmp(==, Parquet.col(:s),
        long * "1"), limits=limits)) == 1

    # Null counts: a required column has no nulls whatever its statistics say; an
    # optional column without a null count may hold nulls.
    bytes = pdbytes((x=[1, 2],))
    allnull = pdstatistics((statistics, _) -> Parquet._withfields(statistics;
        null_count=2), bytes, 1)
    @test rows(allnull, Parquet.colcmp(==, Parquet.col(:x), 1)) == 1
    bytes = pdbytes((x=Union{Missing,Int}[1, missing],))
    @test rows(bytes, Parquet.isnull(Parquet.col(:x))) == 1
    nocount = pdstatistics((statistics, _) -> Parquet._withfields(statistics;
        null_count=nothing), pdbytes((x=Union{Missing,Int}[1, missing],)), 1)
    @test rows(nocount, Parquet.isnull(Parquet.col(:x))) == 1
    zero = pdstatistics((statistics, _) -> Parquet._withfields(statistics; null_count=0),
        bytes, 1)
    @test rows(zero, Parquet.isnull(Parquet.col(:x))) == 0

    # Statistics that do not decode are ignored rather than failing the read.
    bad = pdstatistics((statistics, _) -> Parquet._withfields(statistics;
        min_value=UInt8[1, 2, 3]), pdbytes((x=[1, 2],)), 1)
    @test rows(bad, Parquet.colcmp(==, Parquet.col(:x), 1)) == 1
end

@testset "pushdown: bloom filters and dictionary pages" begin
    rng = Random.Xoshiro(3)
    n = 4_000
    ids = Random.shuffle(rng, collect(1:n))
    table = (id=ids, s=["value-$(i)" for i in ids], cat=[("a", "b", "c", "d")[i % 4 + 1]
        for i in 1:n])
    bytes = pdbytes(table; rowgroupsize=1_000, bloomfilter=[:id, :s], dictionary=true)
    # Statistics of shuffled values cannot prune; bloom filters can.
    @test pdgroups(bytes, Parquet.colcmp(==, Parquet.col(:id), ids[1_500])) == [2]
    @test pdgroups(bytes, Parquet.colcmp(==, Parquet.col(:s), "value-$(ids[3_999])")) == [4]
    @test pdgroups(bytes, Parquet.colin(Parquet.col(:id), (ids[10], ids[3_010]))) == [1, 4]
    @test isempty(pdgroups(bytes, Parquet.colcmp(==, Parquet.col(:s), "value-0")))
    pdcheck(bytes; filter=Parquet.colcmp(==, Parquet.col(:s), "value-$(ids[3_999])"))
    pdcheck(bytes; filter=Parquet.colin(Parquet.col(:id), (ids[10], ids[3_010], -1)))
    nobloom = pdbytes(table; rowgroupsize=1_000, dictionary=false)
    @test length(pdgroups(nobloom, Parquet.colcmp(==, Parquet.col(:id), ids[1_500]))) == 4

    # A value inside the bounds but absent from every dictionary prunes every group.
    @test isempty(pdgroups(bytes, Parquet.colcmp(==, Parquet.col(:cat), "bb")))
    @test isempty(pdgroups(bytes, endswith(Parquet.col(:cat), "z")))
    @test length(pdgroups(bytes, Parquet.colcmp(==, Parquet.col(:cat), "b"))) == 4
    pdcheck(bytes; filter=Parquet.colcmp(==, Parquet.col(:cat), "bb"))
    # Ruling one leaf of an `|` out cannot prune, so its dictionary is not consulted.
    @test length(pdgroups(bytes, Parquet.colcmp(==, Parquet.col(:cat), "bb") |
        (Parquet.col(:id) > 0))) == 4
end

@testset "pushdown: late materialization of strings" begin
    rng = Random.Xoshiro(9)
    n = 6_000
    long = [isodd(i) ? "long string $(i) " * repeat("x", i % 40) : "s$i" for i in 1:n]
    table = (key=rand(rng, 1:50, n), long=Union{Missing,String}[i % 7 == 0 ? missing :
        long[i] for i in 1:n], short=["v$(i % 13)" for i in 1:n])
    for dictionary in (true, false), compress in (:snappy, nothing)
        bytes = pdbytes(table; rowgroupsize=1_500, pagesize=1_024, dictionary=dictionary,
            compress=compress)
        pdcheck(bytes; filter=Parquet.colcmp(==, Parquet.col(:key), 7))
        pdcheck(bytes; filter=Parquet.colcmp(==, Parquet.col(:key), 7), select=(:long,))
        pdcheck(bytes; filter=Parquet.colcmp(!=, Parquet.col(:key), 7))
        pdcheck(bytes; filter=Parquet.isnull(Parquet.col(:long)), limit=100, offset=20)
        pdcheck(bytes; filter=startswith(Parquet.col(:long), "long string 1"),
            select=(:short, :long))
        pdcheck(bytes; filter=Parquet.colcmp(<, Parquet.col(:key), 0))
    end
    # A few selected rows do not keep whole pages alive.
    bytes = pdbytes(table; rowgroupsize=n, pagesize=1_024)
    filtered = Parquet.Table(bytes; filter=Parquet.colcmp(==, Parquet.col(:key), 7))
    column = filtered.long
    kept = sum(length, column.buffers)
    @test kept < 4 * sum(value -> ismissing(value) ? 0 : ncodeunits(value), column; init=0) +
        1_024
end

@testset "pushdown: row windows read only the pages they need" begin
    n = 9_000
    table = (a=collect(1:n), b=["b$i" for i in 1:n])
    bytes = pdbytes(table; rowgroupsize=3_000, pagesize=256)
    for (offset, limit) in ((0, 5), (2_990, 20), (3_000, 1), (8_999, 10), (100, 0),
            (0, nothing), (5_000, nothing), (10_000, 3))
        pdcheck(bytes; limit=limit, offset=offset)
        pdcheck(bytes; limit=limit, offset=offset, select=(:b,))
    end
    pdcheck(bytes; limit=50, filter=Parquet.col(:a) > 2_500)
    pdcheck(bytes; limit=50, offset=600, filter=Parquet.col(:a) > 2_500)
    # A small limit reads the first row group's chunks only.
    bytes = pdbytes((a=collect(1:60_000), b=["b$i" for i in 1:60_000]); rowgroupsize=20_000)
    source = HandleTrackingSource(bytes)
    @test Parquet.Table(source; limit=5).a == 1:5
    limited = sum(last, source.calls)
    source = HandleTrackingSource(bytes)
    Parquet.Table(source)
    @test limited < sum(last, source.calls) ÷ 2
end

@testset "pushdown: Parquet.File skips row groups its filter rules out" begin
    bytes = pdbytes((a=collect(1:1_000),); rowgroupsize=100)
    file = Parquet.File(bytes; filter=Parquet.col(:a) > 750)
    @test length(file) == 3
    @test [Tables.rowcount(table) for table in file] == [50, 100, 100]
    @test file[1].a == 751:800
    close(file)
    file = Parquet.File(bytes; filter=Parquet.col(:a) > 750, limit=120)
    @test [Tables.rowcount(table) for table in file] == [50, 70]
    close(file)
    @test Tables.schema(Parquet.File(bytes; filter=Parquet.col(:a) > 5_000)).names == (:a,)
    @test Tables.rowcount(Parquet.Table(bytes; filter=Parquet.col(:a) > 5_000)) == 0
end

@testset "pushdown: page indexes that disagree with the pages" begin
    bytes = pdbytes((a=collect(1:2_000),); pagesize=256)
    file = Parquet.File(bytes)
    chunk = Parquet.metadata(file).row_groups[1].columns[1]
    close(file)
    @test chunk.offset_index_offset !== nothing && chunk.column_index_offset !== nothing
    range = (chunk.offset_index_offset + 1):(chunk.offset_index_offset +
        chunk.offset_index_length)
    index = Parquet.Thrift.decode(view(bytes, range), PDMD.OffsetIndex)
    locations = copy(index.page_locations)
    @test length(locations) > 2
    # The last page claims to start one row early: a read of its rows must fail.
    locations[end] = Parquet._withfields(locations[end];
        first_row_index=locations[end].first_row_index - 1)
    encoded = Parquet.Thrift.encode(PDMD.OffsetIndex(page_locations=locations))
    @test length(encoded) == length(range)
    broken = copy(bytes)
    copyto!(broken, first(range), encoded, 1, length(encoded))
    @test_throws Parquet.FormatError Parquet.Table(broken; filter=Parquet.col(:a) > 1_990)
    # Reads that do not use the index are unaffected.
    @test Parquet.Table(broken).a == 1:2_000
    @test Parquet.Table(broken; filter=Parquet.col(:a) < 10).a == 1:9
end

@testset "pushdown: Polars float bounds that leave out NaN pages" begin
    # Polars 1.43 bounds each float chunk by its NaN-free pages only, and marks pages
    # holding a NaN as all null in the column index; neither may prune.
    bytes = read(joinpath(@__DIR__, "fixtures", "polars_nan.parquet"))
    for filter in (Parquet.colcmp(<, Parquet.col(:f), -64.0),
            Parquet.colcmp(>, Parquet.col(:f), 60.0) | Parquet.isnull(Parquet.col(:f)),
            Parquet.colcmp(==, Parquet.col(:f), -66.0), Parquet.colin(Parquet.col(:f),
            (-65.0, 66.0)))
        @test Tables.rowcount(Parquet.Table(bytes; filter=filter)) > 0
        pdcheck(bytes; filter=filter)
    end
end
