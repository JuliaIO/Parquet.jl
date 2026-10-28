using PrecompileTools: @compile_workload, @setup_workload

# Writes and reads small tables of the common shapes through the public API, from a
# path and from bytes, so first use in a fresh session skips most compilation. Values
# repeat so that dictionary-encoded pages, which most producers write, are exercised.
@setup_workload begin
    rows = 64
    cycle(values) = [values[mod1(i, length(values))] for i in 1:rows]
    nullable(values) = Union{Missing,eltype(values)}[isodd(i) ? missing : values[i]
        for i in eachindex(values)]
    scalars = (
        bool=cycle(Bool[true, false, true]), i8=cycle(Int8[1, -2, 3]),
        i16=cycle(Int16[1, -2, 3]), i32=cycle(Int32[1, -2, 3]), i64=cycle(Int64[1, -2, 3]),
        u8=cycle(UInt8[1, 2, 3]), u16=cycle(UInt16[1, 2, 3]), u32=cycle(UInt32[1, 2, 3]),
        u64=cycle(UInt64[1, 2, 3]), f32=cycle(Float32[1.5, -2, 3]),
        f64=cycle(Float64[1.5, -2, 3]), str=cycle(["a", "bb", "a longer string value"]),
        date=cycle(Dates.Date(2026, 1, 1) .+ Dates.Day.(0:2)),
        datetime=cycle(Dates.DateTime(2026, 1, 1) .+ Dates.Second.(0:2)),
        ns=cycle([Durations.Timestamp{Dates.Nanosecond}(Dates.UTInstant(
            Dates.Nanosecond(i))) for i in 0:2]),
        us=cycle([Durations.Timestamp{Dates.Microsecond}(Dates.UTInstant(
            Dates.Microsecond(i))) for i in 0:2]),
        utc=cycle([Durations.ZonedTimestamp{Dates.Microsecond,:UTC}(
            Durations.Timestamp{Dates.Microsecond}(Dates.UTInstant(Dates.Microsecond(i))),
            Dates.UTC) for i in 0:2]),
        duration=cycle([Durations.Duration(i, 0, 1_000_000i) for i in 0:2]),
        time=cycle(Dates.Time(1) .+ Dates.Minute.(0:2)),
        dec=cycle(reinterpret(DataDecimals.Decimal{15,2,Int64}, Int64[100, -250, 7])),
        wide=cycle(reinterpret(DataDecimals.Decimal{38,4,Int128}, Int128[1, -2, 3])),
        uuid=cycle([UUIDs.UUID(UInt128(i)) for i in 1:3]),
    )
    optional = NamedTuple{keys(scalars)}(map(nullable, values(scalars)))
    nested = (
        list=cycle([[1, 2], Int[], [3]]),
        strings=cycle([["a"], ["b", "c"], String[]]),
        nlist=cycle(Union{Missing,Vector{Union{Missing,Int64}}}[[1, missing], missing, [3]]),
        st=cycle([(a=Int32(i), b="s$i", c=Float64(i)) for i in 1:3]),
        map=cycle([Dict("a" => i) for i in 1:3]),
        lst=cycle([[(k=i, v="v")] for i in 1:3]),
        pairs=cycle([["a" => i, "b" => -i] for i in 1:3]),
        tuple=cycle([(i, i + 1) for i in 1:3]),
    )
    # pyarrow stores DECIMAL(15,2) (TPC-H prices) as 7-byte FIXED_LEN_BYTE_ARRAY where
    # the writer uses INT64, so reading its layout needs a sample of its own: a
    # 3-row nullable column written by pyarrow 25 with default options.
    pyarrowdecimal = hex2bytes(
        "504152311504151c15204c150415001200000e3400000000000064ffffffffffff06150015121516" *
        "2c15061510150615061c1807000000000000641807ffffffffffff06160228070000000000006418" *
        "07ffffffffffff06111100000009200200000003050103021504192c35001806736368656d611502" *
        "00150e150e1502180164250a1504151e2c5c1504151e0000001606191c191c26001c150e19350006" *
        "10191801641502160616c00116c801264426081c1807000000000000641807ffffffffffff061602" *
        "2807000000000000641807ffffffffffff06111100192c15041500150200150015101502003c2906" *
        "1926020400000016c0011606260816c801002820706172717565742d6370702d6172726f77207665" *
        "7273696f6e2032352e302e31191c1c000000c200000050415231")
    # One nested column sends every column of a table down the nested reader.
    mixed = merge(scalars, NamedTuple{Symbol.(:o_, keys(optional))}(values(optional)),
        nested)
    options = ((;), (compress=:zstd,), (compress=nothing, dictionary=false),
        (pageversion=:v2,))
    dir = mktempdir()
    @compile_workload begin
        for table in (scalars, optional, mixed), option in options
            bytes = take!(write(IOBuffer(), table; option...))
            readback = Table(bytes)
            # Writing what a read returns covers the reader's column types.
            write(IOBuffer(), Tables.columntable(readback); option...)
            close(readback)
            # A fresh name each time: Windows cannot reopen a file a read still maps.
            path = write(tempname(dir) * ".parquet", table; option...)
            readback = Table(path)
            Tables.columns(readback)
            Tables.schema(readback)
            # A rewrite of a read table takes the schema-bearing writer.
            write(IOBuffer(), readback; option...)
            close(readback)
            readback = Table(path; scan=Tables.Scan(select=(first(keys(table)),)))
            close(readback)
        end
        Tables.columns(Table(pyarrowdecimal))
        bytes = take!(write(IOBuffer(), scalars))
        readback = Table(bytes; scan=Tables.Scan(select=(:i64, :str),
            filter=Tables.col(:i64) > 0, limit=2))
        Tables.columns(readback)
        close(readback)
        # Filters and row windows: pruning by statistics and page indexes, filter columns
        # decoded first, the other columns of every type only for matching rows.
        for table in (scalars, optional)
            bytes = take!(write(IOBuffer(), table; rowgroupsize=16, pagesize=64))
            for filter in (Tables.colcmp(>=, Tables.col(:us), Dates.DateTime(1970, 1, 1)),
                    Tables.colcmp(==, Tables.col(:str), "a") | Tables.isnull(Tables.col(:f64)),
                    Tables.colin(Tables.col(:date), (Dates.Date(2026, 1, 2),)) &
                        (Tables.col(:i64) > 0))
                Tables.columns(Table(bytes; filter=filter))
            end
            Tables.columns(Table(bytes; limit=3))
        end
        Writer(joinpath(dir, "writer.parquet")) do writer
            write(writer, optional)
            write(writer, optional)
        end
        write(IOBuffer(), Tables.partitioner((scalars, scalars)))
        write(joinpath(dir, "partitioned"), scalars; partitionby=:i8)
    end
    # Interned schema names are per-process state; the image must start without them.
    lock(_SCHEMA_NAME_REGISTRY.lock) do
        _SCHEMA_NAME_REGISTRY.state = _SchemaNameState(Set{String}(), Int64(0))
    end
end
