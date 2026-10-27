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
    )
    # One nested column sends every column of a table down the nested reader.
    mixed = merge(scalars, NamedTuple{Symbol.(:o_, keys(optional))}(values(optional)),
        nested)
    options = ((;), (dictionary=true, codec=:snappy), (codec=:zstd,),
        (dictionary=true, pageversion=:v2))
    dir = mktempdir()
    @compile_workload begin
        for table in (scalars, optional, mixed), option in options
            bytes = take!(write(IOBuffer(), table; option...))
            read = Table(bytes)
            # Writing what a read returns covers the reader's column types.
            write(IOBuffer(), Tables.columntable(read); option...)
            close(read)
            path = write(joinpath(dir, "workload.parquet"), table; option...)
            read = Table(path)
            Tables.columns(read)
            Tables.schema(read)
            # A rewrite of a read table takes the schema-bearing writer.
            write(IOBuffer(), read; option...)
            close(read)
            read = Table(path; scan=Tables.Scan(select=(first(keys(table)),)))
            close(read)
        end
        bytes = take!(write(IOBuffer(), scalars; dictionary=true, codec=:snappy))
        read = Table(bytes; scan=Tables.Scan(select=(:i64, :str),
            filter=Tables.col(:i64) > 0, limit=2))
        Tables.columns(read)
        close(read)
    end
    # Interned schema names are per-process state; the image must start without them.
    lock(_SCHEMA_NAME_REGISTRY.lock) do
        _SCHEMA_NAME_REGISTRY.state = _SchemaNameState(Set{String}(), Int64(0))
    end
end
