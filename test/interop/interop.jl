# Interop between Parquet.jl and pyarrow, Polars, and DuckDB, with interop.py.
#
#     julia --project=test/interop test/interop/interop.jl read DIR
#         every DIR/<tool>/<case>.parquet (or dataset directory DIR/<tool>/<case>) that
#         a tool wrote must read as the values in DIR/<tool>/<case>.json
#     julia --project=test/interop test/interop/interop.jl write DIR
#         writes DIR/<case>.parquet (a dataset directory DIR/<case> for partitioned
#         cases) and the values written as DIR/<case>.json
#
# Values compare in the canonical JSON form that interop.py describes.
using Parquet, Tables, JSON, Dates, PooledArrays, DataDecimals, Durations
import DataAPI

canon(::Missing) = nothing
canon(x::Bool) = x
canon(x::Integer) = "i:$x"
canon(x::Float32) = "f32:" * string(reinterpret(UInt32, x); base=16, pad=8)
canon(x::Float64) = "f64:" * string(reinterpret(UInt64, x); base=16, pad=16)
canon(x::AbstractString) = "s:" * String(x)
canon(x::Date) = "date:$(Dates.value(x - Date(1970)))"
canon(x::Time) = "time:$(Dates.value(x))"
canon(x::DateTime) = "ts:false:$(Int128(Dates.value(x - DateTime(1970))) * 1_000_000)"
nanos(x::Durations.Timestamp{P}) where {P} = Int128(Dates.value(x)) * Dates.tons(P(1))
canon(x::Durations.Timestamp) = "ts:false:$(nanos(x))"
canon(x::Durations.ZonedTimestamp) = "ts:true:$(nanos(x.utc))"
canon(x::DataDecimals.Decimal) = "dec:$(DataDecimals.unscaled(x))e$(DataDecimals.scale(x))"
canon(x::NamedTuple) = Dict("struct" => Any[Any[String(k), canon(v)] for (k, v) in pairs(x)])
canon(x::AbstractVector{UInt8}) = "b:" * bytes2hex(collect(x))
canon(x::Union{AbstractDict,AbstractVector{<:Pair}}) =
    Dict("map" => Any[Any[canon(k), canon(v)] for (k, v) in x])
canon(x::AbstractVector) = Any[canon(v) for v in x]

function canontable(table)
    columns = Tables.columns(table)
    names = String[String(name) for name in Tables.columnnames(columns)]
    values = Dict(name => canon(Tables.getcolumn(columns, Symbol(name))) for name in names)
    # Both sides go through the same JSON parser before they are compared.
    return JSON.parse(JSON.json(Dict("names" => names, "columns" => values)))
end

# Dataset rows in `id` order, since files may be read in any order.
function sortrows(values)
    order = sortperm(values["columns"]["id"]; by=id -> parse(Int, id[3:end]))
    for (name, column) in values["columns"]
        values["columns"][name] = column[order]
    end
    return values
end

function difference(expected, actual)
    expected["names"] == actual["names"] ||
        return "columns $(actual["names"]), expected $(expected["names"])"
    for name in expected["names"]
        want, got = expected["columns"][name], actual["columns"][name]
        length(got) == length(want) ||
            return "column $name has $(length(got)) rows, expected $(length(want))"
        for (row, (w, g)) in enumerate(zip(want, got))
            w == g || return "column $name row $row is $(JSON.json(g)), expected $(JSON.json(w))"
        end
    end
    return nothing
end

# File and column metadata a tool wrote, as DataAPI reads them.
function metadatadifference(expected, table)
    found = DataAPI.metadata(table)
    all(get(found, key, nothing) == value for (key, value) in expected["metadata"]) ||
        return "metadata $found, expected $(expected["metadata"])"
    for (name, values) in expected["colmetadata"]
        found = DataAPI.colmetadata(table, Symbol(name))
        found == values || return "column $name metadata $found, expected $values"
    end
    return nothing
end

function readall(directory)
    failures = 0
    for tool in sort(readdir(directory)), file in sort(readdir(joinpath(directory, tool)))
        endswith(file, ".json") || continue
        case = joinpath(directory, tool, file[1:(end - 5)])
        problem = try
            if isdir(case)
                actual = sortrows(canontable(Parquet.Table(Parquet.Dataset(case))))
            else
                table = Parquet.Table(case * ".parquet")
                actual = canontable(table)
            end
            expected = JSON.parsefile(case * ".json")
            mismatch = difference(expected, actual)
            mismatch === nothing && haskey(expected, "metadata") ?
                metadatadifference(expected, table) : mismatch
        catch err
            sprint(showerror, err)
        end
        failures += problem !== nothing
        println(problem === nothing ? "ok   " : "FAIL ", rpad(tool, 8), basename(case),
            problem === nothing ? "" : ": " * first(problem, 500))
    end
    return failures
end

timestamp(::Type{P}, ticks) where {P} = Durations.Timestamp{P}(Dates.UTInstant(P(ticks)))
zoned(::Type{P}, ticks) where {P} =
    Durations.ZonedTimestamp{P,:UTC}(timestamp(P, ticks), Dates.UTC)
const Dec = DataDecimals.Decimal
decimal(::Type{D}, unscaled) where {D} = reinterpret(D, unscaled)

const ROWS = 0:59
const CASES = [
    "flat" => (
        i8=Union{Missing,Int8}[1, -128, 127, missing],
        i16=Union{Missing,Int16}[1, typemin(Int16), typemax(Int16), missing],
        i32=Union{Missing,Int32}[1, typemin(Int32), typemax(Int32), missing],
        i64=Union{Missing,Int64}[1, typemin(Int64), typemax(Int64), missing],
        u8=Union{Missing,UInt8}[0, typemax(UInt8), 1, missing],
        u16=Union{Missing,UInt16}[0, typemax(UInt16), 1, missing],
        u32=Union{Missing,UInt32}[0, typemax(UInt32), 1, missing],
        u64=Union{Missing,UInt64}[0, typemax(UInt64), 1, missing],
        f32=Union{Missing,Float32}[1.5, NaN32, -0.0f0, missing],
        f64=Union{Missing,Float64}[1.5, -Inf, -0.0, missing],
        b=Union{Missing,Bool}[true, false, missing, true],
        s=Union{Missing,String}["a", "héllo ✓", "", missing],
        bin=Union{Missing,Vector{UInt8}}[UInt8[0x00, 0xff], UInt8[], missing, UInt8[0x78]],
    ),
    "nested" => (
        l=Union{Missing,Vector{Union{Missing,Int64}}}[[1, 2], [], missing, [missing, 3]],
        ll=Any[[[1], Int[]], missing, [[2, missing]], Vector{Int}[]],
        st=Any[(x=1, y="a"), missing, (x=missing, y="b"), (x=3, y=missing)],
        ls=Any[[(x=1, y="p")], [], missing, [missing, (x=2, y=missing)]],
        sl=Any[(x=Int32(1), v=[1.5]), (x=missing, v=missing), missing, (x=Int32(2), v=Float64[])],
        m=Union{Missing,Vector{Pair{String,Union{Missing,Int64}}}}[
            ["a" => 1, "b" => missing], [], missing, ["c" => 3]],
    ),
    "temporal" => (
        date=Union{Missing,Date}[Date(1970), Date(1969, 12, 31), Date(2024, 2, 29), missing],
        # Whole microseconds: DuckDB reads TIME(NANOS) as microseconds.
        time=Union{Missing,Time}[Time(0), Time(23, 59, 59, 999, 999), Time(12), missing],
        dt=Union{Missing,DateTime}[DateTime(1970), DateTime(1969, 12, 31, 23, 59, 59, 999),
            DateTime(2024, 2, 29, 12, 30), missing],
        ts_us=[timestamp(Microsecond, t) for t in (0, -1, 1_700_000_000_123_456, 5)],
        ts_ns=[timestamp(Nanosecond, t) for t in (0, -1, 1_700_000_000_123_456_789, 5)],
        tz_ms=Union{Missing,Durations.ZonedTimestamp{Millisecond,:UTC}}[
            zoned(Millisecond, 0), zoned(Millisecond, -1), missing,
            zoned(Millisecond, 1_700_000_000_123)],
        tz_us=[zoned(Microsecond, t) for t in (0, -1, 1_700_000_000_123_456, 5)],
    ),
    "decimal" => (
        d9=Union{Missing,Dec{9,2,Int32}}[decimal(Dec{9,2,Int32}, Int32(v))
            for v in (123456789, -1, 0, 999999999)],
        d18=Union{Missing,Dec{18,4,Int64}}[decimal(Dec{18,4,Int64}, v)
            for v in (123456789012345678, -1, 1, 0)],
        d38=Union{Missing,Dec{38,10,Int128}}[missing;
            [decimal(Dec{38,10,Int128}, v) for v in
                (Int128(10)^37 + 12345, Int128(-1), Int128(0))]],
    ),
    "dictionary" => (
        pooled=PooledArray(repeat(["alpha", "beta", "alpha", missing, "gamma", "alpha"], 200)),
        rep=repeat(["alpha", "beta", "alpha", missing, "gamma", "alpha"], 200),
        num=repeat([1, 2, 1, missing, 3, 1], 200),
    ),
]
# Decimals with more digits than their precision, as some writers store them; the tools
# read them as stored.
const WIDE_DECIMALS = (
    d5=Union{Missing,Dec{5,2,Int32}}[decimal(Dec{5,2,Int32}, Int32(v))
        for v in (typemax(Int32), -100_000, 1)],
    d10=[decimal(Dec{10,2,Int64}, v) for v in (typemax(Int64), typemin(Int64), 0)],
    d20=[decimal(Dec{20,0,Int128}, v) for v in (Int128(10)^20 + 5, -Int128(10)^20, 1)],
)
const PARTITIONED = (
    id=collect(ROWS),
    k=[i % 2 for i in ROWS],
    g=[("a", "b c", "d")[i % 3 + 1] for i in ROWS],
    v=[i % 5 == 0 ? missing : i * 1.25 for i in ROWS],
    s=[i % 4 == 0 ? missing : "row $i" for i in ROWS],
)
const KEYS = [:k, :g]

function writeall(directory)
    mkpath(directory)
    save(name, values) = open(io -> JSON.json(io, values), joinpath(directory, name * ".json"), "w")
    for (name, table) in CASES
        Parquet.write(joinpath(directory, name * ".parquet"), table)
        save(name, canontable(table))
        # Data page V2, zstd, and several row groups and pages.
        Parquet.write(joinpath(directory, name * "_v2.parquet"), table; pageversion=:v2,
            compress=:zstd, rowgroupsize=3, pagesize=64)
        save(name * "_v2", canontable(table))
    end
    # File and column metadata, which pyarrow reads from the ARROW:schema.
    Parquet.write(joinpath(directory, "metadata.parquet"), CASES[1][2];
        metadata=["owner" => "interop"], colmetadata=Dict(:i8 => ["unit" => "count"]))
    save("metadata", merge(canontable(CASES[1][2]), Dict("metadata" => Dict("owner" =>
        "interop"), "colmetadata" => Dict("i8" => Dict("unit" => "count")))))
    # Read and written again (compaction), such decimals stay as stored.
    source = tempname()
    Parquet.write(source, WIDE_DECIMALS)
    Parquet.write(joinpath(directory, "decimal_rewrite.parquet"), Parquet.Table(source))
    save("decimal_rewrite", canontable(WIDE_DECIMALS))
    Parquet.write(joinpath(directory, "partitioned"), PARTITIONED; partitionby=KEYS)
    names = [filter(!in(KEYS), collect(keys(PARTITIONED))); KEYS]
    save("partitioned", canontable(NamedTuple{Tuple(names)}(PARTITIONED)))
    println("Parquet.jl wrote $(length(CASES) * 2 + 3) cases")
    return 0
end

command, directory = ARGS
exit(command == "write" ? writeall(directory) : readall(directory) == 0 ? 0 : 1)
