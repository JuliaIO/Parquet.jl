using Dates
using Tables

# ARROW:schema values written by pyarrow 25.0.1 and Polars 1.43.2. pyarrow's schema:
# ts_ny timestamp[us, America/New_York], ts_off timestamp[ms, +05:30], ts_utc
# timestamp[ns, UTC], ts_naive timestamp[s], dur_s/ms/us/ns duration, big large_string,
# dict dictionary<string>, st struct<t timestamp[us, Europe/Paris], d duration[ms]>,
# lst list<timestamp[ms, Asia/Tokyo]>. Polars': ts datetime[us, America/New_York],
# d duration[ms], naive datetime[ns].
const ARROW_SCHEMA_PYARROW = "/////9ADAAAQAAAAAAAKAA4ABgAFAAgACgAAAAABBAAQAAAAAAAKAAwAAAAEAAgACgAAADwAAAAEAAAAAQAAAAwAAAAIAAwABAAIAAgAAAAUAAAABAAAAAcAAABweWFycm93AAYAAABvcmlnaW4AAAwAAAAQAwAAwAIAAIQCAABQAgAAHAIAAPABAADAAQAAkAEAAGgBAAAUAQAAcAAAAAQAAAAw/f//AAABDBQAAAAYAAAABAAAAAEAAAAQAAAAAwAAAGxzdADQ/f//WP3//wAAAQoQAAAAGAAAAAQAAAAAAAAABAAAAGl0ZW0AAAAASP3//wAAAQAEAAAACgAAAEFzaWEvVG9reW8AAJj9//8AAAENGAAAABwAAAAEAAAAAgAAADgAAAAQAAAAAgAAAHN0AAA8/v//xP3//wAAARIQAAAAFAAAAAQAAAAAAAAAAQAAAGQAAABg/v//6P3//wAAAQoQAAAAFAAAAAQAAAAAAAAAAQAAAHQAAADU/f//AAACAAQAAAAMAAAARXVyb3BlL1BhcmlzAAAAABAAGAAIAAYABwAMABAAFAAQAAAAAAABBRQAAABAAAAAIAAAAAQAAAAAAAAABAAAAGRpY3QAAAAACAAIAAAABAAIAAAADAAAAAgADAAIAAcACAAAAAAAAAEgAAAAAP///4j+//8AAAEUEAAAABQAAAAEAAAAAAAAAAMAAABiaWcAJP///6z+//8AAAESEAAAABgAAAAEAAAAAAAAAAYAAABkdXJfbnMAAIL///8AAAMA2P7//wAAARIQAAAAGAAAAAQAAAAAAAAABgAAAGR1cl91cwAArv///wAAAgAE////AAABEhAAAAAYAAAABAAAAAAAAAAGAAAAZHVyX21zAACk////LP///wAAARIQAAAAHAAAAAQAAAAAAAAABQAAAGR1cl9zAAYACAAGAAYAAAAAAAAAXP///wAAAQoQAAAAIAAAAAQAAAAAAAAACAAAAHRzX25haXZlAAAAAAQABAAEAAAAjP///wAAAQoQAAAAGAAAAAQAAAAAAAAABgAAAHRzX3V0YwAAfP///wAAAwAEAAAAAwAAAFVUQwDE////AAABChAAAAAYAAAABAAAAAAAAAAGAAAAdHNfb2ZmAAC0////AAABAAQAAAAGAAAAKzA1OjMwAAAQABQACAAGAAcADAAAABAAEAAAAAAAAQoQAAAAIAAAAAQAAAAAAAAABQAAAHRzX255AAAACAAMAAYACAAIAAAAAAACAAQAAAAQAAAAQW1lcmljYS9OZXdfWW9yawAAAAAAAAAA"
const ARROW_SCHEMA_POLARS = "/////+8AAAAEAAAA8v///xQAAAAEAAEAAAAKAAsACAAKAAQA+P///wwAAAAIAAgAAAAEAAMAAABkAAAAOAAAAAQAAACU////IAAAABAAAAAIAAAAAQoAAAAAAAD6////AwAGAAYABAAFAAAAbmFpdmUAAADE////HAAAABAAAAAIAAAAARIAAAAAAAD8////BAAEAAEAAABkAAAA7P///1AAAAAgAAAAGAAAAAEKAAAQABIABAAQABEACAAAAAwAAAAAAPT///8QAAAAAgAAAAgACgAIAAQAEAAAAEFtZXJpY2EvTmV3X1lvcmsAAAAAAgAAAHRzAA=="

DS = Parquet.Durations

# `bytes` with its key-value metadata replaced by an ARROW:schema of `text`.
function witharrowschema(bytes::Vector{UInt8}, text)
    metadata = Parquet.Thrift.decode(copy(Parquet.readfooter(Parquet.source(bytes)).bytes),
        Parquet.Metadata.FileMetaData)
    return tablerewritefooter(bytes, tablereplace(metadata; key_value_metadata=
        [Parquet.Metadata.KeyValue(key="ARROW:schema", value=text)]))
end

zoned(P, ticks) = DS.ZonedTimestamp{P,:UTC}(DS.Timestamp{P}(Dates.UTInstant(P(ticks))), Dates.UTC)

@testset "ARROW:schema decoding" begin
    fields = Parquet._decodearrowschema(ARROW_SCHEMA_PYARROW, Parquet.Limits())
    @test [field.name for field in fields] == ["ts_ny", "ts_off", "ts_utc", "ts_naive",
        "dur_s", "dur_ms", "dur_us", "dur_ns", "big", "dict", "st", "lst"]
    @test fields[1].type == Parquet._ARROW_TIMESTAMP && fields[1].unit == 2 &&
        fields[1].timezone == "America/New_York"
    @test fields[2].timezone == "+05:30" && fields[4].timezone === nothing
    @test [field.type for field in fields[5:8]] == fill(Parquet._ARROW_DURATION, 4)
    @test [field.unit for field in fields[5:8]] == 0:3
    @test [child.name for child in fields[11].children] == ["t", "d"]
    @test fields[11].children[1].timezone == "Europe/Paris"
    @test only(fields[12].children).timezone == "Asia/Tokyo"
    @test all(field -> field.nullable, fields)

    for text in ("not base64!", ARROW_SCHEMA_PYARROW[1:40], "AAAA",
            Parquet.Base64.base64encode(UInt8[0xff, 0xff, 0xff, 0xff, 0x10, 0x00, 0x00, 0x00]))
        @test_throws Parquet.FormatError Parquet._decodearrowschema(text, Parquet.Limits())
    end
end

@testset "ARROW:schema read types" begin
    # A Julia file shaped like Polars' (UTC timestamps, an INT64 count, a local
    # timestamp) reads with the zone and duration unit Polars recorded.
    bytes = Parquet._encodefile((ts=[zoned(Microsecond, 1_700_000_000_000_000)],
        d=Int64[1500], naive=[DS.Timestamp{Nanosecond}(Dates.UTInstant(Nanosecond(1)))]))
    table = Parquet.Table(witharrowschema(bytes, ARROW_SCHEMA_POLARS))
    @test eltype(table.ts) == DS.ZonedTimestamp{Microsecond,Symbol("America/New_York")}
    @test table.ts[1].utc == zoned(Microsecond, 1_700_000_000_000_000).utc
    @test table.d == [Millisecond(1500)]
    @test eltype(table.naive) == DS.Timestamp{Nanosecond}
    # Without the ARROW:schema, or with one that does not match, the Parquet types stand.
    for text in ("not base64!", ARROW_SCHEMA_PYARROW)
        plain = Parquet.Table(witharrowschema(bytes, text))
        @test eltype(plain.ts) == DS.ZonedTimestamp{Microsecond,:UTC}
        @test plain.d == [1500]
    end

    # Nested fields: struct children, list elements, and map values, from a schema this
    # package generates; pyarrow 25.0.1 and Polars 1.43.2 read these schemas too.
    nested = Parquet._encodefile((
        st=[(t=zoned(Microsecond, 7), d=Int64(2))],
        lst=[[zoned(Millisecond, 3), zoned(Millisecond, 4)]],
        m=[Dict("k" => Int64(9))]))
    schema = Parquet.Schema(Parquet.File(nested).metadata)
    hints = Dict{Vector{String},Any}(["st", "t"] => "Europe/Paris", ["st", "d"] => Int16(1),
        ["lst", "list", "element"] => "Asia/Tokyo", ["m", "key_value", "value"] => Int16(3))
    text = Parquet._arrowschema(schema; hints, metadata=["origin" => "julia"])
    fields = Parquet._decodearrowschema(text, Parquet.Limits())
    @test [field.name for field in fields] == ["st", "lst", "m"]
    @test Parquet._arrowhints(Parquet.File(witharrowschema(nested, text)).metadata, schema,
        Parquet.Limits()) == hints
    table = Parquet.Table(witharrowschema(nested, text))
    @test table.st[1]["t"] isa DS.ZonedTimestamp{Microsecond,Symbol("Europe/Paris")}
    @test table.st[1]["d"] == Millisecond(2)
    @test eltype(table.lst[1]) == DS.ZonedTimestamp{Millisecond,Symbol("Asia/Tokyo")}
    @test last(only(table.m[1])) == Nanosecond(9)

    # A table read with zones and durations writes back with its stored types.
    rewritten = Parquet.Table(take!(Parquet.write(IOBuffer(), table)))
    @test rewritten.st[1]["t"] == zoned(Microsecond, 7)
    @test last(only(rewritten.m[1])) == 9
end
