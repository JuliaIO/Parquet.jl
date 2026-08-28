using Random

if !@isdefined(TH)
    const TH = Parquet.Thrift
end
if !@isdefined(MD)
    const MD = Parquet.Metadata
end

const CORPUS_DIR = get(ENV, "PARQUET_TESTING_DIR", joinpath(@__DIR__, "parquet-testing"))

function corpuspath(parts...)
    return joinpath(CORPUS_DIR, parts...)
end

function metabytes(f)
    w = TH.Writer()
    f(w)
    return w.buffer
end

function footermetadata(path)
    file = Parquet.File(path)
    bytes = copy(file.footer.bytes)
    encrypted = file.footer.encrypted
    close(file)
    r = TH.Reader(bytes)
    value = TH.decode(r, encrypted ? MD.FileCryptoMetaData : MD.FileMetaData)
    return value, bytes, TH.consumed(r)
end

function pageheaders(path, meta)
    bytes = read(path)
    headers = MD.PageHeader[]
    for rowgroup in meta.row_groups, chunk in rowgroup.columns
        md = chunk.meta_data
        start = md.data_page_offset
        if md.dictionary_page_offset !== nothing && md.dictionary_page_offset > 0
            start = min(start, md.dictionary_page_offset)
        end
        stop = start + md.total_compressed_size
        pos = start
        while pos < stop
            r = TH.Reader(bytes, pos + 1, length(bytes))
            header = TH.decode(r, MD.PageHeader)
            push!(headers, header)
            pos += TH.consumed(r) + header.compressed_page_size
        end
        pos == stop || error("page walk overshoot in $path")
    end
    return headers
end

function corpusfiles(parts...)
    dir = corpuspath(parts...)
    return [joinpath(dir, f) for f in sort(readdir(dir)) if endswith(f, ".parquet") || endswith(f, ".encrypted")]
end

@testset "generated metadata structs" begin
    kv = MD.KeyValue(key="a")
    @test kv.value === nothing && kv.unknown_fields == TH.RawField[]
    @test kv == MD.KeyValue(key="a") && hash(kv) == hash(MD.KeyValue(key="a"))
    @test kv != MD.KeyValue(key="a", value="b")
    @test_throws UndefKeywordError MD.KeyValue()
    @test MD.ColumnChunk().file_offset == 0
    @test hasfield(MD.SchemaElement, :type_) && !hasfield(MD.SchemaElement, :type)
    @test MD.CompressionCodec.LZO.value == 3 && MD.Encoding.BYTE_STREAM_SPLIT.value == 9
    @test MD.Type.INT96.value == 3 && TH.name(MD.Type.T(3)) === :INT96
    @test sprint(show, MD.Encoding.RLE_DICTIONARY) == "Encoding.RLE_DICTIONARY"
    @test sprint(show, MD.Encoding.T(10)) == "Encoding.T(10)"
    @test MD.LogicalType(STRING=MD.StringType()).STRING !== nothing
    @test_throws ArgumentError MD.LogicalType(STRING=MD.StringType(), MAP=MD.MapType())
    @test_throws ArgumentError MD.ColumnOrder(TYPE_ORDER=MD.TypeDefinedOrder(), unknown_fields=(TH.RawField(3, TH.STRUCT, UInt8[0x00]),))
    schema = [MD.SchemaElement(name="root", num_children=Int32(2)),
        MD.SchemaElement(name="id", type_=MD.Type.INT64, repetition_type=MD.FieldRepetitionType.REQUIRED,
            logicalType=MD.LogicalType(INTEGER=MD.IntType(bitWidth=Int8(64), isSigned=true))),
        MD.SchemaElement(name="ts", type_=MD.Type.INT64, repetition_type=MD.FieldRepetitionType.OPTIONAL,
            converted_type=MD.ConvertedType.TIMESTAMP_MICROS,
            logicalType=MD.LogicalType(TIMESTAMP=MD.TimestampType(isAdjustedToUTC=true, unit=MD.TimeUnit(MICROS=MD.MicroSeconds()))))]
    stats = MD.Statistics(null_count=Int64(0), min_value=UInt8[1, 0, 0, 0, 0, 0, 0, 0],
        max_value=UInt8[9, 0, 0, 0, 0, 0, 0, 0], is_max_value_exact=true)
    column = MD.ColumnMetaData(type_=MD.Type.INT64, encodings=[MD.Encoding.PLAIN, MD.Encoding.RLE],
        path_in_schema=["id"], codec=MD.CompressionCodec.ZSTD, num_values=Int64(3), total_uncompressed_size=Int64(40),
        total_compressed_size=Int64(30), data_page_offset=Int64(4), statistics=stats,
        encoding_stats=[MD.PageEncodingStats(page_type=MD.PageType.DATA_PAGE, encoding=MD.Encoding.PLAIN, count=Int32(1))],
        size_statistics=MD.SizeStatistics(definition_level_histogram=Int64[0, 3]))
    rowgroup = MD.RowGroup(columns=[MD.ColumnChunk(meta_data=column)], total_byte_size=Int64(40), num_rows=Int64(3),
        sorting_columns=[MD.SortingColumn(column_idx=Int32(0), descending=false, nulls_first=true)], ordinal=Int16(0))
    meta = MD.FileMetaData(version=Int32(1), schema=schema, num_rows=Int64(3), row_groups=[rowgroup],
        key_value_metadata=[MD.KeyValue(key="k", value="v"), MD.KeyValue(key="novalue")], created_by="Parquet.jl test",
        column_orders=[MD.ColumnOrder(TYPE_ORDER=MD.TypeDefinedOrder()), MD.ColumnOrder(IEEE_754_TOTAL_ORDER=MD.IEEE754TotalOrder())])
    bytes = TH.encode(meta)
    decoded = TH.decode(bytes, MD.FileMetaData)
    @test isequal(decoded, meta) && decoded == meta && hash(decoded) == hash(meta)
    @test TH.encode(decoded) == bytes
    @test decoded.schema[3].logicalType.TIMESTAMP.unit.MICROS !== nothing
    @test decoded.row_groups[1].columns[1].meta_data.statistics.is_max_value_exact === true
    @test decoded.row_groups[1].columns[1].meta_data.statistics.is_min_value_exact === nothing
    @test decoded.row_groups[1].columns[1].meta_data.statistics.nan_count === nothing
    @test decoded.row_groups[1].sorting_columns[1].nulls_first === true
    @test decoded.key_value_metadata[2].value === nothing
    v2 = MD.DataPageHeaderV2(num_values=Int32(1), num_nulls=Int32(0), num_rows=Int32(1), encoding=MD.Encoding.PLAIN,
        definition_levels_byte_length=Int32(0), repetition_levels_byte_length=Int32(0), is_compressed=false)
    @test TH.encode(v2)[(end - 1):end] == UInt8[0x12, 0x00]
    @test TH.decode(TH.encode(v2), MD.DataPageHeaderV2).is_compressed === false
    absent = MD.DataPageHeaderV2(num_values=Int32(1), num_nulls=Int32(0), num_rows=Int32(1), encoding=MD.Encoding.PLAIN,
        definition_levels_byte_length=Int32(0), repetition_levels_byte_length=Int32(0))
    @test TH.decode(TH.encode(absent), MD.DataPageHeaderV2).is_compressed === nothing
    src = Parquet.source(bytes)
    @test TH.decode(Parquet.readrange(src, 0, length(bytes)), MD.FileMetaData) == meta
    Parquet.close!(src)
end

@testset "unknown fields are preserved and re-emitted" begin
    bytes = metabytes() do w
        lastid = TH.writefieldheader!(w, Int16(0), Int16(1), TH.BINARY)
        TH.writestring!(w, "k")
        lastid = TH.writefieldheader!(w, lastid, Int16(2), TH.BINARY)
        TH.writestring!(w, "v")
        lastid = TH.writefieldheader!(w, lastid, Int16(3), TH.I32)
        TH.writei32!(w, Int32(42))
        lastid = TH.writefieldheader!(w, lastid, Int16(4), TH.BOOL_TRUE)
        lastid = TH.writefieldheader!(w, lastid, Int16(5), TH.LIST)
        TH.writelist!(w, [MD.KeyValue(key="nested")])
        lastid = TH.writefieldheader!(w, lastid, Int16(32767), TH.BINARY)
        TH.writebinary!(w, UInt8[0xde, 0xad])
        TH.writestop!(w)
    end
    kv = TH.decode(bytes, MD.KeyValue)
    @test kv.key == "k" && kv.value == "v"
    @test [f.id for f in kv.unknown_fields] == Int16[3, 4, 5, 32767]
    @test [f.type for f in kv.unknown_fields] == UInt8[TH.I32, TH.BOOL_TRUE, TH.LIST, TH.BINARY]
    @test TH.payload(kv.unknown_fields[4]) == UInt8[0x02, 0xde, 0xad]
    @test kv.unknown_fields[4].bytes[1:4] == UInt8[0x08, 0xfe, 0xff, 0x03]
    @test kv.unknown_fields[4].headerlength == 4 && kv.unknown_fields[4].previd == 5
    @test TH.encode(kv) == bytes
    leading = metabytes() do w
        lastid = TH.writefieldheader!(w, Int16(0), Int16(9), TH.I64)
        TH.writei64!(w, Int64(-1))
        lastid = TH.writefieldheader!(w, lastid, Int16(1), TH.BINARY)
        TH.writestring!(w, "k")
        TH.writestop!(w)
    end
    lead = TH.decode(leading, MD.KeyValue)
    @test lead.unknown_fields[1].previd == 0 && lead.key == "k"
    @test TH.encode(lead) == leading
    edited = MD.KeyValue(key="kk", value=nothing, unknown_fields=kv.unknown_fields)
    reread = TH.decode(TH.encode(edited), MD.KeyValue)
    @test reread.key == "kk" && reread.value === nothing
    @test reread.unknown_fields == kv.unknown_fields
    manual = MD.KeyValue(key="k", unknown_fields=(TH.RawField(32767, TH.BINARY, UInt8[0x01, 0xaa]),))
    @test TH.encode(manual) == UInt8[0x18, 0x01, 0x6b, 0x08, 0xfe, 0xff, 0x03, 0x01, 0xaa, 0x00]
    @test TH.decode(TH.encode(manual), MD.KeyValue).unknown_fields == manual.unknown_fields
    mismatched = metabytes() do w
        lastid = TH.writefieldheader!(w, Int16(0), Int16(1), TH.I32)
        TH.writei32!(w, Int32(7))
        lastid = TH.writefieldheader!(w, lastid, Int16(2), TH.I64)
        TH.writei64!(w, Int64(0))
        TH.writestop!(w)
    end
    chunk = TH.decode(mismatched, MD.ColumnChunk)
    @test chunk.file_path === nothing && chunk.unknown_fields[1].id == 1 && chunk.unknown_fields[1].type == TH.I32
    @test TH.encode(chunk) == mismatched
    wrongkey = metabytes() do w
        TH.writefieldheader!(w, Int16(0), Int16(1), TH.I32)
        TH.writei32!(w, Int32(1))
        TH.writestop!(w)
    end
    @test_throws Parquet.FormatError TH.decode(wrongkey, MD.KeyValue)
    listbytes = metabytes() do w
        TH.writefieldheader!(w, Int16(0), Int16(2), TH.LIST)
        TH.writelist!(w, ["x"])
        TH.writestop!(w)
    end
    sizes = TH.decode(listbytes, MD.SizeStatistics)
    @test sizes.repetition_level_histogram === nothing && sizes.unknown_fields[1].id == 2 && sizes.unknown_fields[1].type == TH.LIST
    @test TH.encode(sizes) == listbytes
    order = TH.decode(UInt8[0x3c, 0x00, 0x00], MD.ColumnOrder)
    @test order.TYPE_ORDER === nothing && order.IEEE_754_TOTAL_ORDER === nothing && order.unknown_fields[1].id == 3
    @test TH.encode(order) == UInt8[0x3c, 0x00, 0x00]
    @test_throws Parquet.FormatError TH.decode(UInt8[0x1c, 0x00, 0x2c, 0x00, 0x00], MD.ColumnOrder)
    @test_throws Parquet.FormatError TH.decode(UInt8[0x1c, 0x00, 0x1c, 0x00, 0x00], MD.LogicalType)
end

@testset "many unknown fields decode to a Vector, not a per-length tuple type" begin
    # Regression: unknown_fields was Tuple{Vararg{RawField}}, so a footer with
    # thousands of unknown fields minted a fresh NTuple{N} type whose recursive
    # ==/isequal/hash forced seconds of compilation per distinct N.
    bytes = metabytes() do w
        lastid = TH.writefieldheader!(w, Int16(0), Int16(1), TH.BINARY)
        TH.writestring!(w, "k")
        for id in Int16(100):Int16(1599)
            lastid = TH.writefieldheader!(w, lastid, id, TH.I32)
            TH.writei32!(w, Int32(id))
        end
        TH.writestop!(w)
    end
    kv = TH.decode(bytes, MD.KeyValue)
    @test kv.unknown_fields isa Vector{TH.RawField}
    @test length(kv.unknown_fields) == 1500
    other = TH.decode(bytes, MD.KeyValue)
    @test kv == other && isequal(kv, other) && hash(kv) == hash(other)
    @test TH.encode(kv) == bytes
end

@testset "empty unknown-field vectors are budgeted" begin
    bytes = TH.encode(MD.StringType())
    vectorcharge = Parquet._materializedarraybytes(TH.RawField, 0)
    exact = Parquet._materializedsum(Parquet._MATERIALIZED_OBJECT_BYTES,
        vectorcharge)
    @test_throws Parquet.LimitError TH.decode(bytes, MD.StringType;
        limits=Parquet.Limits(max_materialized_bytes=exact - 1))
    limits = Parquet.Limits(max_materialized_bytes=exact)
    budget = Parquet._LiveByteBudget(limits)
    value = TH.decode(bytes, MD.StringType; limits=limits, budget=budget)
    @test isempty(value.unknown_fields)
    @test Parquet._budgetused(budget) == exact
end

@testset "parquet-testing corpus footers" begin
    if !isdir(corpuspath("data"))
        @warn "parquet-testing corpus not found; skipping corpus tests" CORPUS_DIR
    else
        files = corpusfiles("data")
        @test length(files) >= 70
        for path in vcat(files, corpusfiles("data", "aes256"), corpusfiles("data", "geospatial"),
                corpusfiles("shredded_variant"), corpusfiles("bad_data", "variants"))
            meta, bytes, used = footermetadata(path)
            @test isequal(TH.decode(TH.encode(meta), typeof(meta)), meta)
            @test TH.encode(meta) == bytes[1:used]
        end
        plain, _, _ = footermetadata(corpuspath("data", "alltypes_plain.parquet"))
        @test plain.num_rows == 8 && length(plain.schema) == 12 && length(plain.row_groups) == 1
        @test startswith(plain.created_by, "impala version 1.3.0")
        @test plain.schema[1].num_children == 11 && plain.schema[2].type_ == MD.Type.INT32
        headers = pageheaders(corpuspath("data", "alltypes_plain.parquet"), plain)
        @test length(headers) == 21
        @test any(h -> h.dictionary_page_header !== nothing, headers) && all(h -> h.crc === nothing, headers)
        v2, _, _ = footermetadata(corpuspath("data", "datapage_v2.snappy.parquet"))
        v2headers = pageheaders(corpuspath("data", "datapage_v2.snappy.parquet"), v2)
        @test length(v2headers) == 8 && any(h -> h.data_page_header_v2 !== nothing, v2headers)
        v2header = first(h.data_page_header_v2 for h in v2headers if h.data_page_header_v2 !== nothing)
        @test v2header.num_values > 0 && v2header.num_nulls >= 0 && v2header.definition_levels_byte_length >= 0
        crc, _, _ = footermetadata(corpuspath("data", "datapage_v1-corrupt-checksum.parquet"))
        crcheaders = pageheaders(corpuspath("data", "datapage_v1-corrupt-checksum.parquet"), crc)
        @test length(crcheaders) == 4 && all(h -> h.crc !== nothing, crcheaders)
        overflow, _, _ = footermetadata(corpuspath("data", "overflow_i16_page_cnt.parquet"))
        @test overflow.num_rows == 40000
        @test length(pageheaders(corpuspath("data", "overflow_i16_page_cnt.parquet"), overflow)) == 40000
        tiny, _, _ = footermetadata(corpuspath("data", "alltypes_tiny_pages.parquet"))
        @test length(pageheaders(corpuspath("data", "alltypes_tiny_pages.parquet"), tiny)) == 5805
        tinybytes = read(corpuspath("data", "alltypes_tiny_pages.parquet"))
        chunk = tiny.row_groups[1].columns[1]
        ci = TH.decode(TH.Reader(tinybytes, chunk.column_index_offset + 1, chunk.column_index_offset + chunk.column_index_length), MD.ColumnIndex)
        @test length(ci.null_pages) == 325 && !any(ci.null_pages) && ci.boundary_order == MD.BoundaryOrder.UNORDERED
        @test length(ci.null_counts) == 325 && length(ci.min_values) == 325
        oi = TH.decode(TH.Reader(tinybytes, chunk.offset_index_offset + 1, chunk.offset_index_offset + chunk.offset_index_length), MD.OffsetIndex)
        @test length(oi.page_locations) == 325
        @test oi.page_locations[1] == MD.PageLocation(offset=Int64(4), compressed_page_size=Int32(109), first_row_index=Int64(0))
        nation, _, _ = footermetadata(corpuspath("data", "nation.dict-malformed.parquet"))
        @test nation.num_rows == 25 && nation.created_by == "parquet-mr" && nation.column_orders === nothing
        @test isempty(nation.row_groups[1].columns[1].meta_data.encodings)
        dictzero, _, _ = footermetadata(corpuspath("data", "dict-page-offset-zero.parquet"))
        @test dictzero.row_groups[1].columns[1].meta_data.dictionary_page_offset == 0
        unknownlt, _, _ = footermetadata(corpuspath("data", "unknown-logical-type.parquet"))
        lt = unknownlt.schema[3].logicalType
        @test lt.STRING === nothing && length(lt.unknown_fields) == 1
        @test lt.unknown_fields[1].id == 2555 && lt.unknown_fields[1].type == TH.STRUCT
        @test unknownlt.schema[2].logicalType.STRING !== nothing
        int96, _, _ = footermetadata(corpuspath("data", "int96_timestamp_order.parquet"))
        @test int96.column_orders[1].TYPE_ORDER === nothing && int96.column_orders[1].unknown_fields[1].id == 3
        @test int96.row_groups[1].columns[1].meta_data.encodings == [MD.Encoding.PLAIN_DICTIONARY, MD.Encoding.BIT_PACKED]
        alp, _, _ = footermetadata(corpuspath("data", "alp_extended.zstd.parquet"))
        encodings = unique(e for rg in alp.row_groups for c in rg.columns for e in c.meta_data.encodings)
        @test MD.Encoding.T(10) in encodings && TH.name(MD.Encoding.T(10)) === nothing
        bloom = read(corpuspath("data", "bloom_filter.xxhash.bin"))
        r = TH.Reader(bloom)
        header = TH.decode(r, MD.BloomFilterHeader)
        @test header.numBytes == 1024 && TH.consumed(r) == 16 && length(bloom) == 16 + 1024
        @test header.algorithm.BLOCK !== nothing && header.hash.XXHASH !== nothing && header.compression.UNCOMPRESSED !== nothing
        crypto, _, cused = footermetadata(corpuspath("data", "encrypt_columns_and_footer.parquet.encrypted"))
        @test crypto isa MD.FileCryptoMetaData && crypto.key_metadata == b"kf" && cused == 20
        @test crypto.encryption_algorithm.AES_GCM_V1 !== nothing && crypto.encryption_algorithm.AES_GCM_CTR_V1 === nothing
        ctr, _, _ = footermetadata(corpuspath("data", "encrypt_columns_and_footer_ctr.parquet.encrypted"))
        @test ctr.encryption_algorithm.AES_GCM_CTR_V1 !== nothing
        plainfooter, pbytes, pused = footermetadata(corpuspath("data", "encrypt_columns_plaintext_footer.parquet.encrypted"))
        @test plainfooter isa MD.FileMetaData && plainfooter.footer_signing_key_metadata == b"kf"
        @test plainfooter.encryption_algorithm !== nothing && pused == length(pbytes) - 28
        columns = plainfooter.row_groups[1].columns
        @test columns[5].crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY.key_metadata == b"kc2"
        @test columns[6].crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY.key_metadata == b"kc1"
        @test all(c -> c.crypto_metadata === nothing, columns[1:4]) && columns[5].encrypted_column_metadata !== nothing
        geo, _, _ = footermetadata(corpuspath("data", "geospatial", "crs-default.parquet"))
        geocol = first(c for c in geo.row_groups[1].columns if c.meta_data.geospatial_statistics !== nothing)
        @test geocol.meta_data.geospatial_statistics.bbox == MD.BoundingBox(xmin=-111.0, xmax=-104.0, ymin=41.0, ymax=45.0)
        @test geocol.meta_data.geospatial_statistics.geospatial_types == Int32[3]
        @test any(se -> se.logicalType !== nothing && se.logicalType.GEOMETRY !== nothing && se.logicalType.GEOMETRY.crs === nothing, geo.schema)
        geog, _, _ = footermetadata(corpuspath("data", "geospatial", "geography-points.parquet"))
        @test any(se -> se.logicalType !== nothing && se.logicalType.GEOGRAPHY !== nothing &&
            se.logicalType.GEOGRAPHY.algorithm == MD.EdgeInterpolationAlgorithm.SPHERICAL, geog.schema)
        variant, _, _ = footermetadata(corpuspath("shredded_variant", "case-001.parquet"))
        @test any(se -> se.logicalType !== nothing && se.logicalType.VARIANT !== nothing &&
            se.logicalType.VARIANT.specification_version == 1, variant.schema)
        @test_throws Parquet.FormatError footermetadata(corpuspath("bad_data", "ARROW-GH-41317.parquet"))
        corrupt, _, _ = footermetadata(corpuspath("bad_data", "PARQUET-1481.parquet"))
        @test corrupt.schema[2].type_ == MD.Type.T(-7) && TH.name(corrupt.schema[2].type_) === nothing
        for name in ("ARROW-GH-41321.parquet", "ARROW-GH-43605.parquet", "ARROW-GH-45185.parquet",
                "ARROW-RS-GH-6229-DICTHEADER.parquet", "ARROW-RS-GH-6229-LEVELS.parquet")
            meta, bytes, used = footermetadata(corpuspath("bad_data", name))
            @test TH.encode(meta) == bytes[1:used]
        end
        plainfile = Parquet.File(corpuspath("data", "alltypes_plain.parquet"))
        @test TH.decode(plainfile.footer.bytes, MD.FileMetaData) == plain
        @test_throws Parquet.LimitError TH.decode(plainfile.footer.bytes, MD.FileMetaData; limits=Parquet.Limits(max_metadata_depth=3))
        @test_throws Parquet.LimitError TH.decode(plainfile.footer.bytes, MD.FileMetaData; limits=Parquet.Limits(max_container_elements=5))
        @test_throws Parquet.LimitError TH.decode(plainfile.footer.bytes, MD.FileMetaData; limits=Parquet.Limits(max_string_bytes=8))
        close(plainfile)
    end
end

@testset "seeded metadata mutations fail safely" begin
    if !isfile(corpuspath("data", "alltypes_plain.parquet"))
        @warn "parquet-testing corpus not found; skipping mutation tests" CORPUS_DIR
    else
        file = Parquet.File(corpuspath("data", "alltypes_plain.parquet"))
        seed = copy(file.footer.bytes)
        close(file)
        limits = Parquet.Limits(max_string_bytes=1024 * 1024, max_container_elements=100_000,
            max_metadata_depth=64)
        rng = Random.Xoshiro(0x50415251554554)
        for _ in 1:5_000
            bytes = copy(seed)
            operation = rand(rng, 1:4)
            if operation == 1
                index = rand(rng, eachindex(bytes))
                bytes[index] = xor(bytes[index], UInt8(1) << rand(rng, 0:7))
            elseif operation == 2
                resize!(bytes, rand(rng, 0:length(bytes)))
            elseif operation == 3
                insert!(bytes, rand(rng, 1:(length(bytes) + 1)), rand(rng, UInt8))
            else
                first = rand(rng, eachindex(bytes))
                last = min(length(bytes), first + rand(rng, 0:7))
                rand!(rng, view(bytes, first:last))
            end
            try
                value = TH.decode(bytes, MD.FileMetaData; limits=limits)
                encoded = TH.encode(value)
                @test TH.decode(encoded, MD.FileMetaData; limits=limits) == value
            catch err
                @test err isa Union{Parquet.FormatError, Parquet.LimitError}
            end
        end
    end
end
