function dictionarypage(values; width=nothing, encoding=MD.Encoding.PLAIN, crc=:valid,
    count=length(values), extra=UInt8[])
    payload = vcat(columnplain(values; width=width), extra)
    header = MD.DictionaryPageHeader(num_values=Int32(count), encoding=encoding)
    return columnpage(payload; type=MD.PageType.DICTIONARY_PAGE, dict=header, crc=crc)
end

function dictionarydatapage(indices; levels=nothing, maxlevel=0,
    bitwidth=Parquet._dictionarybitwidth(isempty(indices) ? 0 : Int(maximum(indices)) + 1),
    encoding=MD.Encoding.RLE_DICTIONARY, extra=UInt8[], omit_indices=false, crc=:valid)
    count = levels === nothing ? length(indices) : length(levels)
    payload = levels === nothing ? UInt8[] : columnlevels(levels, maxlevel)
    if !omit_indices
        push!(payload, UInt8(bitwidth))
        append!(payload, Parquet.encode_hybrid(UInt64.(indices), bitwidth))
    end
    append!(payload, extra)
    return columnpage(payload; v1=columnv1(count; encoding=encoding), crc=crc)
end

function readdictionarypages(dictionary, data, leaf; num_values, data_page_offset=nothing,
    dictionary_page_offset=Int64(4), kwargs...)
    dataoffset = something(data_page_offset, 4 + length(dictionary))
    pages = data isa Vector{UInt8} ? [dictionary, data] : vcat([dictionary], data)
    return readsynthetic(pages, leaf; num_values=num_values, data_page_offset=dataoffset,
        dictionary_page_offset=dictionary_page_offset, kwargs...)
end

@testset "dictionary primitives" begin
    @test Parquet._isdictionaryencoding(MD.Encoding.PLAIN_DICTIONARY)
    @test Parquet._isdictionaryencoding(MD.Encoding.RLE_DICTIONARY)
    @test !Parquet._isdictionaryencoding(MD.Encoding.PLAIN)
    @test [Parquet._dictionarybitwidth(count) for count in 0:9] == [0, 0, 1, 2, 2, 3, 3, 3, 3, 4]
    @test Parquet._encodedictionaryindices(zeros(UInt64, 128), 0) == UInt8[0x00, 0x80, 0x02]
    @test Parquet._encodedictionaryindices(fill(UInt64(0x123), 3), 9) == UInt8[0x09, 0x06, 0x23, 0x01]
    encoded = vcat(UInt8[0x02], Parquet.encode_hybrid(UInt64[0, 1, 2, 3, 2], 2))
    @test Parquet._decodedictionaryindices(encoded, 5, 1, Parquet.Limits()) ==
        (UInt64[0, 1, 2, 3, 2], length(encoded) + 1)
    @test Parquet._decodedictionaryindices(UInt8[], 0, 1, Parquet.Limits()) == (UInt64[], 1)
    @test_throws Parquet.FormatError Parquet._decodedictionaryindices(UInt8[33], 1, 1, Parquet.Limits())
    dictionary = Parquet.DecodedDictionary([UInt8[0x01]])
    values = Parquet._lookupdictionary(dictionary, UInt64[0, 0])
    values[1][1] = 0xff
    @test values[2] == UInt8[0x01]
    @test_throws Parquet.FormatError Parquet._lookupdictionary(dictionary, UInt64[1])
    bits = UInt32[0x00000000, 0x80000000, 0x7fc00001, 0x7fc00002]
    floats = reinterpret(Float32, bits)
    floatplan = Parquet._dictionaryplan(Parquet._writecolumn(:value, repeat(collect(floats), 8)),
        Parquet.Limits())
    @test reinterpret(UInt32, floatplan.values) == bits
    raw = Vector{UInt8}[UInt8[1, 2], UInt8[1, 2], UInt8[3], UInt8[1, 2]]
    rawplan = Parquet._dictionaryplan(Parquet._writecolumn(:value, raw), Parquet.Limits())
    @test rawplan.values == Vector{UInt8}[UInt8[1, 2], UInt8[3]]
    @test rawplan.indices == UInt64[0, 0, 1, 0]
end

@testset "dictionary V1 decoding" begin
    int32 = columnleaf(MD.Type.INT32)
    dictionary = dictionarypage(Int32[10, 20, 30])
    data = dictionarydatapage(UInt64[2, 0, 1, 2])
    @test readdictionarypages(dictionary, data, int32; num_values=4) == Int32[30, 10, 20, 30]
    legacy = dictionarypage(Int32[10, 20, 30]; encoding=MD.Encoding.PLAIN_DICTIONARY)
    legacydata = dictionarydatapage(UInt64[1, 2, 0]; encoding=MD.Encoding.PLAIN_DICTIONARY)
    @test readdictionarypages(legacy, legacydata, int32; num_values=3) == Int32[20, 30, 10]
    # parquet-mr 1.10 can omit the dictionary offset and point the data offset at it.
    @test readdictionarypages(legacy, legacydata, int32; num_values=3,
        data_page_offset=4, dictionary_page_offset=nothing) ==
        Int32[20, 30, 10]
    # Modern footer offsets identify both frames exactly.
    @test readdictionarypages(legacy, legacydata, int32; num_values=3,
        data_page_offset=4 + length(legacy), dictionary_page_offset=Int64(4)) ==
        Int32[20, 30, 10]
    optional = columnleaf(MD.Type.INT32; repetition=MD.FieldRepetitionType.OPTIONAL)
    optionaldata = dictionarydatapage(UInt64[1, 0, 1]; levels=[1, 0, 1, 1], maxlevel=1)
    @test isequal(readdictionarypages(dictionary, optionaldata, optional; num_values=4),
        Union{Missing,Int32}[20, missing, 10, 20])
    allnull = dictionarydatapage(UInt64[]; levels=[0, 0, 0], maxlevel=1, omit_indices=true)
    @test isequal(readdictionarypages(dictionary, allnull, optional; num_values=3),
        Union{Missing,Int32}[missing, missing, missing])
    booldictionary = dictionarypage(Bool[false, true])
    booldata = dictionarydatapage(UInt64[1, 0, 1, 1])
    @test readdictionarypages(booldictionary, booldata, columnleaf(MD.Type.BOOLEAN);
        num_values=4) == Bool[true, false, true, true]
    bytesdictionary = dictionarypage(Vector{UInt8}[UInt8[0x61], UInt8[0x62, 0x63]])
    bytesdata = dictionarydatapage(UInt64[1, 0, 1])
    bytes = readdictionarypages(bytesdictionary, bytesdata, columnleaf(MD.Type.BYTE_ARRAY);
        num_values=3)
    @test bytes == Vector{UInt8}[UInt8[0x62, 0x63], UInt8[0x61], UInt8[0x62, 0x63]]
    bytes[1][1] = 0xff
    @test bytes[3] == UInt8[0x62, 0x63]
    fixedvalues = Vector{UInt8}[UInt8[1, 2], UInt8[3, 4]]
    fixeddictionary = dictionarypage(fixedvalues; width=2)
    fixeddata = dictionarydatapage(UInt64[1, 0, 1])
    @test readdictionarypages(fixeddictionary, fixeddata,
        columnleaf(MD.Type.FIXED_LEN_BYTE_ARRAY; width=2); num_values=3) ==
        Vector{UInt8}[UInt8[3, 4], UInt8[1, 2], UInt8[3, 4]]
    first = dictionarydatapage(UInt64[0, 1])
    fallback = datapage(Int32[30, 40])
    @test readdictionarypages(dictionary, [first, fallback], int32; num_values=4) ==
        Int32[10, 20, 30, 40]
    emptydictionary = dictionarypage(Int32[])
    @test readdictionarypages(emptydictionary, Vector{UInt8}[], int32; num_values=0,
        data_page_offset=0) == Int32[]
end

@testset "dictionary malformed input" begin
    int32 = columnleaf(MD.Type.INT32)
    dictionary = dictionarypage(Int32[10, 20])
    data = dictionarydatapage(UInt64[0, 1])
    @test_throws Parquet.FormatError readsynthetic([data], int32; num_values=2)
    outofrange = dictionarydatapage(UInt64[0, 2])
    @test_throws Parquet.FormatError readdictionarypages(dictionary, outofrange, int32; num_values=2)
    wide = dictionarydatapage(UInt64[0]; bitwidth=33)
    @test_throws Parquet.FormatError readdictionarypages(dictionary, wide, int32; num_values=1)
    trailing = dictionarydatapage(UInt64[0]; extra=UInt8[0x00])
    @test_throws Parquet.FormatError readdictionarypages(dictionary, trailing, int32; num_values=1)
    baddictionary = dictionarypage(Int32[10]; extra=UInt8[0x00])
    @test_throws Parquet.FormatError readdictionarypages(baddictionary,
        dictionarydatapage(UInt64[0]), int32; num_values=1)
    wrongencoding = dictionarypage(Int32[10]; encoding=MD.Encoding.DELTA_BINARY_PACKED)
    @test_throws Parquet.FormatError readdictionarypages(wrongencoding,
        dictionarydatapage(UInt64[0]), int32; num_values=1)
    negative = dictionarypage(Int32[]; count=-1)
    @test_throws Parquet.FormatError readdictionarypages(negative, Vector{UInt8}[], int32;
        num_values=0, data_page_offset=0)
    truncated = dictionarypage(Int32[10]; count=2)
    @test_throws Parquet.FormatError readdictionarypages(truncated,
        dictionarydatapage(UInt64[0]), int32; num_values=1)
    emptydictionary = dictionarypage(Int32[])
    @test_throws Parquet.FormatError readdictionarypages(emptydictionary,
        dictionarydatapage(UInt64[0]), int32; num_values=1)
    @test_throws Parquet.FormatError readsynthetic([datapage(Int32[1]), dictionary, data], int32;
        num_values=3, dictionary_page_offset=4 + length(datapage(Int32[1])), data_page_offset=4)
    @test_throws Parquet.FormatError readdictionarypages(dictionary, [dictionary, data], int32;
        num_values=2)
    index = columnpage(UInt8[]; type=MD.PageType.INDEX_PAGE,
        index=MD.IndexPageHeader(), crc=:none)
    @test_throws Parquet.FormatError readsynthetic([index, dictionary, data], int32;
        num_values=2, index_page_offset=Int64(4),
        dictionary_page_offset=nothing, data_page_offset=4 + length(index))
    unknown = columnpage(UInt8[]; type=MD.PageType.T(99), crc=:none)
    @test readsynthetic([dictionary, unknown, data], int32; num_values=2,
        dictionary_page_offset=4, data_page_offset=4 + length(dictionary) + length(unknown)) ==
        Int32[10, 20]
    corrupt = dictionarypage(Int32[10]; crc=Int32(0))
    @test_throws Parquet.FormatError readdictionarypages(corrupt,
        dictionarydatapage(UInt64[0]), int32; num_values=1)
    @test_throws Parquet.LimitError readdictionarypages(dictionary, data, int32;
        num_values=2, limits=Parquet.Limits(max_container_elements=1))
end

@testset "adaptive dictionary writer" begin
    rawvalue = UInt8[0x00, 0xff, 0x41]
    floatpattern = Float64[0.0, -0.0, reinterpret(Float64, UInt64(0x7ff8000000000001)),
        reinterpret(Float64, UInt64(0x7ff8000000000002))]
    input = (
        integers=fill(Int32(42), 128),
        floats=repeat(floatpattern, 32),
        strings=fill("repeated string", 128),
        raw=fill(rawvalue, 128),
        optional=Union{Missing,Int64}[isodd(index) ? 7 : missing for index in 1:128],
        flags=repeat(Bool[true, false], 64),
    )
    bytes = Parquet._encodefile(input; dictionary=true)
    @test bytes == Parquet._encodefile(input; dictionary=true)
    file = Parquet.File(bytes)
    metadata = TH.decode(file.footer.bytes, MD.FileMetaData)
    chunks = metadata.row_groups[1].columns
    @test all(chunks[index].meta_data.dictionary_page_offset !== nothing for index in 1:5)
    @test chunks[6].meta_data.dictionary_page_offset === nothing
    @test all(MD.Encoding.RLE_DICTIONARY in chunks[index].meta_data.encodings for index in 1:5)
    @test chunks[6].meta_data.encodings == [MD.Encoding.PLAIN]
    @test chunks[1].meta_data.encoding_stats == [
        MD.PageEncodingStats(page_type=MD.PageType.DICTIONARY_PAGE,
            encoding=MD.Encoding.PLAIN, count=Int32(1)),
        MD.PageEncodingStats(page_type=MD.PageType.DATA_PAGE,
            encoding=MD.Encoding.RLE_DICTIONARY, count=Int32(1)),
    ]
    close(file)
    table = Parquet.Table(bytes)
    @test table.columns.integers == input.integers
    @test reinterpret(UInt64, table.columns.floats) == reinterpret(UInt64, input.floats)
    @test table.columns.strings == input.strings
    @test table.columns.raw == input.raw
    @test isequal(table.columns.optional, input.optional)
    @test table.columns.flags == input.flags
    table.columns.raw[1][1] = 0xaa
    @test table.columns.raw[2] == rawvalue
    close(table)
    boolean = Parquet._encodefile((value=fill(true, 2000),); dictionary=true)
    booleanfile = Parquet.File(boolean)
    booleanmeta = TH.decode(booleanfile.footer.bytes, MD.FileMetaData)
    booleanchunk = booleanmeta.row_groups[1].columns[1].meta_data
    @test booleanchunk.dictionary_page_offset === nothing
    @test booleanchunk.encodings == [MD.Encoding.PLAIN]
    close(booleanfile)
    booleantable = Parquet.Table(boolean)
    @test booleantable.columns.value == fill(true, 2000)
    close(booleantable)
    allmissing = Union{Missing,Int32}[missing for _ in 1:8]
    missingcolumn = Parquet._writecolumn(:value, allmissing)
    missinglimit = length(Parquet._definitionpayload(missingcolumn))
    tight = Parquet.Limits(max_page_bytes=missinglimit)
    @test Parquet._encodefile((value=allmissing,); dictionary=true, limits=tight) ==
        Parquet._encodefile((value=allmissing,); dictionary=false, limits=tight)
    plain = Parquet._encodefile((value=fill(Int32(1), 128),))
    plainfile = Parquet.File(plain)
    plainmeta = TH.decode(plainfile.footer.bytes, MD.FileMetaData)
    @test plainmeta.row_groups[1].columns[1].meta_data.dictionary_page_offset === nothing
    close(plainfile)
    unique = Parquet._encodefile((value=collect(Int32(1):Int32(32)),); dictionary=true)
    uniquefile = Parquet.File(unique)
    uniquemeta = TH.decode(uniquefile.footer.bytes, MD.FileMetaData)
    @test uniquemeta.row_groups[1].columns[1].meta_data.dictionary_page_offset === nothing
    close(uniquefile)
    io = IOBuffer()
    Parquet.write(io, (value=fill(Int64(9), 100),); dictionary=true, checksum=false)
    @test Parquet.Table(take!(io)).columns.value == fill(Int64(9), 100)
end

@testset "adaptive dictionary V2 writer" begin
    input = (value=fill(Int32(42), 128),)
    dictionarycount = 0
    plaincount = 0
    for codec in (:uncompressed, :snappy, :gzip, :brotli, :zstd, :lz4_raw)
        bytes = Parquet._encodefile(input; dictionary=true, codec=codec, pageversion=:v2)
        plain = Parquet._encodefile(input; dictionary=false, codec=codec, pageversion=:v2)
        table = Parquet.Table(bytes)
        @test table.columns.value == input.value
        close(table)
        pages, metadata = writtenpages(bytes, 1)
        _, plainmetadata = writtenpages(plain, 1)
        @test metadata.total_compressed_size <= plainmetadata.total_compressed_size
        if metadata.dictionary_page_offset === nothing
            plaincount += 1
            @test [page.header.type_ for page in pages] == [MD.PageType.DATA_PAGE_V2]
            @test pages[1].header.data_page_header_v2.encoding == MD.Encoding.PLAIN
        else
            dictionarycount += 1
            @test [page.header.type_ for page in pages] ==
                [MD.PageType.DICTIONARY_PAGE, MD.PageType.DATA_PAGE_V2]
            @test pages[2].header.data_page_header_v2.encoding == MD.Encoding.RLE_DICTIONARY
            @test metadata.encoding_stats == [
                MD.PageEncodingStats(page_type=MD.PageType.DICTIONARY_PAGE,
                    encoding=MD.Encoding.PLAIN, count=Int32(1)),
                MD.PageEncodingStats(page_type=MD.PageType.DATA_PAGE_V2,
                    encoding=MD.Encoding.RLE_DICTIONARY, count=Int32(1)),
            ]
        end
    end
    @test dictionarycount > 0
    @test plaincount > 0
end

@testset "official dictionary fixtures" begin
    if !isdir(columncorpus())
        @warn "parquet-testing corpus not found; skipping dictionary corpus tests" COLUMN_CORPUS
    else
        longs, _ = corpuscolumn(columncorpus("plain-dict-uncompressed-checksum.parquet"), 1)
        binary, _ = corpuscolumn(columncorpus("plain-dict-uncompressed-checksum.parquet"), 2)
        expected = collect(codeunits("a655fd0e-9949-4059-bcae-fd6a002a4652"))
        @test longs == zeros(Int64, 1000)
        @test length(binary) == 1000 && all(==(expected), binary)
        @test binary[1] !== binary[2]
        @test_throws Parquet.FormatError corpuscolumn(
            columncorpus("rle-dict-uncompressed-corrupt-checksum.parquet"), 1)
        indexed = Parquet.Table(columncorpus("data_index_bloom_encoding_with_length.parquet"))
        @test indexed.columns.String == ["Hello", "This is", "a", "test", "How", "are you",
            "doing ", "today", "the quick", "brown fox", "jumps", "over", "the lazy", "dog"]
        close(indexed)
        alltypes = Parquet.File(columncorpus("alltypes_dictionary.parquet"))
        allmeta = TH.decode(alltypes.footer.bytes, MD.FileMetaData)
        allschema = Parquet.Schema(allmeta)
        @test Parquet.readcolumn(alltypes, allmeta, allschema, 1, 1) == Int32[0, 1]
        @test Parquet.readcolumn(alltypes, allmeta, allschema, 1, 2) == Bool[true, false]
        @test Parquet.readcolumn(alltypes, allmeta, allschema, 1, 6) == Int64[0, 10]
        @test Parquet.readcolumn(alltypes, allmeta, allschema, 1, 9) ==
            Vector{UInt8}[collect(codeunits("01/01/09")), collect(codeunits("01/01/09"))]
        close(alltypes)
        tiny = Parquet.File(columncorpus("alltypes_tiny_pages.parquet"))
        tinymeta = TH.decode(tiny.footer.bytes, MD.FileMetaData)
        tinyschema = Parquet.Schema(tinymeta)
        tinyints = Parquet.readcolumn(tiny, tinymeta, tinyschema, 1, 3)
        tinystrings = Parquet.readcolumn(tiny, tinymeta, tinyschema, 1, 10)
        @test length(tinyints) == 7300 && sum(Int64, tinyints) == 32850 && extrema(tinyints) == (0, 9)
        @test tinyints[1:12] == Int32[2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3]
        @test length(unique(tinystrings)) == 10
        @test String.(tinystrings[1:12]) == ["2", "3", "4", "5", "6", "7", "8", "9", "0", "1", "2", "3"]
        @test tinymeta.row_groups[1].columns[3].meta_data.dictionary_page_offset === nothing
        close(tiny)
        empty = Parquet.Table(columncorpus("column_chunk_key_value_metadata.parquet"))
        @test length(empty) == 0
        close(empty)
    end
end
