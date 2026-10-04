using Dates
using Test

const WSMD = Parquet.Metadata
const WSTH = Parquet.Thrift

function wsframes(file::Parquet.File, chunk::WSMD.ColumnChunk)
    metadata = something(chunk.meta_data)
    start = metadata.dictionary_page_offset === nothing ?
        metadata.data_page_offset : min(metadata.data_page_offset,
            metadata.dictionary_page_offset)
    stop = Base.checked_add(start, metadata.total_compressed_size)
    @test 0 <= start < stop <= file.footer.offset
    output = []
    position = start
    while position < stop
        frame = Parquet.readpage(file.source, position, stop, Parquet.Limits())
        frameend = Parquet.pageend(frame)
        @test frameend > position
        @test frameend <= stop
        push!(output, (
            offset=position,
            header=frame.header,
            headerlength=frame.headerlength,
            payload=collect(frame.payload),
            frameend=frameend,
        ))
        position = frameend
    end
    @test position == stop
    return output
end

function wsinspect(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    try
        metadata = WSTH.decode(copy(file.footer.bytes), WSMD.FileMetaData)
        schema = Parquet.Schema(metadata.schema)
        groups = []
        for group in metadata.row_groups
            push!(groups, [wsframes(file, chunk) for chunk in group.columns])
        end
        return (; metadata, schema, groups, footer_offset=file.footer.offset)
    finally
        close(file)
    end
end

function wsmetadata(bytes::Vector{UInt8})
    file = Parquet.File(bytes)
    try
        return WSTH.decode(copy(file.footer.bytes), WSMD.FileMetaData)
    finally
        close(file)
    end
end

function wsdatapage(frame)
    type = frame.header.type_
    return type == WSMD.PageType.DATA_PAGE ||
        type == WSMD.PageType.DATA_PAGE_V2
end

function wsframeencoding(frame)
    header = frame.header
    header.type_ == WSMD.PageType.DICTIONARY_PAGE &&
        return header.dictionary_page_header.encoding
    header.type_ == WSMD.PageType.DATA_PAGE &&
        return header.data_page_header.encoding
    header.type_ == WSMD.PageType.DATA_PAGE_V2 &&
        return header.data_page_header_v2.encoding
    throw(ArgumentError("unexpected writer page type $(header.type_)"))
end

function wsframevalues(frame)
    header = frame.header
    header.type_ == WSMD.PageType.DATA_PAGE &&
        return Int64(header.data_page_header.num_values)
    header.type_ == WSMD.PageType.DATA_PAGE_V2 &&
        return Int64(header.data_page_header_v2.num_values)
    return Int64(0)
end

function wsderivedstats(frames)
    keys = Tuple{WSMD.PageType.T,WSMD.Encoding.T}[]
    counts = Int32[]
    for frame in frames
        key = (frame.header.type_, wsframeencoding(frame))
        index = findfirst(==(key), keys)
        if index === nothing
            push!(keys, key)
            push!(counts, Int32(1))
        else
            counts[index] = Base.checked_add(counts[index], Int32(1))
        end
    end
    return WSMD.PageEncodingStats[
        WSMD.PageEncodingStats(page_type=key[1], encoding=key[2],
            count=count) for (key, count) in zip(keys, counts)
    ]
end

function wscheckaccounting(result)
    metadata = result.metadata
    for (groupindex, (group, chunks)) in enumerate(zip(metadata.row_groups,
            result.groups))
        compressed = Int64(0)
        uncompressed = Int64(0)
        for (chunk, frames) in zip(group.columns, chunks)
            column = something(chunk.meta_data)
            @test chunk.file_offset == 0
            framecompressed = sum(frame -> frame.frameend - frame.offset,
                frames; init=Int64(0))
            frameuncompressed = sum(frame -> Int64(frame.headerlength) +
                Int64(frame.header.uncompressed_page_size), frames;
                init=Int64(0))
            @test column.total_compressed_size == framecompressed
            @test column.total_uncompressed_size == frameuncompressed
            @test column.num_values == sum(wsframevalues, frames;
                init=Int64(0))
            data = filter(wsdatapage, frames)
            @test !isempty(data)
            @test column.data_page_offset == first(data).offset
            dictionaries = filter(frame ->
                frame.header.type_ == WSMD.PageType.DICTIONARY_PAGE, frames)
            if isempty(dictionaries)
                @test column.dictionary_page_offset === nothing
            else
                @test length(dictionaries) == 1
                @test column.dictionary_page_offset == only(dictionaries).offset
                @test first(frames).offset == only(dictionaries).offset
            end
            @test column.encoding_stats == wsderivedstats(frames)
            compressed = Base.checked_add(compressed, framecompressed)
            uncompressed = Base.checked_add(uncompressed, frameuncompressed)
        end
        @test group.total_compressed_size == compressed
        @test group.total_byte_size == uncompressed
        @test group.file_offset == first(first(chunks)).offset
        @test group.num_rows > 0
        @test groupindex <= length(metadata.row_groups)
    end
    return
end

function wsdecodelevels(bytes::AbstractVector{UInt8}, count::Int,
        maximum::Int16; offset::Int=1, length_prefix::Bool=false)
    iszero(maximum) && return zeros(UInt64, count), offset
    return Parquet.decode_hybrid(bytes, count,
        Parquet._levelbitwidth(maximum); offset=offset,
        length_prefix=length_prefix)
end

function wslevels(frame, node::Parquet.SchemaNode,
        codec::WSMD.CompressionCodec.T)
    header = frame.header
    if header.type_ == WSMD.PageType.DATA_PAGE
        count = Int(header.data_page_header.num_values)
        bytes = Parquet.decompress(codec, frame.payload,
            header.uncompressed_page_size)
        repetition, position = wsdecodelevels(bytes, count,
            node.max_repetition_level; length_prefix=true)
        definition, position = wsdecodelevels(bytes, count,
            node.max_definition_level; offset=position, length_prefix=true)
        return (; repetition, definition,
            values=collect(@view bytes[position:end]))
    end
    @test header.type_ == WSMD.PageType.DATA_PAGE_V2
    page = header.data_page_header_v2
    count = Int(page.num_values)
    repetitionlength = Int(page.repetition_levels_byte_length)
    definitionlength = Int(page.definition_levels_byte_length)
    definitionstart = repetitionlength + 1
    definitionstop = repetitionlength + definitionlength
    repetitionbytes = @view frame.payload[1:repetitionlength]
    definitionbytes = @view frame.payload[definitionstart:definitionstop]
    repetition, repetitionposition = wsdecodelevels(repetitionbytes, count,
        node.max_repetition_level)
    definition, definitionposition = wsdecodelevels(definitionbytes, count,
        node.max_definition_level)
    @test repetitionposition == length(repetitionbytes) + 1
    @test definitionposition == length(definitionbytes) + 1
    encoded = @view frame.payload[(definitionstop + 1):end]
    expected = Int(header.uncompressed_page_size) - repetitionlength -
        definitionlength
    values = if something(page.is_compressed, true)
        Parquet.decompress(codec, encoded, expected)
    else
        @test length(encoded) == expected
        collect(encoded)
    end
    return (; repetition, definition, values)
end

function wsroundtrip(bytes::Vector{UInt8}, expected::NamedTuple)
    table = Parquet.Table(bytes)
    try
        for name in keys(expected)
            @test isequal(getproperty(table.columns, name),
                getproperty(expected, name))
        end
    finally
        close(table)
    end
    return
end

function wsgoldeninput()
    E = Union{Missing,Date}
    days = Union{Missing,Vector{E}}[
        missing,
        E[],
        E[missing],
        E[Date(1970, 1, 1), missing, Date(1969, 12, 31)],
        E[Date(2000, 2, 29)],
    ]
    return (; id=Int32[1, 2, 3, 4, 5], days)
end

function wsdatacounts(frames)
    return Int32[frame.header.type_ == WSMD.PageType.DATA_PAGE ?
        frame.header.data_page_header.num_values :
        frame.header.data_page_header_v2.num_values
        for frame in frames if wsdatapage(frame)]
end

function wsdecodedictionary(frame, codec::WSMD.CompressionCodec.T)
    header = frame.header
    @test header.type_ == WSMD.PageType.DICTIONARY_PAGE
    count = Int(header.dictionary_page_header.num_values)
    bytes = Parquet.decompress(codec, frame.payload,
        header.uncompressed_page_size)
    values, position = Parquet.decode_plain_byte_array(bytes, count)
    @test position == length(bytes) + 1
    return String[String(value) for value in values]
end

@testset "writer row-boundary prefixes" begin
    input = wsgoldeninput()
    plan = Parquet._writeplan(input)
    leaf = only(filter(leaf -> leaf.path == ["days", "list", "element"],
        only(plan.rowgroups).leaves))
    @test leaf.entry_offsets == Int64[0, 1, 2, 3, 6, 7]
    @test leaf.dense_offsets == Int64[0, 0, 0, 0, 2, 3]
    @test leaf.payload_offsets == Int64[0, 0, 0, 0, 8, 12]
    @test leaf.column.repetitions == UInt64[0, 0, 0, 0, 1, 1, 0]
    @test leaf.column.definitions == UInt64[0, 1, 2, 3, 2, 3, 3]
    @test leaf.column.values == Int32[0, -1, 11016]
    for prefix in (leaf.entry_offsets, leaf.dense_offsets,
            leaf.payload_offsets)
        @test length(prefix) == length(input.id) + 1
        @test first(prefix) == 0
        @test issorted(prefix)
    end
end

@testset "writer row groups and nested slices" begin
    input = wsgoldeninput()
    expectedrepetition = (
        UInt64[0, 0], UInt64[0, 0, 1, 1], UInt64[0])
    expecteddefinition = (
        UInt64[0, 1], UInt64[2, 3, 2, 3], UInt64[3])
    expectedphysical = (Int32[], Int32[0, -1], Int32[11016])
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; rowgroupsize=2,
            pagesize=nothing, pageversion=pageversion)
        result = wsinspect(bytes)
        @test result.metadata.num_rows == 5
        @test [group.num_rows for group in result.metadata.row_groups] ==
            Int64[2, 2, 1]
        @test [group.ordinal for group in result.metadata.row_groups] ==
            Union{Nothing,Int16}[0, 1, 2]
        @test [group.columns[1].meta_data.num_values
            for group in result.metadata.row_groups] == Int64[2, 2, 1]
        @test [group.columns[2].meta_data.num_values
            for group in result.metadata.row_groups] == Int64[2, 4, 1]
        @test all(group -> [chunk.meta_data.path_in_schema
            for chunk in group.columns] == [["id"],
                ["days", "list", "element"]], result.metadata.row_groups)
        for groupindex in 1:3
            frame = only(filter(wsdatapage, result.groups[groupindex][2]))
            levels = wslevels(frame, result.schema.leaves[2],
                WSMD.CompressionCodec.UNCOMPRESSED)
            @test levels.repetition == expectedrepetition[groupindex]
            @test levels.definition == expecteddefinition[groupindex]
            physical, position = Parquet.decode_plain(Int32, levels.values,
                length(expectedphysical[groupindex]))
            @test physical == expectedphysical[groupindex]
            @test position == length(levels.values) + 1
            if pageversion === :v2
                page = frame.header.data_page_header_v2
                @test page.num_rows == Int32[2, 2, 1][groupindex]
                @test page.num_nulls == Int32[2, 2, 0][groupindex]
            end
        end
        wscheckaccounting(result)
        wsroundtrip(bytes, input)
    end
    one = Parquet._encodefile(input; rowgroupsize=nothing,
        pagesize=nothing)
    @test length(wsinspect(one).metadata.row_groups) == 1
    @test Parquet._encodefile(input) == Parquet._encodefile(input;
        rowgroupsize=1_048_576, pagesize=1024 * 1024)
end

@testset "writer soft page targets" begin
    input = (; value=Int32[1, 2, 3, 4, 5])
    for pageversion in (:v1, :v2)
        for (pagesize, expected) in ((nothing, Int32[5]),
                (8, Int32[2, 2, 1]), (4, Int32[1, 1, 1, 1, 1]))
            bytes = Parquet._encodefile(input; rowgroupsize=nothing,
                pagesize=pagesize, pageversion=pageversion)
            result = wsinspect(bytes)
            @test wsdatacounts(only(result.groups)[1]) == expected
            @test all(frame -> begin
                levels = wslevels(frame, only(result.schema.leaves),
                    WSMD.CompressionCodec.UNCOMPRESSED)
                !isempty(levels.repetition) && iszero(first(levels.repetition))
            end, filter(wsdatapage, only(result.groups)[1]))
            wscheckaccounting(result)
            wsroundtrip(bytes, input)
        end
    end
end

@testset "writer hard page retry and propagation" begin
    input = (; value=Int32.(1:16))
    limits = Parquet.Limits(max_page_bytes=16)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; pagesize=nothing,
            pageversion=pageversion, limits=limits)
        result = wsinspect(bytes)
        @test wsdatacounts(only(result.groups)[1]) == Int32[4, 4, 4, 4]
        @test all(frame -> frame.header.compressed_page_size <= 16 &&
            frame.header.uncompressed_page_size <= 16,
            filter(wsdatapage, only(result.groups)[1]))
        wscheckaccounting(result)
        wsroundtrip(bytes, input)
    end

    plan = Parquet._writeplan(input)
    leaf = only(only(plan.rowgroups).leaves)
    budget = Parquet._LiveByteBudget(limits)
    pages, charge = Parquet._budgetedsplitcolumnpages(leaf, limits, budget;
        pagesize=nothing, checksum=true, dictionary=false,
        codec=WSMD.CompressionCodec.UNCOMPRESSED, compressionlevel=nothing,
        pageversion=:v1)
    @test only(pages.encoding_stats).count == 4
    @test Parquet._budgetused(budget) == charge
    Parquet._release!(budget, charge)
    @test Parquet._budgetused(budget) == 0

    # A page build that throws must release its working reservation, leaving only
    # what the caller already held.
    failbudget = Parquet._LiveByteBudget(Parquet.Limits())
    Parquet._reserve!(failbudget, Int64(64))
    failerror = try
        Parquet._budgetedsplitcolumnpages(leaf,
            Parquet.Limits(max_page_bytes=3), failbudget;
            pagesize=nothing, checksum=true, dictionary=false,
            codec=WSMD.CompressionCodec.UNCOMPRESSED, compressionlevel=nothing,
            pageversion=:v1)
        nothing
    catch err
        err
    end
    @test failerror isa Parquet.LimitError
    @test Parquet._budgetused(failbudget) == 64

    error = try
        Parquet._encodefile((value=Int32[1],); pagesize=nothing,
            limits=Parquet.Limits(max_page_bytes=3))
        nothing
    catch err
        err
    end
    @test error isa Parquet.LimitError
    @test error.resource == :page_bytes
    headererror = try
        Parquet._encodefile(input; pagesize=nothing,
            limits=Parquet.Limits(max_page_header_bytes=1))
        nothing
    catch err
        err
    end
    @test headererror isa Parquet.LimitError
    @test headererror.resource == :page_header_bytes
    stringerror = try
        Parquet._encodefile((value=["large"],); pagesize=nothing,
            limits=Parquet.Limits(max_string_bytes=4))
        nothing
    catch err
        err
    end
    @test stringerror isa Parquet.LimitError
    @test stringerror.resource == :string_bytes
    @test_throws ArgumentError Parquet._encodefile(input; pagesize=nothing,
        encoding=:delta_byte_array)
    io = IOBuffer()
    @test_throws Parquet.LimitError Parquet.write(io, (value=Int32[1],);
        pagesize=nothing, limits=Parquet.Limits(max_page_bytes=3))
    @test isempty(take!(io))
    mktempdir() do directory
        path = joinpath(directory, "failed.parquet")
        @test_throws Parquet.LimitError Parquet.write(path,
            (value=Int32[1],); pagesize=nothing,
            limits=Parquet.Limits(max_page_bytes=3))
        @test !ispath(path)
    end
end

@testset "writer dictionary scope and fallback" begin
    reset = (; value=vcat(fill("alpha", 128), fill("beta", 128)))
    bytes = Parquet._encodefile(reset; rowgroupsize=128,
        pagesize=nothing, dictionary=true)
    result = wsinspect(bytes)
    @test length(result.metadata.row_groups) == 2
    for (index, expected) in enumerate(("alpha", "beta"))
        frames = result.groups[index][1]
        @test [frame.header.type_ for frame in frames] ==
            [WSMD.PageType.DICTIONARY_PAGE, WSMD.PageType.DATA_PAGE]
        @test wsdecodedictionary(first(frames),
            WSMD.CompressionCodec.UNCOMPRESSED) == [expected]
        @test result.metadata.row_groups[index].columns[1].meta_data.encoding_stats ==
            WSMD.PageEncodingStats[
                WSMD.PageEncodingStats(page_type=WSMD.PageType.DICTIONARY_PAGE,
                    encoding=WSMD.Encoding.PLAIN, count=Int32(1)),
                WSMD.PageEncodingStats(page_type=WSMD.PageType.DATA_PAGE,
                    encoding=WSMD.Encoding.RLE_DICTIONARY, count=Int32(1)),
            ]
    end
    wscheckaccounting(result)
    wsroundtrip(bytes, reset)

    mixed = (; value=vcat(fill("same", 128),
        ["unique-$(lpad(index, 4, '0'))" for index in 1:128]))
    for policy in ((dictionary=true,), (encoding=:dictionary,))
        bytes = Parquet._encodefile(mixed; rowgroupsize=128,
            pagesize=nothing, policy...)
        result = wsinspect(bytes)
        firstcolumn = result.metadata.row_groups[1].columns[1].meta_data
        secondcolumn = result.metadata.row_groups[2].columns[1].meta_data
        @test firstcolumn.dictionary_page_offset !== nothing
        @test secondcolumn.dictionary_page_offset === nothing
        @test firstcolumn.encoding_stats == WSMD.PageEncodingStats[
            WSMD.PageEncodingStats(page_type=WSMD.PageType.DICTIONARY_PAGE,
                encoding=WSMD.Encoding.PLAIN, count=Int32(1)),
            WSMD.PageEncodingStats(page_type=WSMD.PageType.DATA_PAGE,
                encoding=WSMD.Encoding.RLE_DICTIONARY, count=Int32(1)),
        ]
        @test secondcolumn.encoding_stats == WSMD.PageEncodingStats[
            WSMD.PageEncodingStats(page_type=WSMD.PageType.DATA_PAGE,
                encoding=WSMD.Encoding.PLAIN, count=Int32(1)),
        ]
        wscheckaccounting(result)
        wsroundtrip(bytes, mixed)
    end

    split = (; value=fill("same", 128))
    bytes = Parquet._encodefile(split; dictionary=true, pagesize=16)
    result = wsinspect(bytes)
    frames = only(result.groups)[1]
    @test first(frames).header.type_ == WSMD.PageType.DICTIONARY_PAGE
    @test count(wsdatapage, frames) == 64
    @test count(frame -> frame.header.type_ == WSMD.PageType.DICTIONARY_PAGE,
        frames) == 1
    @test result.metadata.row_groups[1].columns[1].meta_data.encoding_stats ==
        WSMD.PageEncodingStats[
            WSMD.PageEncodingStats(page_type=WSMD.PageType.DICTIONARY_PAGE,
                encoding=WSMD.Encoding.PLAIN, count=Int32(1)),
            WSMD.PageEncodingStats(page_type=WSMD.PageType.DATA_PAGE,
                encoding=WSMD.Encoding.RLE_DICTIONARY, count=Int32(64)),
        ]
    wscheckaccounting(result)
    wsroundtrip(bytes, split)

    tie = (; value=fill(Int32(1), 5))
    plan = Parquet._writeplan(tie)
    leaf = only(only(plan.rowgroups).leaves)
    limits = Parquet.Limits()
    plain = Parquet._writeencodedchunk(leaf, nothing, WSMD.Encoding.PLAIN,
        limits; checksum=false, codec=WSMD.CompressionCodec.UNCOMPRESSED,
        compressionlevel=nothing, pageversion=:v1)
    dictionary = Parquet._writedictionarychunk(leaf, nothing, limits;
        checksum=false, codec=WSMD.CompressionCodec.UNCOMPRESSED,
        compressionlevel=nothing, pageversion=:v1)
    @test length(plain.bytes) == length(dictionary.bytes)
    result = wsinspect(Parquet._encodefile(tie; dictionary=true,
        checksum=false, pagesize=nothing))
    @test result.metadata.row_groups[1].columns[1].meta_data.dictionary_page_offset ===
        nothing

    rescue = (; value=[fill("x", 20)])
    rescuelimits = Parquet.Limits(max_page_bytes=20)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(rescue; dictionary=true, checksum=false,
            pagesize=nothing, pageversion=pageversion, limits=rescuelimits)
        result = wsinspect(bytes)
        frames = only(result.groups)[1]
        datapagetype = pageversion === :v1 ? WSMD.PageType.DATA_PAGE :
            WSMD.PageType.DATA_PAGE_V2
        @test [frame.header.type_ for frame in frames] ==
            [WSMD.PageType.DICTIONARY_PAGE, datapagetype]
        column = result.metadata.row_groups[1].columns[1].meta_data
        @test column.dictionary_page_offset == first(frames).offset
        @test column.data_page_offset == last(frames).offset
        @test wsframeencoding(last(frames)) == WSMD.Encoding.RLE_DICTIONARY
        wscheckaccounting(result)
        wsroundtrip(bytes, rescue)
    end
end

@testset "writer V1 V2 codec splitting matrix" begin
    input = wsgoldeninput()
    codecs = (
        (:uncompressed, WSMD.CompressionCodec.UNCOMPRESSED),
        (:snappy, WSMD.CompressionCodec.SNAPPY),
        (:gzip, WSMD.CompressionCodec.GZIP),
        (:brotli, WSMD.CompressionCodec.BROTLI),
        (:zstd, WSMD.CompressionCodec.ZSTD),
        (:lz4_raw, WSMD.CompressionCodec.LZ4_RAW),
    )
    expectedrepetition = (
        UInt64[0], UInt64[0], UInt64[0], UInt64[0, 1, 1], UInt64[0])
    expecteddefinition = (
        UInt64[0], UInt64[1], UInt64[2], UInt64[3, 2, 3], UInt64[3])
    expectedphysical = (Int32[], Int32[], Int32[], Int32[0, -1],
        Int32[11016])
    for pageversion in (:v1, :v2), (codecname, codec) in codecs
        bytes = Parquet._encodefile(input; rowgroupsize=2, pagesize=1,
            pageversion=pageversion, codec=codecname)
        result = wsinspect(bytes)
        @test [group.num_rows for group in result.metadata.row_groups] ==
            Int64[2, 2, 1]
        @test all(group -> all(chunk -> chunk.meta_data.codec == codec,
            group.columns), result.metadata.row_groups)
        dayframes = [frame for group in result.groups
            for frame in group[2] if wsdatapage(frame)]
        @test wsdatacounts(dayframes) == Int32[1, 1, 1, 3, 1]
        for index in 1:5
            levels = wslevels(dayframes[index], result.schema.leaves[2], codec)
            @test levels.repetition == expectedrepetition[index]
            @test levels.definition == expecteddefinition[index]
            @test iszero(first(levels.repetition))
            physical, position = Parquet.decode_plain(Int32, levels.values,
                length(expectedphysical[index]))
            @test physical == expectedphysical[index]
            @test position == length(levels.values) + 1
            if pageversion === :v2
                page = dayframes[index].header.data_page_header_v2
                @test page.num_rows == 1
                @test page.num_nulls == Int32[1, 1, 1, 1, 0][index]
            end
        end
        wscheckaccounting(result)
        wsroundtrip(bytes, input)
    end
end

@testset "writer Boolean row and page slices" begin
    values = Bool[true, false, true, true, false, false, true, false,
        true, true, false, true, false, true, true, false, false, true, true]
    input = (; value=values)
    for pageversion in (:v1, :v2)
        bytes = Parquet._encodefile(input; rowgroupsize=nothing,
            pagesize=1, pageversion=pageversion, dictionary=true)
        result = wsinspect(bytes)
        frames = filter(wsdatapage, only(result.groups)[1])
        @test wsdatacounts(frames) == Int32[8, 8, 3]
        decoded = Bool[]
        for frame in frames
            levels = wslevels(frame, only(result.schema.leaves),
                WSMD.CompressionCodec.UNCOMPRESSED)
            count = length(levels.definition)
            valuespart, position = Parquet.decode_plain(Bool, levels.values,
                count)
            append!(decoded, valuespart)
            @test position == length(levels.values) + 1
        end
        @test decoded == values
        @test result.metadata.row_groups[1].columns[1].meta_data.dictionary_page_offset ===
            nothing
        wscheckaccounting(result)
        wsroundtrip(bytes, input)

        grouped = Parquet._encodefile(input; rowgroupsize=5,
            pagesize=nothing, pageversion=pageversion)
        groupedresult = wsinspect(grouped)
        @test [group.num_rows for group in groupedresult.metadata.row_groups] ==
            Int64[5, 5, 5, 4]
        @test [only(wsdatacounts(group[1])) for group in groupedresult.groups] ==
            Int32[5, 5, 5, 4]
        wscheckaccounting(groupedresult)
        wsroundtrip(grouped, input)
    end
end

@testset "writer zero rows and split keyword validation" begin
    empty = (
        scalar=Int32[],
        list=Vector{Union{Missing,Int32}}[],
        map=Dict{String,Union{Missing,Int32}}[],
        record=NamedTuple{(:id,),Tuple{Int32}}[],
    )
    for pageversion in (:v1, :v2), codec in (:uncompressed, :zstd)
        bytes = Parquet._encodefile(empty; pageversion=pageversion,
            codec=codec, rowgroupsize=1, pagesize=1, dictionary=true)
        result = wsinspect(bytes)
        @test result.metadata.num_rows == 0
        @test isempty(result.metadata.row_groups)
        @test isempty(result.groups)
        wsroundtrip(bytes, empty)
    end

    input = (; value=Int32[1, 2])
    for invalid in (0, -1, true, 1.0, "1", :invalid)
        @test_throws ArgumentError Parquet._encodefile(input;
            rowgroupsize=invalid)
        @test_throws ArgumentError Parquet._encodefile(input;
            pagesize=invalid)
    end
    @test_throws ArgumentError Parquet._encodefile(input;
        rowgroupsize=typemax(UInt128))
    @test_throws ArgumentError Parquet._encodefile(input;
        pagesize=typemax(UInt128))
    @test length(wsinspect(Parquet._encodefile(input;
        rowgroupsize=nothing, pagesize=nothing)).metadata.row_groups) == 1
    io = IOBuffer()
    @test_throws ArgumentError Parquet.write(io, input; pagesize=0)
    @test isempty(take!(io))
end

@testset "writer omits overflowing row-group ordinals" begin
    rows = Int(typemax(Int16)) + 2
    bytes = Parquet._encodefile((value=fill(true, rows),);
        rowgroupsize=1, pagesize=nothing, checksum=false)
    metadata = wsmetadata(bytes)
    @test length(metadata.row_groups) == rows
    @test all(group -> group.ordinal === nothing, metadata.row_groups)
end

@testset "writer plan construction budget rollback" begin
    limits = Parquet.Limits()
    field = Parquet._writefieldplan(Parquet._writecolumn(:value, Int32[1]))
    ordinary = Parquet._LiveByteBudget(limits)
    Parquet._reservearray!(ordinary, UInt8, 0)
    ordinaryentry = Parquet._budgetused(ordinary)
    @test ordinaryentry == 64
    @test_throws ArgumentError Parquet._writeplan(
        Parquet.WriteFieldPlan[field], 2, limits, ordinary)
    @test Parquet._budgetused(ordinary) == ordinaryentry

    table = Parquet.Table(Parquet._encodefile((value=Int32[1],)))
    try
        owner = Parquet._LiveByteBudget(limits)
        fields, rows = Parquet._writefieldsencoded(table, limits, owner,
            nothing, false)
        provenance = Parquet._LiveByteBudget(limits)
        Parquet._reservearray!(provenance, UInt8, 0)
        provenanceentry = Parquet._budgetused(provenance)
        @test provenanceentry == 64
        @test_throws ArgumentError Parquet._writeplan(fields, rows + 1,
            limits, provenance)
        @test Parquet._budgetused(provenance) == provenanceentry
    finally
        close(table)
    end

    columnlimits = Parquet.Limits(max_materialized_bytes=1800)
    columnbudget = Parquet._LiveByteBudget(columnlimits)
    Parquet._reservearray!(columnbudget, UInt8, 0)
    columnentry = Parquet._budgetused(columnbudget)
    columns = Parquet.WriteColumn[
        Parquet._writecolumn(:value, Int32[1]),
    ]
    @test_throws Parquet.LimitError Parquet._writeplan(columns, 1,
        columnlimits, columnbudget)
    @test Parquet._budgetused(columnbudget) == columnentry == 64

    ordinarylimits = Parquet.Limits(max_materialized_bytes=5000)
    ordinarytable = Parquet._LiveByteBudget(ordinarylimits)
    Parquet._reservearray!(ordinarytable, UInt8, 0)
    ordinarytableentry = Parquet._budgetused(ordinarytable)
    @test_throws Parquet.LimitError Parquet._writeplan(
        (value=Int32[1],), ordinarylimits, ordinarytable)
    @test Parquet._budgetused(ordinarytable) == ordinarytableentry == 64

    source = Parquet.Table(Parquet._encodefile((value=Int32[1],)))
    try
        provenancelimits = Parquet.Limits(max_materialized_bytes=4000)
        provenancetable = Parquet._LiveByteBudget(provenancelimits)
        Parquet._reservearray!(provenancetable, UInt8, 0)
        provenanceentry = Parquet._budgetused(provenancetable)
        @test_throws Parquet.LimitError Parquet._writeplan(source,
            provenancelimits, provenancetable)
        @test Parquet._budgetused(provenancetable) == provenanceentry == 64
    finally
        close(source)
    end
end
