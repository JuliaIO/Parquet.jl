using Dates
using Test

const OIMD = Parquet.Metadata
const OITH = Parquet.Thrift

function oireplace(value; replacements...)
    names = fieldnames(typeof(value))
    fields = map(names) do name
        return haskey(replacements, name) ? replacements[name] :
            getfield(value, name)
    end
    return typeof(value)(fields...)
end

function oimetadata(file::Parquet.File)
    reader = OITH.Reader(file.footer.bytes)
    metadata = OITH.decode(reader, OIMD.FileMetaData)
    @test OITH.remaining(reader) == 0
    return metadata
end

function oimetadata(bytes::AbstractVector{UInt8})
    file = Parquet.File(bytes)
    try
        return oimetadata(file)
    finally
        close(file)
    end
end

function oichunkstart(metadata::OIMD.ColumnMetaData)
    offsets = Int64[]
    metadata.data_page_offset > 0 && push!(offsets,
        Int64(metadata.data_page_offset))
    dictionary = metadata.dictionary_page_offset
    dictionary !== nothing && dictionary > 0 && push!(offsets,
        Int64(dictionary))
    index = metadata.index_page_offset
    index !== nothing && push!(offsets, Int64(index))
    isempty(offsets) && return Int64(metadata.data_page_offset)
    return minimum(offsets)
end

function oiframes(file::Parquet.File, chunk::OIMD.ColumnChunk)
    metadata = something(chunk.meta_data)
    start = oichunkstart(metadata)
    stop = Base.checked_add(start, metadata.total_compressed_size)
    output = []
    position = start
    while position < stop
        frame = Parquet.readpage(file.source, position, stop, Parquet.Limits())
        frameend = Parquet.pageend(frame)
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

function oirawindex(bytes::AbstractVector{UInt8}, footer::Int64,
        chunk::OIMD.ColumnChunk)
    offset = chunk.offset_index_offset
    count = chunk.offset_index_length
    @test offset !== nothing
    @test count !== nothing
    offset = Int64(offset)
    count = Int64(count)
    @test offset >= 0
    @test count > 0
    @test Base.checked_add(offset, count) <= footer
    first = Int(offset) + 1
    last = Int(offset) + Int(count)
    raw = @view bytes[first:last]
    @test length(raw) == count
    reader = OITH.Reader(raw)
    index = OITH.decode(reader, OIMD.OffsetIndex)
    @test OITH.remaining(reader) == 0
    return index, collect(raw)
end

function oidatapage(frame)
    type = frame.header.type_
    return type == OIMD.PageType.DATA_PAGE ||
        type == OIMD.PageType.DATA_PAGE_V2
end

function oipagevalues(frame)
    header = frame.header
    header.type_ == OIMD.PageType.DATA_PAGE &&
        return Int64(header.data_page_header.num_values)
    header.type_ == OIMD.PageType.DATA_PAGE_V2 &&
        return Int64(header.data_page_header_v2.num_values)
    return Int64(0)
end

function oipagerows(frame, metadata::OIMD.ColumnMetaData,
        node::Parquet.SchemaNode)
    header = frame.header
    if header.type_ == OIMD.PageType.DATA_PAGE_V2
        page = header.data_page_header_v2
        count = Int(page.num_values)
        node.max_repetition_level == 0 && return Int64(page.num_rows)
        bytes = @view frame.payload[1:Int(page.repetition_levels_byte_length)]
        repetition = UInt16[]
        Parquet._levelsv2!(repetition, Parquet._byteview(bytes), count,
            Int(node.max_repetition_level), "repetition")
        return Int64(Base.count(iszero, view(repetition, 1:count)))
    end
    page = header.data_page_header
    count = Int(page.num_values)
    node.max_repetition_level == 0 && return Int64(count)
    payload = Parquet.decompress(metadata.codec, frame.payload,
        header.uncompressed_page_size)
    repetition = UInt16[]
    Parquet._levelsv1!(repetition, Parquet._byteview(payload), 1, count,
        page.repetition_level_encoding, Int(node.max_repetition_level),
        "repetition", Parquet.Limits())
    return Int64(Base.count(iszero, view(repetition, 1:count)))
end

function oiexpectedlocations(frames, metadata::OIMD.ColumnMetaData,
        node::Parquet.SchemaNode)
    output = OIMD.PageLocation[]
    row = Int64(0)
    for frame in frames
        oidatapage(frame) || continue
        size = frame.frameend - frame.offset
        @test 0 < size <= typemax(Int32)
        push!(output, OIMD.PageLocation(offset=frame.offset,
            compressed_page_size=Int32(size), first_row_index=row))
        row = Base.checked_add(row, oipagerows(frame, metadata, node))
    end
    return output, row
end

function oiinspect(bytes::Vector{UInt8}; pageindex::Bool=true)
    file = Parquet.File(bytes)
    try
        metadata = oimetadata(file)
        schema = Parquet.Schema(metadata)
        groups = []
        indexgroups = []
        physicalintervals = Tuple{Int64,Int64}[]
        indexintervals = Tuple{Int64,Int64}[]
        for group in metadata.row_groups
            frames = []
            indexes = Union{Nothing,OIMD.OffsetIndex}[]
            compressed = Int64(0)
            uncompressed = Int64(0)
            for (chunk, node) in zip(group.columns, schema.leaves)
                column = something(chunk.meta_data)
                chunkframes = oiframes(file, chunk)
                push!(frames, chunkframes)
                start = oichunkstart(column)
                stop = Base.checked_add(start, column.total_compressed_size)
                push!(physicalintervals, (start, stop))
                framecompressed = sum(frame -> frame.frameend - frame.offset,
                    chunkframes; init=Int64(0))
                frameuncompressed = sum(frame -> Int64(frame.headerlength) +
                    Int64(frame.header.uncompressed_page_size), chunkframes;
                    init=Int64(0))
                @test framecompressed == column.total_compressed_size
                @test frameuncompressed == column.total_uncompressed_size
                @test sum(oipagevalues, chunkframes; init=Int64(0)) ==
                    column.num_values
                compressed = Base.checked_add(compressed, framecompressed)
                uncompressed = Base.checked_add(uncompressed,
                    frameuncompressed)
                dataframes = filter(oidatapage, chunkframes)
                @test !isempty(dataframes)
                @test column.data_page_offset == first(dataframes).offset
                dictionaries = filter(frame ->
                    frame.header.type_ == OIMD.PageType.DICTIONARY_PAGE,
                    chunkframes)
                if isempty(dictionaries)
                    @test column.dictionary_page_offset === nothing
                else
                    @test length(dictionaries) == 1
                    @test column.dictionary_page_offset ==
                        only(dictionaries).offset
                end
                @test column.index_page_offset === nothing
                if pageindex
                    index, raw = oirawindex(bytes, file.footer.offset, chunk)
                    expected, rows = oiexpectedlocations(chunkframes, column,
                        node)
                    @test index.page_locations == expected
                    @test index.unencoded_byte_array_data_bytes === nothing
                    @test rows == group.num_rows
                    @test length(raw) == chunk.offset_index_length
                    push!(indexes, index)
                    push!(indexintervals, (Int64(chunk.offset_index_offset),
                        Int64(chunk.offset_index_offset) +
                            Int64(chunk.offset_index_length)))
                else
                    @test chunk.offset_index_offset === nothing
                    @test chunk.offset_index_length === nothing
                    push!(indexes, nothing)
                end
            end
            @test group.total_compressed_size == compressed
            @test group.total_byte_size == uncompressed
            push!(groups, frames)
            push!(indexgroups, indexes)
        end
        if !isempty(physicalintervals)
            @test first(first(physicalintervals)) == 4
            for index in 2:length(physicalintervals)
                @test physicalintervals[index - 1][2] ==
                    physicalintervals[index][1]
            end
            if pageindex
                @test last(physicalintervals)[2] == first(indexintervals)[1]
                for index in 2:length(indexintervals)
                    @test indexintervals[index - 1][2] ==
                        indexintervals[index][1]
                end
                @test last(indexintervals)[2] == file.footer.offset
            else
                @test last(physicalintervals)[2] == file.footer.offset
                @test isempty(indexintervals)
            end
        end
        return (; metadata, schema, groups, indexgroups,
            footer_offset=file.footer.offset, physicalintervals,
            indexintervals)
    finally
        close(file)
    end
end

function oiroundtrip(bytes::Vector{UInt8}, expected::NamedTuple)
    table = Parquet.Table(bytes)
    try
        for name in keys(expected)
            @test isequal(getproperty(Parquet.Tables.columns(table), name),
                getproperty(expected, name))
        end
    finally
        close(table)
    end
    return
end

function oigoldeninput()
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

function oirewritefooter(bytes::Vector{UInt8}, metadata::OIMD.FileMetaData)
    file = Parquet.File(bytes)
    offset = try
        file.footer.offset
    finally
        close(file)
    end
    output = copy(bytes[1:Int(offset)])
    footer = OITH.encode(metadata)
    append!(output, footer)
    Parquet._writelittle!(output, UInt32(length(footer)))
    append!(output, Parquet.PARQUET_MAGIC)
    return output
end

function oimetadatawithchunk(metadata::OIMD.FileMetaData, groupindex::Int,
        columnindex::Int, chunk::OIMD.ColumnChunk)
    groups = copy(metadata.row_groups)
    group = groups[groupindex]
    columns = copy(group.columns)
    columns[columnindex] = chunk
    groups[groupindex] = oireplace(group; columns=columns)
    return oireplace(metadata; row_groups=groups)
end

function oichunkrewrite(bytes::Vector{UInt8}, groupindex::Int,
        columnindex::Int; replacements...)
    metadata = oimetadata(bytes)
    chunk = metadata.row_groups[groupindex].columns[columnindex]
    replaced = oireplace(chunk; replacements...)
    return oirewritefooter(bytes, oimetadatawithchunk(metadata, groupindex,
        columnindex, replaced))
end

function oiindexraws(bytes::Vector{UInt8}, metadata::OIMD.FileMetaData)
    file = Parquet.File(bytes)
    try
        return [Any[chunk.offset_index_offset === nothing ? nothing :
            last(oirawindex(bytes, file.footer.offset, chunk))
            for chunk in group.columns] for group in metadata.row_groups]
    finally
        close(file)
    end
end

function oiphysicalend(metadata::OIMD.FileMetaData)
    stop = Int64(4)
    for group in metadata.row_groups, chunk in group.columns
        column = something(chunk.meta_data)
        start = oichunkstart(column)
        stop = max(stop, Base.checked_add(start,
            column.total_compressed_size))
    end
    return stop
end

function oirebuildsections(bytes::Vector{UInt8}; indexes=nothing,
        columns=nothing)
    metadata = oimetadata(bytes)
    indexraws = indexes === nothing ? oiindexraws(bytes, metadata) : indexes
    columnraws = columns === nothing ?
        [Any[nothing for _ in group.columns]
            for group in metadata.row_groups] : columns
    length(indexraws) == length(metadata.row_groups) ||
        throw(ArgumentError("offset-index group count differs"))
    length(columnraws) == length(metadata.row_groups) ||
        throw(ArgumentError("column-index group count differs"))
    bodyend = oiphysicalend(metadata)
    output = copy(bytes[1:Int(bodyend)])
    chunks = [copy(group.columns) for group in metadata.row_groups]
    for groupindex in eachindex(chunks)
        length(columnraws[groupindex]) == length(chunks[groupindex]) ||
            throw(ArgumentError("column-index leaf count differs"))
        for columnindex in eachindex(chunks[groupindex])
            raw = columnraws[groupindex][columnindex]
            if raw === nothing
                chunks[groupindex][columnindex] = oireplace(
                    chunks[groupindex][columnindex];
                    column_index_offset=nothing,
                    column_index_length=nothing)
                continue
            end
            encoded = raw isa AbstractVector{UInt8} ? collect(raw) :
                OITH.encode(raw)
            offset = Int64(length(output))
            append!(output, encoded)
            chunks[groupindex][columnindex] = oireplace(
                chunks[groupindex][columnindex];
                column_index_offset=offset,
                column_index_length=Int32(length(encoded)))
        end
    end
    for groupindex in eachindex(chunks)
        length(indexraws[groupindex]) == length(chunks[groupindex]) ||
            throw(ArgumentError("offset-index leaf count differs"))
        for columnindex in eachindex(chunks[groupindex])
            raw = indexraws[groupindex][columnindex]
            if raw === nothing
                chunks[groupindex][columnindex] = oireplace(
                    chunks[groupindex][columnindex];
                    offset_index_offset=nothing,
                    offset_index_length=nothing)
                continue
            end
            encoded = raw isa AbstractVector{UInt8} ? collect(raw) :
                OITH.encode(raw)
            offset = Int64(length(output))
            append!(output, encoded)
            chunks[groupindex][columnindex] = oireplace(
                chunks[groupindex][columnindex];
                offset_index_offset=offset,
                offset_index_length=Int32(length(encoded)))
        end
    end
    groups = OIMD.RowGroup[
        oireplace(group; columns=chunks[index])
        for (index, group) in enumerate(metadata.row_groups)
    ]
    rebuilt = oireplace(metadata; row_groups=groups)
    footer = OITH.encode(rebuilt)
    append!(output, footer)
    Parquet._writelittle!(output, UInt32(length(footer)))
    append!(output, Parquet.PARQUET_MAGIC)
    return output
end

function oiindexobjects(bytes::Vector{UInt8})
    metadata = oimetadata(bytes)
    file = Parquet.File(bytes)
    try
        return [Any[chunk.offset_index_offset === nothing ? nothing :
            first(oirawindex(bytes, file.footer.offset, chunk))
            for chunk in group.columns] for group in metadata.row_groups]
    finally
        close(file)
    end
end

mutable struct OITrackedSource <: Parquet.AbstractSource
    bytes::Vector{UInt8}
    closed::Bool
end

function Parquet.sourcelength(source::OITrackedSource)
    source.closed && throw(ArgumentError("tracked source is closed"))
    return Int64(length(source.bytes))
end

function Parquet.readrange(source::OITrackedSource, offset::Integer,
        count::Integer)
    source.closed && throw(ArgumentError("tracked source is closed"))
    offset >= 0 || throw(BoundsError(source.bytes, offset))
    count >= 0 || throw(ArgumentError("byte count must be nonnegative"))
    stop = Base.checked_add(Int64(offset), Int64(count))
    stop <= length(source.bytes) || throw(BoundsError(source.bytes,
        (offset, count)))
    first = Int(offset) + 1
    return @view source.bytes[first:(first + Int(count) - 1)]
end

function Parquet.close!(source::OITrackedSource)
    source.closed = true
    return
end

function oireject(bytes::Vector{UInt8}, type::Type{<:Exception}=Parquet.FormatError)
    @test_throws type Parquet.Table(bytes)
    source = OITrackedSource(copy(bytes), false)
    @test_throws type Parquet.Table(source)
    @test source.closed
    return
end

function oiframe(payload::Vector{UInt8}; type=OIMD.PageType.INDEX_PAGE,
        data=nothing, index=OIMD.IndexPageHeader(), dictionary=nothing,
        datav2=nothing, checksum::Bool=true, compressed=length(payload),
        uncompressed=compressed)
    crc = checksum ? reinterpret(Int32, Parquet.pagechecksum(payload)) :
        nothing
    header = OIMD.PageHeader(type_=type,
        uncompressed_page_size=Int32(uncompressed),
        compressed_page_size=Int32(compressed), crc=crc,
        data_page_header=data, index_page_header=index,
        dictionary_page_header=dictionary,
        data_page_header_v2=datav2)
    return vcat(OITH.encode(header), payload)
end

function oidatav1(value::Int32)
    payload = Parquet.encode_plain(Int32[value])
    header = OIMD.DataPageHeader(num_values=Int32(1),
        encoding=OIMD.Encoding.PLAIN,
        definition_level_encoding=OIMD.Encoding.RLE,
        repetition_level_encoding=OIMD.Encoding.RLE)
    return oiframe(payload; type=OIMD.PageType.DATA_PAGE, data=header,
        index=nothing)
end

function oilegacyfile(frames::Vector{Vector{UInt8}};
        indexposition=nothing, dictionaryposition=nothing)
    offsets = Int64[]
    parsed = []
    offset = Int64(4)
    values = Int64(0)
    rows = Int64(0)
    locations = OIMD.PageLocation[]
    uncompressed = Int64(0)
    for raw in frames
        source = Parquet.source(raw)
        frame = Parquet.readpage(source, Int64(0), Int64(length(raw)),
            Parquet.Limits())
        push!(offsets, offset)
        push!(parsed, frame.header)
        uncompressed = Base.checked_add(uncompressed,
            Int64(frame.headerlength) +
                Int64(frame.header.uncompressed_page_size))
        if Parquet.pagekind(frame) in (:data_v1, :data_v2)
            push!(locations, OIMD.PageLocation(offset=offset,
                compressed_page_size=Int32(length(raw)),
                first_row_index=rows))
            pagevalues = Parquet._pageentrycount(frame)
            values = Base.checked_add(values, pagevalues)
            pagerows = frame.header.type_ == OIMD.PageType.DATA_PAGE ?
                pagevalues : Int64(frame.header.data_page_header_v2.num_rows)
            rows = Base.checked_add(rows, pagerows)
        end
        offset = Base.checked_add(offset, Int64(length(raw)))
    end
    datapos = findfirst(header -> header.type_ in
        (OIMD.PageType.DATA_PAGE, OIMD.PageType.DATA_PAGE_V2), parsed)
    datapos === nothing && throw(ArgumentError("legacy fixture needs data"))
    dataoffset = offsets[datapos]
    indexoffset = indexposition === nothing ? nothing :
        offsets[indexposition]
    dictionaryoffset = dictionaryposition === nothing ? nothing :
        offsets[dictionaryposition]
    column = OIMD.ColumnMetaData(type_=OIMD.Type.INT32,
        encodings=OIMD.Encoding.T[OIMD.Encoding.PLAIN, OIMD.Encoding.RLE],
        path_in_schema=["value"], codec=OIMD.CompressionCodec.UNCOMPRESSED,
        num_values=values, total_uncompressed_size=uncompressed,
        total_compressed_size=offset - 4, data_page_offset=dataoffset,
        index_page_offset=indexoffset,
        dictionary_page_offset=dictionaryoffset)
    index = OIMD.OffsetIndex(page_locations=locations)
    body = vcat(Parquet.PARQUET_MAGIC, frames...)
    indexraw = OITH.encode(index)
    indexoffset = Int64(length(body))
    append!(body, indexraw)
    chunk = OIMD.ColumnChunk(meta_data=column,
        offset_index_offset=indexoffset,
        offset_index_length=Int32(length(indexraw)))
    group = OIMD.RowGroup(columns=OIMD.ColumnChunk[chunk],
        total_byte_size=uncompressed, num_rows=rows,
        total_compressed_size=offset - 4, file_offset=Int64(4),
        ordinal=Int16(0))
    schema = OIMD.SchemaElement[
        OIMD.SchemaElement(name="schema", num_children=Int32(1)),
        OIMD.SchemaElement(name="value", type_=OIMD.Type.INT32,
            repetition_type=OIMD.FieldRepetitionType.REQUIRED),
    ]
    metadata = OIMD.FileMetaData(version=Int32(1), schema=schema,
        num_rows=rows, row_groups=OIMD.RowGroup[group],
        created_by="Parquet.jl N4-C test")
    footer = OITH.encode(metadata)
    append!(body, footer)
    Parquet._writelittle!(body, UInt32(length(footer)))
    append!(body, Parquet.PARQUET_MAGIC)
    return body
end

function oirewritepageheader(transform, bytes::Vector{UInt8},
        offset::Int64)
    file = Parquet.File(bytes)
    try
        frame = Parquet.readpage(file.source, offset, file.footer.offset,
            Parquet.Limits())
        header = transform(frame.header)
        encoded = OITH.encode(header)
        length(encoded) == frame.headerlength || throw(ArgumentError(
            "replacement PageHeader changes its serialized length"))
        output = copy(bytes)
        first = Int(offset) + 1
        copyto!(output, first, encoded, 1, length(encoded))
        return output
    finally
        close(file)
    end
end

function oierror(f)
    try
        f()
    catch err
        return err
    end
    return nothing
end

const OICODECS = (:uncompressed, :snappy, :gzip, :brotli, :zstd,
    :lz4_raw)

@testset "offset-index writer default and opt-out" begin
    input = oigoldeninput()
    default = Parquet._encodefile(input; rowgroupsize=2, pagesize=1)
    explicit = Parquet._encodefile(input; rowgroupsize=2, pagesize=1,
        pageindex=true)
    disabled = Parquet._encodefile(input; rowgroupsize=2, pagesize=1,
        pageindex=false)
    @test default == explicit
    indexed = oiinspect(default)
    unindexed = oiinspect(disabled; pageindex=false)
    @test [group.num_rows for group in indexed.metadata.row_groups] ==
        Int64[2, 2, 1]
    @test indexed.physicalintervals == unindexed.physicalintervals
    lastpage = Int(last(indexed.physicalintervals)[2])
    @test default[1:lastpage] == disabled[1:lastpage]
    oiroundtrip(default, input)
    oiroundtrip(disabled, input)

    empty = (id=Int32[], days=Vector{Union{Missing,Date}}[])
    emptydefault = Parquet._encodefile(empty)
    emptydisabled = Parquet._encodefile(empty; pageindex=false)
    @test emptydefault == emptydisabled
    emptyresult = oiinspect(emptydefault)
    @test emptyresult.metadata.num_rows == 0
    @test isempty(emptyresult.metadata.row_groups)
    @test emptyresult.footer_offset == 4
    oiroundtrip(emptydefault, empty)
end

@testset "offset-index V1 V2 codec and nested row-group matrix" begin
    input = oigoldeninput()
    expectedrows = (Int64[0, 1], Int64[0, 1], Int64[0])
    expecteddayvalues = (Int64[1, 1], Int64[1, 3], Int64[1])
    for pageversion in (:v1, :v2), codec in OICODECS
        bytes = Parquet._encodefile(input; rowgroupsize=2, pagesize=1,
            pageversion=pageversion, codec=codec)
        result = oiinspect(bytes)
        @test length(result.metadata.row_groups) == 3
        for groupindex in 1:3
            for leafindex in 1:2
                locations = result.indexgroups[groupindex][leafindex].page_locations
                @test [location.first_row_index for location in locations] ==
                    expectedrows[groupindex]
            end
            dayframes = filter(oidatapage,
                result.groups[groupindex][2])
            @test oipagevalues.(dayframes) ==
                expecteddayvalues[groupindex]
        end
        oiroundtrip(bytes, input)
    end
end

@testset "offset-index dictionary pages are excluded" begin
    text = "same-value-" * repeat("x", 64)
    input = (; value=fill(text, 32))
    for pageversion in (:v1, :v2), codec in OICODECS
        bytes = Parquet._encodefile(input; rowgroupsize=16, pagesize=128,
            pageversion=pageversion, codec=codec, dictionary=true,
            checksum=false)
        result = oiinspect(bytes)
        @test length(result.metadata.row_groups) == 2
        for groupindex in 1:2
            frames = result.groups[groupindex][1]
            @test first(frames).header.type_ ==
                OIMD.PageType.DICTIONARY_PAGE
            locations = only(result.indexgroups[groupindex]).page_locations
            @test length(locations) == count(oidatapage, frames)
            @test all(location -> location.offset != first(frames).offset,
                locations)
            @test first(locations).offset ==
                something(result.metadata.row_groups[groupindex].columns[1].meta_data).data_page_offset
        end
        oiroundtrip(bytes, input)
    end

    mixed = (; value=vcat(fill("same", 128),
        ["unique-$(lpad(index, 4, '0'))" for index in 1:128]))
    bytes = Parquet._encodefile(mixed; rowgroupsize=128,
        pagesize=64, dictionary=true)
    result = oiinspect(bytes)
    @test result.metadata.row_groups[1].columns[1].meta_data.dictionary_page_offset !==
        nothing
    @test result.metadata.row_groups[2].columns[1].meta_data.dictionary_page_offset ===
        nothing
    @test all(index -> index !== nothing, Iterators.flatten(
        result.indexgroups))
    oiroundtrip(bytes, mixed)
end

@testset "legacy INDEX_PAGE and unknown framed pages" begin
    data1 = oidatav1(Int32(1))
    data2 = oidatav1(Int32(2))
    index = oiframe(UInt8[0x10, 0x20])
    unknown = oiframe(UInt8[0x30, 0x40]; type=OIMD.PageType.T(9),
        index=nothing)

    leading = oilegacyfile([index, data1]; indexposition=1)
    oiroundtrip(leading, (; value=Int32[1]))
    leadingmetadata = oimetadata(leading)
    leadingchunk = leadingmetadata.row_groups[1].columns[1]
    @test leadingchunk.meta_data.index_page_offset == 4
    @test first(oiindexobjects(leading)[1][1].page_locations).offset ==
        4 + length(index)

    interleaved = oilegacyfile([data1, index, data2]; indexposition=2)
    oiroundtrip(interleaved, (; value=Int32[1, 2]))
    interleavedindex = oiindexobjects(interleaved)[1][1]
    @test [location.offset for location in interleavedindex.page_locations] ==
        Int64[4, 4 + length(data1) + length(index)]
    @test all(location -> location.offset != 4 + length(data1),
        interleavedindex.page_locations)

    unknownfile = oilegacyfile([data1, unknown, data2])
    oiroundtrip(unknownfile, (; value=Int32[1, 2]))
    unknownindex = oiindexobjects(unknownfile)[1][1]
    @test [location.offset for location in unknownindex.page_locations] ==
        Int64[4, 4 + length(data1) + length(unknown)]

    dictionary = oiframe(Parquet.encode_plain(Int32[9]);
        type=OIMD.PageType.DICTIONARY_PAGE, index=nothing,
        dictionary=OIMD.DictionaryPageHeader(num_values=Int32(1),
            encoding=OIMD.Encoding.PLAIN))
    dictionaryfirst = oilegacyfile([dictionary, index, data1];
        indexposition=2, dictionaryposition=1)
    oiroundtrip(dictionaryfirst, (; value=Int32[1]))
    indexbeforedictionary = oilegacyfile([index, dictionary, data1];
        indexposition=1, dictionaryposition=2)
    oireject(indexbeforedictionary)

    validtail = oilegacyfile([data1, index]; indexposition=2)
    corruptcrc = copy(validtail)
    corruptcrc[4 + length(data1) + length(index)] ⊻= 0x01
    oireject(corruptcrc)
    indexoffset = Int64(4 + length(data1))
    truncated = oirewritepageheader(validtail, indexoffset) do header
        return oireplace(header;
            compressed_page_size=header.compressed_page_size + Int32(1))
    end
    oireject(truncated)
    oireject(oichunkrewrite(leading, 1, 1;
        meta_data=oireplace(something(leadingchunk.meta_data);
            index_page_offset=Int64(0))))
    # parquet-cpp before 1.4 (pyarrow < 0.9) wrote index_page_offset=0 for "absent".
    plain = oilegacyfile([data1, data2])
    plainchunk = oimetadata(plain).row_groups[1].columns[1]
    oiroundtrip(oichunkrewrite(plain, 1, 1;
        meta_data=oireplace(something(plainchunk.meta_data);
            index_page_offset=Int64(0))), (; value=Int32[1, 2]))
    oireject(oichunkrewrite(leading, 1, 1;
        meta_data=oireplace(something(leadingchunk.meta_data);
            index_page_offset=Int64(-1))))
    oireject(oichunkrewrite(leading, 1, 1;
        meta_data=oireplace(something(leadingchunk.meta_data);
            index_page_offset=something(leadingchunk.meta_data).data_page_offset)))
end

@testset "offset-index accepts empty data pages" begin
    # Arrow C++ writes data pages with no values, e.g. at the end of a chunk.
    emptyv1 = oiframe(UInt8[]; type=OIMD.PageType.DATA_PAGE, index=nothing,
        data=OIMD.DataPageHeader(num_values=Int32(0),
            encoding=OIMD.Encoding.PLAIN,
            definition_level_encoding=OIMD.Encoding.RLE,
            repetition_level_encoding=OIMD.Encoding.RLE))
    emptyv2 = oiframe(UInt8[]; type=OIMD.PageType.DATA_PAGE_V2, index=nothing,
        datav2=OIMD.DataPageHeaderV2(num_values=Int32(0), num_nulls=Int32(0),
            num_rows=Int32(0), encoding=OIMD.Encoding.PLAIN,
            definition_levels_byte_length=Int32(0),
            repetition_levels_byte_length=Int32(0), is_compressed=false))
    for empty in (emptyv1, emptyv2)
        bytes = oilegacyfile([empty, oidatav1(Int32(1)), empty,
            oidatav1(Int32(2)), empty])
        table = Parquet.Table(bytes)
        @test Parquet.Tables.columns(table).value == Int32[1, 2]
        close(table)
    end
end

@testset "offset-index V1 V2 row and value semantics" begin
    E = Union{Missing,Date}
    nested = (; days=Union{Missing,Vector{E}}[
        E[Date(2000, 1, 1), Date(2001, 1, 1), Date(2002, 1, 1)],
        E[Date(2003, 1, 1)],
        E[Date(2004, 1, 1)],
    ])
    v1 = Parquet._encodefile(nested; pageversion=:v1, pagesize=40,
        rowgroupsize=nothing, checksum=false)
    v1result = oiinspect(v1)
    @test [location.first_row_index for location in
        v1result.indexgroups[1][1].page_locations] == Int64[0, 1]
    @test [oipagerows(frame,
        something(v1result.metadata.row_groups[1].columns[1].meta_data),
        only(v1result.schema.leaves)) for frame in
        filter(oidatapage, v1result.groups[1][1])] == Int64[1, 2]
    oiroundtrip(v1, nested)

    v2 = Parquet._encodefile(nested; pageversion=:v2, pagesize=40,
        rowgroupsize=nothing, checksum=false)
    result = oiinspect(v2)
    frames = filter(oidatapage, result.groups[1][1])
    @test [frame.header.data_page_header_v2.num_rows for frame in frames] ==
        Int32[1, 2]
    swapped = oirewritepageheader(v2, frames[1].offset) do header
        page = oireplace(header.data_page_header_v2; num_rows=Int32(2))
        return oireplace(header; data_page_header_v2=page)
    end
    swapped = oirewritepageheader(swapped, frames[2].offset) do header
        page = oireplace(header.data_page_header_v2; num_rows=Int32(1))
        return oireplace(header; data_page_header_v2=page)
    end
    swappedindex = oiindexobjects(swapped)[1][1]
    swappedlocations = copy(swappedindex.page_locations)
    swappedlocations[2] = oireplace(swappedlocations[2];
        first_row_index=Int64(2))
    oireject(oirebuildsections(swapped; indexes=[Any[
        oireplace(swappedindex; page_locations=swappedlocations)] ]))

    changedvalues = oirewritepageheader(v2, frames[1].offset) do header
        page = oireplace(header.data_page_header_v2;
            num_values=header.data_page_header_v2.num_values + Int32(1))
        return oireplace(header; data_page_header_v2=page)
    end
    oireject(changedvalues)

    flat = Parquet._encodefile((value=Int32[1, 2, 3],);
        pageversion=:v2, pagesize=nothing, checksum=false)
    flatresult = oiinspect(flat)
    flatoffset = only(only(flatresult.indexgroups)).page_locations[1].offset
    wrongflat = oirewritepageheader(flat, flatoffset) do header
        page = oireplace(header.data_page_header_v2; num_rows=Int32(2))
        return oireplace(header; data_page_header_v2=page)
    end
    oireject(wrongflat)
end

@testset "offset-index writer limits" begin
    probe = (; value=Int32.(1:64))
    indexed = Parquet._encodefile(probe; pagesize=4, checksum=false)
    metadata = oimetadata(indexed)
    lengths = Int64[chunk.offset_index_length for group in
        metadata.row_groups for chunk in group.columns]
    total = sum(lengths; init=Int64(0))
    @test total > 0
    @test Parquet._encodefile(probe; pagesize=4, checksum=false,
        limits=Parquet.Limits(max_page_index_bytes=total)) == indexed
    writerlimit = oierror() do
        Parquet._encodefile(probe; pagesize=4, checksum=false,
            limits=Parquet.Limits(max_page_index_bytes=total - 1))
    end
    @test writerlimit isa Parquet.LimitError
    @test writerlimit.resource == :page_index_bytes

    index = oiindexobjects(indexed)[1][1]
    exact = Parquet._writeoffsetindexencodedsize(index)
    encoded, cumulativebytes = Parquet._writeencodeoffsetindex(index,
        Int64(0), Parquet.Limits())
    @test length(encoded) == exact == cumulativebytes
end

@testset "offset-index public failure atomicity" begin
    input = (; value=Int32[1, 2, 3, 4])
    bytes = Parquet._encodefile(input; pagesize=4, checksum=false)
    limits = Parquet.Limits(max_page_index_bytes=1)
    io = IOBuffer()
    error = oierror() do
        Parquet.write(io, input; pagesize=4, limits=limits)
    end
    @test error isa Parquet.LimitError
    @test isempty(take!(io))
    mktempdir() do directory
        newpath = joinpath(directory, "new.parquet")
        error = oierror() do
            Parquet.write(newpath, input; pagesize=4, limits=limits)
        end
        @test error isa Parquet.LimitError
        @test !ispath(newpath)
        existing = joinpath(directory, "existing.parquet")
        sentinel = UInt8[0x73, 0x61, 0x66, 0x65]
        open(existing, "w") do output
            write(output, sentinel)
        end
        error = oierror() do
            Parquet.write(existing, input; pagesize=4, limits=limits)
        end
        @test error isa Parquet.LimitError
        @test read(existing) == sentinel
    end
end

@testset "column-index bytes are outside page-index budgets" begin
    input = (; left=Int32[1, 2], right=Int32[3, 4])
    bytes = Parquet._encodefile(input; pagesize=4)
    metadata = oimetadata(bytes)
    total = sum(Int64(chunk.offset_index_length) for group in
        metadata.row_groups for chunk in group.columns; init=Int64(0))
    columnraws = [Any[fill(UInt8(0xaa), 16_384),
        fill(UInt8(0xbb), 16_384)]]
    withcolumns = oirebuildsections(bytes; columns=columnraws)
    table = Parquet.Table(withcolumns; limits=Parquet.Limits(
        max_page_index_bytes=total))
    try
        @test Parquet.Tables.columns(table) == input
    finally
        close(table)
    end
end
