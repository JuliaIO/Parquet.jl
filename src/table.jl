import DataAPI, Dates, Tables

# `Table <: Tables.AbstractColumns`, so `table.x` is column `x`; the package reads the
# fields below through `getfield` (the `_table*` accessors). A table owns its columns and
# holds no file: reading copies every value out of the source, which is then released.
# `colmetadata` holds each column's key-value metadata, in column order.
struct Table{C<:NamedTuple} <: Tables.AbstractColumns
    metadata::Metadata.FileMetaData
    schema::Schema
    columns::C
    rows::Int
    colmetadata::Vector{Vector{Pair{String,String}}}
end

function Table(metadata::Metadata.FileMetaData, schema::Schema, columns::NamedTuple,
        rows::Int)
    return Table(metadata, schema, columns, rows,
        [Pair{String,String}[] for _ in 1:length(columns)])
end

_tablecolmetadata(table::Table) = getfield(table, :colmetadata)

function _tablecolumns(table::Table)
    return getfield(table, :columns)
end

function _tablemetadata(table::Table)
    return getfield(table, :metadata)
end

function _tableschema(table::Table)
    return getfield(table, :schema)
end

function _tablerows(table::Table)
    return getfield(table, :rows)
end

function _writekeyvaluemetadata(table::Table, limits::Limits)
    metadata = _tablemetadata(table).key_value_metadata
    metadata === nothing && return nothing
    return _rewritekeyvaluemetadata(metadata, limits)
end

function _tablenames(schema::Schema, limits::Limits)
    return _internschemanames(String[node.element.name for node in schema.root.children],
        limits)
end

function _validatetablerowgroupmetadata(group::Metadata.RowGroup,
        rowindex::Int, leafcount::Int, footeroffset::Int64)
    group.total_byte_size >= 0 || throw(FormatError(
        "row group $rowindex has a negative total byte size"))
    compressed = group.total_compressed_size
    compressed === nothing || compressed >= 0 || throw(FormatError(
        "row group $rowindex has a negative compressed byte size"))
    offset = group.file_offset
    if offset !== nothing
        offset >= 0 || throw(FormatError(
            "row group $rowindex has a negative file offset"))
        (iszero(offset) || 4 <= offset < footeroffset) || throw(FormatError(
            "row group $rowindex file offset $offset is outside the file body"))
    end
    ordinal = group.ordinal
    ordinal === nothing || ordinal >= 0 || throw(FormatError(
        "row group $rowindex has a negative ordinal"))
    sorting = group.sorting_columns
    sorting === nothing && return
    for column in sorting
        index = Int64(column.column_idx)
        0 <= index < leafcount || throw(FormatError(
            "row group $rowindex sorting column index $index is outside " *
            "the schema leaf range"))
    end
    return
end

function _validatetablechunkranges(metadata::Metadata.FileMetaData,
        schema::Schema, footeroffset::Int64)
    leafcount = length(schema.leaves)
    for (rowindex, group) in enumerate(metadata.row_groups)
        _validatetablerowgroupmetadata(group, rowindex, leafcount,
            footeroffset)
        length(group.columns) == leafcount || throw(FormatError(
            "row group $rowindex has $(length(group.columns)) columns for " *
            "$leafcount schema leaves"))
        for (columnindex, (chunk, leaf)) in enumerate(zip(group.columns,
                schema.leaves))
            chunk.file_offset >= 0 || throw(FormatError(
                "row group $rowindex column chunk $columnindex has a " *
                "negative file offset"))
            _chunkrange(_chunkmetadata(chunk, leaf), footeroffset)
        end
    end
    return
end

# Tasks that decode `count` work items: up to `ntasks`, or one when the source does not
# allow concurrent reads.
function _readtasks(file::File, ntasks::Int, count::Int)
    ntasks >= 1 || throw(ArgumentError("ntasks must be at least 1, got $ntasks"))
    concurrentreads(file.source) || return 1
    return max(1, min(ntasks, count))
end

struct _ReadFailure
    error::Any
end

mutable struct _ReadQueue
    @atomic next::Int
end

# `Any[f(index, buffers) for index in indices]`, where `buffers` are the `_PageBuffers`
# of the task that runs `f`. With `ntasks > 1`, that many tasks take the indices in
# order. The result is a `Vector{Any}` for any `ntasks`, so callers compile once rather
# than for each combination of result types. An error from `f` is rethrown as itself
# rather than in a TaskFailedException.
function _readmap(f, indices, ntasks::Int)
    if ntasks <= 1
        buffers = _PageBuffers()
        results = Vector{Any}(undef, length(indices))
        for (position, index) in enumerate(indices)
            results[position] = f(index, buffers)
        end
        return results
    end
    items = collect(indices)
    results = Vector{Any}(undef, length(items))
    queue = _ReadQueue(1)
    workers = map(1:min(ntasks, length(items))) do _
        return errormonitor(Threads.@spawn _readworker(f, items, results, queue))
    end
    foreach(wait, workers)
    for result in results
        result isa _ReadFailure && throw(result.error)
    end
    return results
end

function _readworker(f, items::Vector, results::Vector{Any}, queue::_ReadQueue)
    buffers = _PageBuffers()
    while true
        index = (@atomic queue.next += 1) - 1
        index > length(items) && return
        results[index] = try
            f(items[index], buffers)
        catch err
            _ReadFailure(err)
        end
    end
end

# Index of each row group's first row in the table, minus one.
function _rowgroupstarts(metadata::Metadata.FileMetaData, rows::Int)
    starts = Vector{Int}(undef, length(metadata.row_groups))
    total = 0
    for (rowindex, group) in enumerate(metadata.row_groups)
        starts[rowindex] = total
        total += Int(group.num_rows)
    end
    total == rows || throw(FormatError(
        "row groups contain $total rows but $rows were validated"))
    return starts
end

# Whether every chunk of a flat field declares no more rows than its bytes could encode
# (`_claimcapacity`). Only then is the column allocated at full length up front, so its
# chunks can decode into it in any order.
function _flatchunksbacked(metadata::Metadata.FileMetaData, node::SchemaNode)
    index = Int(node.column_index)
    for group in metadata.row_groups
        md = group.columns[index].meta_data
        md === nothing && return false
        group.num_rows <= _claimcapacity(group.num_rows, md.total_compressed_size) ||
            return false
    end
    return true
end

function _flatcolumnvector(node::SchemaNode, rows::Int, limits::Limits,
        options::_ReadOptions)
    _, L = _valueconverter(node, limits, options)
    return Vector{_flatoutputtype(L, node)}(undef, rows)
end

# One row group's chunk of a flat field, decoded into `column[(offset + 1):]`. Returns
# the page buffers a STRING chunk's payloads point into, or `nothing`.
function _readtablechunk!(column::Vector, offset::Int, file::File,
        metadata::Metadata.FileMetaData, rowindex::Int, node::SchemaNode,
        limits::Limits, budget::_ByteBudget, buffers::_PageBuffers, options::_ReadOptions)
    group = metadata.row_groups[rowindex]
    md = _chunkmetadata(group.columns[Int(node.column_index)], node)
    md.num_values == group.num_rows || throw(FormatError(
        "flat column $(node.column_index) has $(md.num_values) values for " *
        "$(group.num_rows) rows"))
    start, stop = _chunkrange(md, file.footer.offset)
    convert, _ = _valueconverter(node, limits, options)
    _readflatchunk!(column, offset, _physicaleltype(md.type_), convert, file.source, md,
        node, start, stop, limits, budget, buffers; checksum=options.checksum)
    return convert isa _StringPayloads ? convert.buffers : nothing
end

# A STRING (or binary) column from its payloads and each row group's page buffers.
# Payload buffer indices count from the row group's first buffer, so later row groups
# are rebased.
function _payloadcolumn(payloads::Vector{DataStrings.StringPayload}, node::SchemaNode,
        chunks::Vector{Vector{Vector{UInt8}}}, starts::Vector{Int}, stringtype::Type)
    pages = Vector{UInt8}[]
    for (rowindex, chunk) in enumerate(chunks)
        base = length(pages)
        if base > 0 && !isempty(chunk)
            last = rowindex == length(starts) ? length(payloads) : starts[rowindex + 1]
            for index in (starts[rowindex] + 1):last
                payload = payloads[index]
                DataStrings.payloadlength(payload) > DataStrings.INLINE_MAX || continue
                buffer = UInt64(DataStrings.payloadbufidx(payload) + base)
                payloads[index] = DataStrings.StringPayload(payload.a,
                    (payload.b & 0xffffffff00000000) | buffer)
            end
        end
        append!(pages, chunk)
    end
    return _stringcolumn(_payloadvector(node, node.max_definition_level > 0, payloads,
        pages), stringtype)
end

# A StringVector for STRING and ENUM values, a BytesVector for other byte arrays.
function _payloadvector(node::SchemaNode, optional::Bool,
        payloads::Vector{DataStrings.StringPayload}, pages::Vector{Vector{UInt8}})
    if _logicalkind(node) in (:string, :enum)
        T = optional ? Union{Missing,DataStrings.DataString} : DataStrings.DataString
        return DataStrings.StringVector{T}(payloads, pages, Val(:trusted))
    end
    T = optional ? Union{Missing,DataStrings.DataBytes} : DataStrings.DataBytes
    return DataStrings.BytesVector{T}(payloads, pages, Val(:trusted))
end

function _finishflatcolumn(column::Vector, node::SchemaNode, chunks, starts::Vector{Int},
        stringtype::Type)
    column isa Vector{DataStrings.StringPayload} || return column
    return _payloadcolumn(column, node, chunks, starts, stringtype)
end

# Columns of a table whose fields are all flat leaves. Each chunk decodes straight into
# its column, one task per chunk; a column whose chunks are not all backed by their
# bytes grows as its chunks decode in order, on one task.
function _readflattable(file::File, metadata::Metadata.FileMetaData, schema::Schema,
        rows::Int, limits::Limits, budget::_ByteBudget, ntasks::Int=Threads.nthreads(),
        options::_ReadOptions=_ReadOptions())
    fields = schema.root.children
    starts = _rowgroupstarts(metadata, rows)
    # STRING page buffers, per field and row group.
    pages = [Vector{Vector{UInt8}}[Vector{UInt8}[] for _ in starts] for _ in fields]
    tasks = Tuple{Int,Int}[]
    backed = falses(length(fields))
    for (fieldindex, node) in enumerate(fields)
        node.max_definition_level in (0, 1) ||
            throw(FormatError("nested definition levels are not supported yet"))
        backed[fieldindex] = _flatchunksbacked(metadata, node)
        if backed[fieldindex]
            append!(tasks, (fieldindex, rowindex) for rowindex in eachindex(starts))
        else
            push!(tasks, (fieldindex, 0))
        end
    end
    # Allocating a column zeroes part of it, so columns allocate on the read's tasks too.
    columns = _readmap(eachindex(fields), _readtasks(file, ntasks, length(fields))) do fieldindex, _
        column = _flatcolumnvector(fields[fieldindex], backed[fieldindex] ? rows : 0,
            limits, options)
        backed[fieldindex] || sizehint!(column,
            _claimcapacity(rows, _checkedsourcelength(file.source)))
        return column
    end
    _readmap(tasks, _readtasks(file, ntasks, length(tasks))) do (fieldindex, rowindex), buffers
        column = columns[fieldindex]
        node = fields[fieldindex]
        for index in (rowindex == 0 ? eachindex(starts) : (rowindex:rowindex))
            offset = rowindex == 0 ? length(column) : starts[index]
            kept = _readtablechunk!(column, offset, file, metadata, index, node, limits,
                budget, buffers, options)
            kept === nothing || (pages[fieldindex][index] = kept)
        end
        return nothing
    end
    return _readmap(eachindex(fields), _readtasks(file, ntasks, length(fields))) do fieldindex, _
        node = fields[fieldindex]
        column = columns[fieldindex]
        length(column) == rows || throw(FormatError(
            "flat column $(node.column_index) has $(length(column)) values for $rows rows"))
        return _finishflatcolumn(column, node, pages[fieldindex], starts,
            options.stringtype)
    end
end

# Copy of a Thrift struct with some fields replaced.
function _withfields(value::T; fields...) where {T}
    return T((haskey(fields, name) ? fields[name] : getfield(value, name)
        for name in fieldnames(T))...)
end

# Index of the last element in the preorder subtree that starts at `index`.
function _subtreeend(elements::Vector{Metadata.SchemaElement}, index::Int)
    last = index
    for _ in 1:something(elements[index].num_children, Int32(0))
        last = _subtreeend(elements, last + 1)
    end
    return last
end

# File metadata and schema restricted to the top-level fields at `positions`, in that
# order and renamed to `names`. Row groups keep only those fields' column chunks, so
# the chunks of every other field are never read.
function _selectfields(metadata::Metadata.FileMetaData, schema::Schema,
        positions::Vector{Int}, names::Vector{String}, limits::Limits)
    elements = metadata.schema
    leafcount = length(schema.leaves)
    elementranges = UnitRange{Int}[]
    leafranges = UnitRange{Int}[]
    element, leaf = 2, 1
    for child in schema.root.children
        last = _subtreeend(elements, element)
        count = _writeschemaleafcount(child)
        push!(elementranges, element:last)
        push!(leafranges, leaf:(leaf + count - 1))
        element, leaf = last + 1, leaf + count
    end
    selected = Metadata.SchemaElement[_withfields(first(elements);
        num_children=Int32(length(positions)))]
    leaves = Int[]
    # The chunk paths of a renamed field start with its new name.
    renamed = Dict{Int,String}()
    for (position, name) in zip(positions, names)
        range = elementranges[position]
        element = elements[first(range)]
        push!(selected, _withfields(element; name=name))
        append!(selected, @view elements[(first(range) + 1):last(range)])
        if element.name != name
            for offset in eachindex(leafranges[position])
                renamed[length(leaves) + offset] = name
            end
        end
        append!(leaves, leafranges[position])
    end
    groups = map(enumerate(metadata.row_groups)) do (rowindex, group)
        length(group.columns) == leafcount || throw(FormatError(
            "row group $rowindex has $(length(group.columns)) columns for " *
            "$leafcount schema leaves"))
        columns = group.columns[leaves]
        for (index, name) in renamed
            columns[index] = _renamedchunk(columns[index], name)
        end
        return _withfields(group; columns=columns, sorting_columns=nothing)
    end
    orders = metadata.column_orders
    if orders !== nothing
        length(orders) == leafcount || throw(FormatError(
            "file declares $(length(orders)) column orders for $leafcount leaves"))
        orders = orders[leaves]
    end
    selectedmetadata = _withfields(metadata; schema=selected, row_groups=groups,
        column_orders=orders)
    return selectedmetadata, Schema(selected; limits=limits)
end

# `chunk` with the first component of its column path replaced by `name`.
function _renamedchunk(chunk::Metadata.ColumnChunk, name::String)
    md = chunk.meta_data
    (md === nothing || isempty(md.path_in_schema)) && return chunk
    path = copy(md.path_in_schema)
    path[1] = name
    return _withfields(chunk; meta_data=_withfields(md; path_in_schema=path))
end

# File metadata and schema whose top-level fields have unique names: a repeated name
# gets a suffix (`_uniquenames`), and its column chunks' paths follow.
function _uniquefields(metadata::Metadata.FileMetaData, schema::Schema, limits::Limits)
    names = String[node.element.name for node in schema.root.children]
    unique = _uniquenames(names)
    unique === names && return metadata, schema
    return _selectfields(metadata, schema, collect(eachindex(names)), unique, limits)
end

function _isflattableplan(plan::_NestedSchemaPlan)
    for child in plan.root.children
        child isa _NestedLeafPlan || return false
        child.source.max_repetition_level == 0 || return false
    end
    return true
end

# The read keywords of a file resolved against its schema: the scan bound to the file's
# top-level columns, and the file metadata and schema restricted to the columns the scan
# reads (selected or filtered on). Without a scan every column is read.
struct _ReadPlan
    bound::Union{Nothing,Tables.BoundScan}
    metadata::Metadata.FileMetaData
    schema::Schema
    ntasks::Int
    options::_ReadOptions
    limits::Limits
    colmetadata::Vector{Vector{Pair{String,String}}}
end

# The key-value metadata of each top-level field of a file: its ARROW:schema field
# metadata (where pyarrow and Polars keep it), then that of its first column chunk in the
# first row group (where `Parquet.write` stores `colmetadata`).
function _filecolmetadata(metadata::Metadata.FileMetaData, schema::Schema, limits::Limits)
    fields = schema.root.children
    result = [Pair{String,String}[] for _ in fields]
    entries = something(metadata.key_value_metadata, Metadata.KeyValue[])
    index = findlast(entry -> entry.key == "ARROW:schema", entries)
    if index !== nothing && entries[index].value !== nothing
        arrow = try
            _decodearrowschema(entries[index].value, limits)
        catch err
            err isa Union{FormatError,LimitError} || rethrow()
            nothing
        end
        if arrow !== nothing && length(arrow) == length(fields)
            for (position, (node, field)) in enumerate(zip(fields, arrow))
                node.element.name == field.name && append!(result[position], field.metadata)
            end
        end
    end
    isempty(metadata.row_groups) && return result
    columns = first(metadata.row_groups).columns
    leaf = 1
    for (position, node) in enumerate(fields)
        md = leaf <= length(columns) ? columns[leaf].meta_data : nothing
        values = md === nothing ? nothing : md.key_value_metadata
        values === nothing || append!(result[position],
            (entry.key => something(entry.value, "") for entry in values))
        leaf += _writeschemaleafcount(node)
    end
    return result
end

# The key-value metadata of each column a read with `plan` returns.
function _outputcolmetadata(plan::_ReadPlan)
    plan.bound === nothing && return plan.colmetadata
    return [plan.colmetadata[column.index] for column in plan.bound.columns]
end

# The scan the read keywords ask for: `nothing` for a whole-file read, the `scan`, or for
# the select/drop/filter/limit keywords a function of the columns' names that returns the
# scan (`_scanfor`).
function _scanrequest(select, drop, filter, limit, scan)
    sugar = select !== nothing || drop !== nothing || filter !== nothing ||
        limit !== nothing
    if scan !== nothing
        sugar && throw(ArgumentError("pass `scan` or the select/drop/filter/limit " *
            "keywords, not both; a Tables.Scan can express all of them"))
        return scan
    end
    sugar || return nothing
    select === nothing || drop === nothing || throw(ArgumentError(
        "pass `select` or `drop`, not both"))
    # The filter and limit are checked now, before the file is read.
    Tables.Scan(; filter=filter, limit=limit)
    return names -> begin
        columns = drop === nothing ? _selection(:select, something(select, Tables.All()),
            names) : Tables.Not(_selection(:drop, drop, names))
        Tables.Scan(; select=columns, filter=filter, limit=limit)
    end
end

# The scan a read request asks for over columns `names`.
_scanfor(request, names) = request isa Function ? request(names) : request

# The columns `select` or `drop` (`keyword`) names among `names`: a function `(i, name) ->
# Bool` picks them by index and name, as in CSV.jl; names, indices, and patterns must
# each match a column.
function _selection(keyword::Symbol, selection, names)
    selection isa Function &&
        return Int[index for (index, name) in enumerate(names) if selection(index, name)]
    items = selection isa Union{Symbol,AbstractString,Integer,Regex,Pair} ? (selection,) :
        selection isa Union{AbstractVector,Tuple} ? selection : ()
    for item in items
        ref = item isa Pair ? first(item) : item
        found = ref isa Union{Symbol,AbstractString} ? Symbol(ref) in names :
            ref isa Integer ? 1 <= ref <= length(names) :
            ref isa Regex ? any(name -> occursin(ref, String(name)), names) : true
        found || throw(ArgumentError("$keyword names no column $(repr(ref)); the " *
            "columns are $(join(first(names, 20), ", "))" *
            (length(names) > 20 ? ", … ($(length(names)) in all)" : "")))
    end
    return selection
end

function _readplan(metadata::Metadata.FileMetaData, schema::Schema, scan, ntasks::Int,
        options::_ReadOptions, limits::Limits)
    colmetadata = _filecolmetadata(metadata, schema, limits)
    scan === nothing && return _ReadPlan(nothing, metadata, schema, ntasks, options, limits,
        colmetadata)
    sourcenames = _tablenames(schema, limits)
    bound = Tables.resolve(_scanfor(scan, sourcenames), sourcenames)
    any(column -> column.type !== nothing, bound.columns) && throw(ArgumentError(
        "Parquet does not apply scan type overrides; convert the columns after reading"))
    positions = sort!(unique!(vcat(Int[column.index for column in bound.columns],
        bound.filtercols)))
    metadata, schema = _selectfields(metadata, schema, positions,
        String[String(sourcenames[position]) for position in positions], limits)
    types = Dict{Symbol,Type}()
    for (position, node) in zip(positions, schema.root.children)
        T = _filtertype(node, limits, options)
        T === nothing || (types[sourcenames[position]] = T)
    end
    return _ReadPlan(_bindscan(bound, types), metadata, schema, ntasks, options, limits,
        colmetadata)
end

"""
    Parquet.File(source; select=nothing, drop=nothing, filter=nothing, limit=nothing,
                 scan=nothing, ntasks=Threads.nthreads(), stringtype=DataString,
                 int96=Dates.Nanosecond, checksum=false, limits=Parquet.Limits())

Open a Parquet file and decode its footer, without reading column data. The keywords
are those of [`Parquet.Table`](@ref) and apply to every table the file yields.

- `Tables.schema(file)` and `Tables.columnnames(file)` give the columns a read returns.
- Iterating the file yields one `Parquet.Table` per row group, so `file` is its own
  `Tables.partitions`; `length(file)` and `file[i]` give the row groups directly. With a
  filter, row groups whose statistics rule it out are skipped.
  With both a filter and a `limit` (or a scan `offset`), which row groups contribute
  is known only while reading, so such a file can be iterated but not indexed.
- `DataAPI.nrow(file)` is the row count of an unfiltered read.
- `DataAPI.metadata(file)` gives the file's key-value metadata (without the
  `ARROW:schema` entry, which sets column types), `DataAPI.colmetadata(file, column)` a
  column's (its ARROW:schema field metadata and its column chunks' key-value metadata),
  and [`Parquet.metadata`](@ref) the raw footer metadata (statistics, encodings).
- As a Tables.jl source (for example `DataFrame(file)`), the file reads every row group.

`source` is a path, a byte vector, an `IO`, or a [`Parquet.AbstractSource`](@ref). A
path stays memory mapped until `close(file)`; an `AbstractSource` is never closed.
"""
function File(input; select=nothing, drop=nothing, filter=nothing, limit=nothing,
        scan::Union{Nothing,Tables.Scan}=nothing, ntasks::Integer=Threads.nthreads(),
        stringtype::Type=DataStrings.DataString, int96::Type=Dates.Nanosecond,
        checksum::Bool=false, limits::Limits=Limits(), budget::_ByteBudget=_ByteBudget(limits))
    return _openfile(input, nothing; select=select, drop=drop, filter=filter, limit=limit,
        scan=scan, ntasks=ntasks, stringtype=stringtype, int96=int96, checksum=checksum,
        limits=limits, budget=budget)
end

# A `File`; `cached` is the footer and decoded metadata of an earlier open of the same
# file, which are reused (a dataset reads each footer once).
function _openfile(input, cached::Union{Nothing,Tuple{Footer,Metadata.FileMetaData}};
        select=nothing, drop=nothing, filter=nothing, limit=nothing,
        scan::Union{Nothing,Tables.Scan}=nothing, ntasks::Integer=Threads.nthreads(),
        stringtype::Type=DataStrings.DataString, int96::Type=Dates.Nanosecond,
        checksum::Bool=false, limits::Limits=Limits(), budget::_ByteBudget=_ByteBudget(limits))
    ntasks >= 1 || throw(ArgumentError("ntasks must be at least 1, got $ntasks"))
    request = _scanrequest(select, drop, filter, limit, scan)
    options = _ReadOptions(; int96=int96, checksum=checksum, stringtype=stringtype)
    owned = !(input isa AbstractSource)
    src = source(input; budget=budget)
    try
        if cached === nothing
            footer = readfooter(src, limits)
            metadata = _decodefooter(footer, limits, budget)
            metadata.num_rows >= 0 || throw(FormatError("file metadata has a negative row count"))
            metadata.num_rows <= typemax(Int) || throw(FormatError("file row count overflows Int"))
            _checklimit(:container_elements, metadata.num_rows, limits.max_container_elements)
            metadata = _parquet816(src, metadata, footer.offset, limits)
        else
            footer, metadata = cached
            _checkedsourcelength(src) == footer.offset + footer.length + 8 || throw(
                FormatError("the file changed after its footer was read"))
        end
        schema = Schema(metadata; limits=limits)
        metadata, schema = _uniquefields(metadata, schema, limits)
        if cached === nothing
            _validatetablerowgroups(metadata, schema, limits)
            _validatetablechunkranges(metadata, schema, footer.offset)
        end
        options = _ReadOptions(options.int96, _arrowhints(metadata, schema, limits),
            _checksumpolicy(options.checksum, metadata), options.stringtype)
        plan = _readplan(metadata, schema, request, Int(ntasks), options, limits)
        file = File(src, footer, metadata, plan, owned, false)
        owned && finalizer(close!, file)
        return file
    catch
        owned && close!(src)
        rethrow()
    end
end

# parquet-mr before 1.2.9 left the dictionary page header out of a column chunk's size
# (PARQUET-816, IMPALA-694). A chunk of such a file with a dictionary page gets the size
# its pages take, found by walking their headers up to 100 bytes past the declared end, as
# Arrow C++ reads it.
function _parquet816(src::AbstractSource, metadata::Metadata.FileMetaData,
        footeroffset::Int64, limits::Limits)
    producer = _statisticsarrowproducer(metadata.created_by)
    (producer.application === :parquet_mr &&
        _statisticsversionbelow(producer.version, UInt64(1), UInt64(2), UInt64(9))) ||
        return metadata
    groups = map(metadata.row_groups) do group
        columns = map(group.columns) do chunk
            md = chunk.meta_data
            size = md === nothing ? nothing : _parquet816size(src, md, footeroffset, limits)
            size === nothing && return chunk
            return _withfields(chunk; meta_data=_withfields(md; total_compressed_size=size))
        end
        return _withfields(group; columns=columns)
    end
    return _withfields(metadata; row_groups=groups)
end

function _parquet816size(src::AbstractSource, md::Metadata.ColumnMetaData,
        footeroffset::Int64, limits::Limits)
    return try
        start = _chunkstart(md)
        stop = min(start + md.total_compressed_size + 100, footeroffset)
        position, values, dictionary = start, Int64(0), false
        while position < stop && values < md.num_values
            header, headerlength = readpageheader(src, position, stop, limits)
            kind = validatepageheader(header)
            position = _pageframeend(position, headerlength, header.compressed_page_size)
            dictionary |= kind === :dictionary
            kind === :data_v1 && (values += header.data_page_header.num_values)
            kind === :data_v2 && (values += header.data_page_header_v2.num_values)
        end
        dictionary && values == md.num_values && position <= stop ? position - start : nothing
    catch err
        err isa Union{FormatError,LimitError} || rethrow()
        nothing
    end
end

# The table of row groups `groups` (ascending indices) with the file's scan applied.
# `window` replaces the scan's (offset, limit) for these groups. A filter skips the row
# groups and pages it cannot match (`pushdown.jl`); a flat table then decodes its filter
# columns first and its other columns only for the matching rows, and a row window
# decodes only the pages it overlaps.
function _readtable(file::File, groups::AbstractVector{Int}, budget::_ByteBudget;
        window=nothing)
    plan = file.plan
    bound = plan.bound
    offset, limit = something(window, bound === nothing ? (0, nothing) :
        (bound.offset, bound.limit))
    filter = bound === nothing ? nothing : bound.filter
    nested = _nestedplan(plan.schema; limits=plan.limits)
    nested.plan_count > 0 || throw(AssertionError("nested schema plan has no root"))
    filter === nothing && iszero(offset) && limit === nothing &&
        return _readgroups(file, groups, budget, nested, offset, limit)
    parts = if filter === nothing
        _windowparts(_GroupRows[_GroupRows(index,
            [0:(Int(plan.metadata.row_groups[index].num_rows) - 1)]) for index in groups
            if plan.metadata.row_groups[index].num_rows > 0], offset, limit)
    else
        _filterparts(file, groups, filter, budget)
    end
    if !_isflattableplan(nested)
        # Nested columns decode whole row groups; the scan's residual does the rest.
        filter === nothing && (offset = isempty(parts) ? 0 : first(parts[1].rows[1]))
        return _readgroups(file, Int[part.group for part in parts], budget, nested,
            offset, limit)
    end
    limits = plan.limits
    metadata = _withgroups(plan.metadata, Int[part.group for part in parts])
    schema = plan.schema
    _validatetablerowgroups(metadata, schema, limits)
    columns, rows = _readselectedflat(file, plan, parts, filter !== nothing, offset, limit,
        budget)
    named = NamedTuple{Tuple(_tablenames(schema, limits))}(Tuple(columns))
    bound === nothing && return Table(metadata, schema, named, rows, _outputcolmetadata(plan))
    projection = Tables.BoundScan(bound.columns, nothing, bound.filtercols, nothing, 0,
        bound.validate)
    named, rows, metadata, schema = _scanresidual(named, rows, projection, metadata, schema,
        limits, 0, nothing)
    return Table(metadata, schema, named, rows, _outputcolmetadata(plan))
end

# File metadata restricted to the row groups `groups`.
function _withgroups(metadata::Metadata.FileMetaData, groups::AbstractVector{Int})
    groups == eachindex(metadata.row_groups) && return metadata
    selected = metadata.row_groups[groups]
    return _withfields(metadata; row_groups=selected,
        num_rows=sum(group -> group.num_rows, selected; init=Int64(0)))
end

# Every row of row groups `groups`, then the scan's filter and the row window `offset`,
# `limit` applied to the columns read.
function _readgroups(file::File, groups::AbstractVector{Int}, budget::_ByteBudget,
        nested::_NestedSchemaPlan, offset::Int, limit)
    plan = file.plan
    limits = plan.limits
    metadata = _withgroups(plan.metadata, groups)
    schema = plan.schema
    rows = _validatetablerowgroups(metadata, schema, limits)
    _checklimit(:container_elements, rows, limits.max_container_elements)
    file = _coalescedfile(file, metadata, schema)
    names = _tablenames(schema, limits)
    if _isflattableplan(nested)
        columns = _readflattable(file, metadata, schema, rows, limits, budget, plan.ntasks,
            plan.options)
    else
        columns = _readnestedcolumns(file, metadata, schema, nested, rows, limits, budget,
            plan.ntasks, plan.options)
    end
    length(columns) == length(names) || throw(AssertionError(
        "table column count does not match its names"))
    named = NamedTuple{Tuple(names)}(Tuple(columns))
    bound = plan.bound
    if bound !== nothing
        named, rows, metadata, schema = _scanresidual(named, rows, bound, metadata, schema,
            limits, offset, limit)
    end
    return Table(metadata, schema, named, rows, _outputcolmetadata(plan))
end

# `file` reading through a `_CoalescedSource` over the column chunks of `metadata`, when
# its source is not in memory.
function _coalescedfile(file::File, metadata::Metadata.FileMetaData, schema::Schema)
    file.source isa MemorySource && return file
    wanted = Tuple{Int64,Int64}[]
    for group in metadata.row_groups, leaf in schema.leaves
        md = _chunkmetadata(group.columns[Int(leaf.column_index)], leaf)
        push!(wanted, _chunkrange(md, file.footer.offset))
    end
    return File(_CoalescedSource(file.source, wanted), file.footer, file.metadata,
        file.plan, false, false)
end

"""
    Parquet.Table(source; select=nothing, drop=nothing, filter=nothing, limit=nothing,
                  scan=nothing, ntasks=Threads.nthreads(), stringtype=DataString,
                  int96=Dates.Nanosecond, checksum=false, partitioning=:hive,
                  limits=Parquet.Limits())

Read a Parquet file from a path, a byte vector, an `IO`, or a
[`Parquet.AbstractSource`](@ref) into Tables.jl columns. The table owns its columns: a
path is unmapped and closed before `Table` returns, and a source you pass stays open. A
directory, a glob pattern, a vector of paths or sources, or a
[`Parquet.Dataset`](@ref) reads every file of the dataset into one table, with the
dataset's partition columns (`partitioning`).

- `select` keeps the given columns, in that order (names, indices, a regex, or a vector
  of them, or a function `(i, name) -> Bool` as in CSV.jl); `drop` keeps all but the
  given columns.
- `filter` keeps the rows where a predicate built from [`Parquet.col`](@ref) is `true`,
  for example `filter=Parquet.col(:fare) > 10`. Row groups and pages that statistics,
  page indexes, bloom filters, or dictionary pages show cannot match are skipped, and
  the columns not filtered on are decoded only for the rows that match.
- `limit` keeps at most that many (filtered) rows; only the pages it reaches are decoded.
- `scan` takes a full `Tables.Scan` (renames, offset, `validate`) instead of the keywords
  above. Type overrides are not supported.

Only the columns a read selects or filters on are decoded; the column chunks of every
other column are skipped. The result keeps their stored Parquet schema (renamed), so
writing it preserves their annotations.

Column chunks decode on up to `ntasks` tasks; `ntasks=1` reads on the calling task.
The result does not depend on `ntasks`.

STRING and ENUM values read as `DataStrings.DataString`s, which refer to the column's
decoded bytes; `stringtype=String` reads them as `String`s, including inside nested
columns, at the cost of one allocation per value.

INT96 timestamps (written by Spark, Hive, and Impala) read as
`Durations.Timestamp{int96}`. Spark's `9999-12-31` placeholder values are outside the
nanosecond range (1677 to 2262); read such files with `int96=Dates.Microsecond`.

`checksum=true` verifies the CRC32 of every page that has one and throws a
[`Parquet.FormatError`](@ref) on a mismatch. Reads do not verify by default, as pyarrow,
Polars, and DuckDB do not. Parquet.jl 0.x wrote a CRC of 0 for every page, so zero
CRCs in its files are not checked.
"""
function Table(input; select=nothing, drop=nothing, filter=nothing, limit=nothing,
        scan::Union{Nothing,Tables.Scan}=nothing, ntasks::Integer=Threads.nthreads(),
        stringtype::Type=DataStrings.DataString, int96::Type=Dates.Nanosecond,
        checksum::Bool=false, partitioning=:hive, limits::Limits=Limits())
    _isdataset(input) && return _readdataset(Dataset(input; partitioning=partitioning,
        select=select, drop=drop, filter=filter, limit=limit, scan=scan, ntasks=ntasks,
        stringtype=stringtype, int96=int96, checksum=checksum, limits=limits))
    budget = _ByteBudget(limits)
    file = File(input; select=select, drop=drop, filter=filter, limit=limit, scan=scan,
        ntasks=ntasks, stringtype=stringtype, int96=int96, checksum=checksum,
        limits=limits, budget=budget)
    try
        return _readtable(file, eachindex(file.plan.metadata.row_groups), budget)
    finally
        close!(file)
    end
end

# Whether `Table` reads `input` as a dataset: a directory, a glob pattern, or a vector of
# paths or sources.
_isdataset(input::AbstractString) = !isfile(input) && (isdir(input) || _isglob(input))
_isdataset(input::AbstractVector) = !(input isa AbstractVector{UInt8})
_isdataset(input) = false

# Applies the filter, row bounds, order, and renames of a resolved scan to the columns
# read for it, and restricts the schema to the output columns.
function _scanresidual(columns::NamedTuple, rows::Int, bound::Tables.BoundScan,
        metadata::Metadata.FileMetaData, schema::Schema, limits::Limits,
        offset::Int=bound.offset, limit=bound.limit)
    # Compiled once, not for each set of column types: only the residual scan needs them.
    @nospecialize columns
    readpositions = sort!(unique!(vcat(Int[column.index for column in bound.columns],
        bound.filtercols)))
    positions = Int[searchsortedfirst(readpositions, column.index)
        for column in bound.columns]
    names = Symbol[column.name for column in bound.columns]
    readnames = keys(columns)
    if bound.filter === nothing && limit === nothing && iszero(offset)
        positions == eachindex(readnames) && Tuple(names) == readnames &&
            return columns, rows, metadata, schema
        # Reordered or renamed columns keep their vectors; a column selected twice is
        # copied so the table never holds one vector under two names.
        seen = falses(length(readnames))
        output = NamedTuple{Tuple(names)}(Tuple(begin
            column = columns[position]
            seen[position] ? copy(column) : (seen[position] = true; column)
        end for position in positions))
        metadata, schema = _selectfields(metadata, schema, positions,
            String[String(name) for name in names], limits)
        return output, rows, metadata, schema
    end
    residual = Tables.Scan(; select=[readnames[position] => name
        for (position, name) in zip(positions, names)], filter=bound.filter,
        limit=limit, offset=offset, validate=bound.validate)
    # With no column read, a stand-in column carries the row count to the executor.
    result = Tables.scan(isempty(readnames) ? (rows=Base.OneTo(rows),) : columns,
        residual)
    outrows = Tables.rowcount(result)
    output = NamedTuple{Tuple(names)}(Tuple(Tables.getcolumn(result, name)
        for name in names))
    metadata, schema = _selectfields(metadata, schema, positions,
        String[String(name) for name in names], limits)
    return output, outrows, metadata, schema
end

# A table holds no file; `close` has nothing to release.
function Base.close(::Table)
    return
end

"""
    copy(table::Parquet.Table) -> Parquet.Table

A table with the same schema and metadata whose columns are new, mutable vectors
(`copy` of each column). List, struct, and map columns become `Vector`s of their rows;
`copy(row)` makes a row's own vector.
"""
Base.copy(table::Table) = Table(_tablemetadata(table), _tableschema(table),
    map(copy, _tablecolumns(table)), _tablerows(table), _tablecolmetadata(table))

# The row groups a file yields and each one's (offset, limit) row window, or `nothing`
# when a filter combined with a limit or offset leaves them unknown before reading.
function _filegroups(file::File)
    plan = file.plan
    groups = plan.metadata.row_groups
    bound = plan.bound
    (bound === nothing || (iszero(bound.offset) && bound.limit === nothing)) &&
        return Tuple{Int,Int,Union{Nothing,Int}}[(index, 0, nothing)
            for index in _statisticsgroups(file, eachindex(groups))]
    bound.filter === nothing || return nothing
    first = bound.offset
    last = bound.limit === nothing ? typemax(Int) : Base.checked_add(first, bound.limit)
    windows = Tuple{Int,Int,Union{Nothing,Int}}[]
    start = 0
    for (index, group) in enumerate(groups)
        stop = start + Int(group.num_rows)
        low, high = max(start, first), min(stop, last)
        low < high && push!(windows, (index, low - start, high - low))
        start = stop
    end
    return windows
end

function _filegroupsknown(file::File)
    groups = _filegroups(file)
    groups === nothing && throw(ArgumentError("the row groups of a Parquet.File with " *
        "both a filter and a limit or offset are known only while reading; iterate it"))
    return groups
end

function _readfilegroup(file::File, (index, offset, limit))
    return _readtable(file, index:index, _ByteBudget(file.plan.limits);
        window=(offset, limit))
end

Base.length(file::File) = length(_filegroupsknown(file))
Base.IteratorSize(::Type{<:File}) = Base.SizeUnknown()
Base.eltype(::Type{<:File}) = Table

function Base.getindex(file::File, index::Integer)
    groups = _filegroupsknown(file)
    checkbounds(groups, index)
    return _readfilegroup(file, groups[index])
end

function Base.iterate(file::File)
    groups = _filegroups(file)
    groups === nothing || return iterate(file, (groups, 1))
    bound = file.plan.bound
    return iterate(file, (1, bound.offset, bound.limit))
end

function Base.iterate(file::File, (groups, position)::Tuple{Vector,Int})
    position > length(groups) && return nothing
    return _readfilegroup(file, groups[position]), (groups, position + 1)
end

# With a filter and a row window: filter each row group, then skip and keep rows across
# the groups until the window is filled.
function Base.iterate(file::File, (index, skip, keep)::Tuple{Int,Int,Any})
    groups = file.plan.metadata.row_groups
    keep == 0 && return nothing
    while index <= length(groups) && isempty(_statisticsgroups(file, index:index))
        index += 1
    end
    index > length(groups) && return nothing
    table = _readtable(file, index:index, _ByteBudget(file.plan.limits);
        window=(0, nothing))
    rows = _tablerows(table)
    drop = min(skip, rows)
    take = keep === nothing ? rows - drop : min(keep, rows - drop)
    return _slicetable(table, drop, take),
        (index + 1, skip - drop, keep === nothing ? nothing : keep - take)
end

# The `take` rows of `table` after its first `drop`.
function _slicetable(table::Table, drop::Int, take::Int)
    rows = _tablerows(table)
    (drop == 0 && take == rows) && return table
    range = (drop + 1):(drop + take)
    named = map(column -> _slicecolumn(column, range), _tablecolumns(table))
    return Table(_tablemetadata(table), _tableschema(table), named, take,
        _tablecolmetadata(table))
end

# Rows `range` of a column; string and byte columns keep their kind and share buffers.
_slicecolumn(column::AbstractVector, range::UnitRange{Int}) = column[range]

function _slicecolumn(column::Union{DataStrings.StringVector,DataStrings.BytesVector},
        range::UnitRange{Int})
    return typeof(column)(column.payloads[range], column.buffers, Val(:trusted))
end

Tables.partitions(file::File) = file
Tables.istable(::Type{<:File}) = true
Tables.columnaccess(::Type{<:File}) = true

function Tables.columns(file::File)
    table = _readtable(file, eachindex(file.plan.metadata.row_groups),
        _ByteBudget(file.plan.limits))
    return Tables.columns(table)
end

# The columns a read returns, from a read of no row groups.
Tables.schema(file::File) = Tables.schema(_readtable(file, 1:0, _ByteBudget(file.plan.limits)))
Tables.columnnames(file::File) = Tables.schema(file).names

function DataAPI.nrow(file::File)
    bound = file.plan.bound
    bound === nothing || bound.filter === nothing || throw(ArgumentError(
        "the row count of a filtered Parquet.File is known only after reading it"))
    return sum(window -> something(window[3], Int(file.plan.metadata.row_groups[window[1]].num_rows)),
        _filegroupsknown(file); init=0)
end

"""
    Parquet.metadata(x) -> Parquet.Metadata.FileMetaData

The footer metadata of a [`Parquet.File`](@ref) or of the file a [`Parquet.Table`](@ref)
was read from (restricted to the columns read): schema, row groups, column chunk
encodings and statistics, and key-value metadata. It displays as a summary; its fields
are the Thrift structures of the format. [`Parquet.statistics`](@ref) decodes a
column's statistics.
"""
metadata(file::File) = file.metadata
metadata(table::Table) = _tablemetadata(table)

function Base.show(io::IO, ::MIME"text/plain", metadata::Metadata.FileMetaData)
    groups = something(metadata.row_groups, Metadata.RowGroup[])
    schema = metadata.schema
    root = isempty(schema) ? nothing : first(schema)
    names = String[]
    if root !== nothing
        element = 2
        for _ in 1:something(root.num_children, Int32(0))
            element > length(schema) && break
            push!(names, schema[element].name)
            element = _subtreeend(schema, element) + 1
        end
    end
    print(io, "Parquet file metadata: ", metadata.num_rows, " rows in ", length(groups),
        " row groups, ", length(names), " columns")
    isempty(names) || print(io, "\n  columns: ", join(first(names, 10), ", "),
        length(names) > 10 ? ", … ($(length(names) - 10) more)" : "")
    metadata.created_by === nothing || print(io, "\n  created by: ", metadata.created_by)
    keys = unique(String[entry.key
        for entry in something(metadata.key_value_metadata, Metadata.KeyValue[])])
    isempty(keys) || print(io, "\n  key-value metadata: ", join(keys, ", "))
    return
end

"""
    Parquet.statistics(file::Parquet.File, column) -> Vector

The statistics of a flat column of `file` (its name or its index among the columns the
file reads) in each row group, as `(rows, nulls, min, max)` named tuples. `min` and `max`
have the column's element type; a statistic the file leaves out, or one that does not
hold (see [Statistics](@ref)), is `nothing`.

```julia
stats = Parquet.statistics(Parquet.File("trips.parquet"), :fare)
stats[1].max  # the largest fare in the first row group
```
"""
function statistics(file::File, column::Union{Symbol,AbstractString,Integer})
    plan = file.plan
    names = _tablenames(plan.schema, plan.limits)
    index = column isa Integer ? Int(column) : findfirst(==(Symbol(column)), names)
    (index === nothing || !(1 <= index <= length(names))) && throw(ArgumentError(
        "$(repr(column)) is not a column of this file; its columns are " *
        join(names, ", ")))
    node = plan.schema.root.children[index]
    field = _prunefield(node, plan)
    field === nothing && throw(ArgumentError("column $(names[index]) is nested; " *
        "Parquet.metadata(file) has the statistics of its leaves"))
    return map(plan.metadata.row_groups) do group
        md = _chunkcolumnmetadata(group, node)
        facts = md === nothing ? nothing : try
            _statisticsfacts(plan.schema, Int(node.column_index),
                plan.metadata.created_by, plan.metadata.column_orders, md;
                limits=plan.limits)
        catch err
            err isa Union{FormatError,LimitError,ArgumentError} || rethrow()
            nothing
        end
        facts === nothing &&
            return (rows=Int(group.num_rows), nulls=nothing, min=nothing, max=nothing)
        bound(fact) = fact.state === :known ? _boundvalue(field, fact.raw) : nothing
        nulls = facts.null_count.state === :known ? Int(facts.null_count.value) : nothing
        return (rows=Int(group.num_rows), nulls=nulls, min=bound(facts.lower),
            max=bound(facts.upper))
    end
end

# Parquet's key-value metadata as DataAPI metadata (style `:default`).
DataAPI.metadatasupport(::Type{<:Union{File,Table}}) = (read=true, write=false)

# A file's ARROW:schema becomes column types (time zones, durations), so DataAPI
# metadata leaves it out, as pyarrow does; `Parquet.metadata` still has it.
function _keyvaluemetadata(x::Union{File,Table})
    entries = something(metadata(x).key_value_metadata, Metadata.KeyValue[])
    return filter(entry -> entry.key != "ARROW:schema", entries)
end

function DataAPI.metadatakeys(x::Union{File,Table})
    return unique!(String[entry.key for entry in _keyvaluemetadata(x)])
end

function DataAPI.metadata(x::Union{File,Table}, key::AbstractString; style::Bool=false)
    for entry in Iterators.reverse(_keyvaluemetadata(x))
        entry.key == key && return style ? (entry.value, :default) : entry.value
    end
    throw(KeyError(key))
end

function DataAPI.metadata(x::Union{File,Table}, key::AbstractString, default;
        style::Bool=false)
    for entry in Iterators.reverse(_keyvaluemetadata(x))
        entry.key == key && return style ? (entry.value, :default) : entry.value
    end
    return style ? (default, :default) : default
end

# Column metadata through DataAPI (style `:default`): each column's names and entries.
DataAPI.colmetadatasupport(::Type{<:Union{File,Table}}) = (read=true, write=false)

_colmetadataentries(table::Table) = collect(keys(_tablecolumns(table))),
    _tablecolmetadata(table)

function _colmetadataentries(file::File)
    plan = file.plan
    names = plan.bound === nothing ? _tablenames(file.plan.schema, plan.limits) :
        Symbol[column.name for column in plan.bound.columns]
    return names, _outputcolmetadata(plan)
end

"""
    Parquet.read(source, sink; kw...)

Read `source` as [`Parquet.Table`](@ref)`(source; kw...)` does and pass its columns to
`sink` without copying their values, for example `Parquet.read(path, DataFrame)`.
Nested columns arrive as `Vector`s of their rows, so the sink can grow and reorder them.
A sink that takes DataAPI metadata, such as a `DataFrame`, gets the file and column
metadata, as `DataFrame(Parquet.Table(path))` does.
"""
function read(source, sink; kw...)
    table = Table(source; kw...)
    columns = map(_rowcolumn, _tablecolumns(table))
    copied = Table(_tablemetadata(table), _tableschema(table), columns, _tablerows(table),
        _tablecolmetadata(table))
    result = sink(Tables.CopiedColumns(copied))
    # The CopiedColumns wrapper, which lets the sink keep the columns, hides the
    # table's metadata from it.
    if DataAPI.metadatasupport(typeof(result)).write
        for key in DataAPI.metadatakeys(copied)
            DataAPI.metadata!(result, key, DataAPI.metadata(copied, key); style=:default)
        end
    end
    if DataAPI.colmetadatasupport(typeof(result)).write
        for (column, keys) in DataAPI.colmetadatakeys(copied), key in keys
            DataAPI.colmetadata!(result, column, key,
                DataAPI.colmetadata(copied, column, key); style=:default)
        end
    end
    return result
end

_rowcolumn(column::AbstractVector) = column
_rowcolumn(column::Union{ListVector,StructVector,MapVector}) = collect(column)

function read(source; kw...)
    throw(ArgumentError("Parquet.read needs a sink, for example " *
        "`Parquet.read(source, DataFrame)`; `Parquet.Table(source)` returns the table itself"))
end

"""
    Parquet.col(name)

A column reference for a read `filter`, the same as `Tables.col`:
`Parquet.Table(path; filter=Parquet.col(:x) > 3)`. `<`, `<=`, `>`, and `>=` compare it with
numbers, strings, and characters, and `&`, `|`, and `!` combine filters; equality and
other values go through [`Parquet.colcmp`](@ref). See also [`Parquet.colin`](@ref) and
[`Parquet.isnull`](@ref).
"""
const col = Tables.col

"""
    Parquet.colcmp(op, column, value)

A filter comparison, the same as `Tables.colcmp`; `op` is `==`, `!=`, `<`, `<=`, `>`, or
`>=`. Use it for equality (`Parquet.col(:x) == 3` is the ordinary `==`, which is `false`)
and for values the operator forms of [`Parquet.col`](@ref) do not cover, such as dates
and timestamps: `Parquet.colcmp(>=, Parquet.col(:day), Date(2025, 1, 1))`.

A read checks `value` against the column's type: a value of another kind, such as a
number for a string column, is an `ArgumentError`, and a `Date`, `DateTime`, or
`Durations.Timestamp` compared with a UTC timestamp column is that time in UTC.
"""
const colcmp = Tables.colcmp

"""
    Parquet.colin(column, values)

A filter that keeps rows whose value is in `values`, the same as `Tables.colin`.
"""
const colin = Tables.colin

"""
    Parquet.isnull(column)

A filter that keeps rows whose value is missing, the same as `Tables.isnull`.
"""
const isnull = Tables.isnull

function Tables.columns(table::Table)
    return _tablecolumns(table)
end

function Tables.columnnames(table::Table)
    return keys(_tablecolumns(table))
end

function Tables.getcolumn(table::Table, name::Symbol)
    return getfield(_tablecolumns(table), name)
end

function Tables.getcolumn(table::Table, index::Int)
    return getfield(_tablecolumns(table), index)
end

function Tables.getcolumn(table::Table, ::Type{T}, index::Int, name::Symbol) where {T}
    return getfield(_tablecolumns(table), index)
end

function Tables.schema(table::Table)
    columns = _tablecolumns(table)
    return Tables.Schema(Tuple(keys(columns)), Tuple(eltype(column) for column in columns))
end

function Tables.rowcount(table::Table)
    return _tablerows(table)
end

function DataAPI.nrow(table::Table)
    return _tablerows(table)
end

function DataAPI.ncol(table::Table)
    return length(_tablecolumns(table))
end

function Base.show(io::IO, table::Table)
    print(io, "Parquet.Table(", _tablerows(table), " rows, ", DataAPI.ncol(table),
        " columns)")
    return
end

function Base.show(io::IO, ::MIME"text/plain", table::Table)
    print(io, "Parquet.Table with ", _tablerows(table), " rows, ", DataAPI.ncol(table),
        " columns, and schema:\n")
    show(IOContext(io, :print_schema_header => false), Tables.schema(table))
    return
end
