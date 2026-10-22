import Tables

mutable struct Table{F<:File,C<:NamedTuple}
    file::F
    metadata::Metadata.FileMetaData
    schema::Schema
    columns::C
    rows::Int
    @atomic closed::Bool
end

function _tablewritecolumn(name, values::AbstractVector, node::SchemaNode,
    limits::Limits)
    node.element.type_ === nothing && return _writecolumn(name, values, limits)
    length(node.path) == 1 || throw(ArgumentError(
        "cannot write a nested primitive as a top-level table column"))
    String(name) == node.element.name || throw(ArgumentError(
        "table column $(repr(name)) does not match stored schema field " *
        repr(node.element.name)))
    return _writescalarlogicalcolumn(name, values, node.element, limits)
end

function _writecolumns(table::Table, limits::Limits,
    budget::_LiveByteBudget)
    columns = Tables.columns(table)
    raw_names = Tables.columnnames(columns)
    _reservearray!(budget, Any, length(raw_names))
    names = collect(raw_names)
    nodes = table.schema.root.children
    length(names) == length(nodes) || throw(ArgumentError(
        "table columns no longer match the stored Parquet schema"))
    _reservearray!(budget, WriteColumn, length(names))
    output = WriteColumn[]
    sizehint!(output, length(names))
    rows = nothing
    for (name, node) in zip(names, nodes)
        values = Tables.getcolumn(columns, name)
        values isa AbstractVector || throw(ArgumentError(
            "Parquet columns must be vectors"))
        if rows === nothing
            rows = length(values)
            _checklimit(:container_elements, rows, limits.max_container_elements)
        end
        length(values) == rows || throw(ArgumentError(
            "Parquet columns have different lengths"))
        _reservewritenormalization!(budget, values, limits)
        push!(output, _tablewritecolumn(name, values, node, limits))
    end
    return output, something(rows, 0)
end

function _writecolumns(table::Table, limits::Limits)
    return _writecolumns(table, limits, _LiveByteBudget(limits))
end

function _writefields(table::Table, limits::Limits, ::_LiveByteBudget)
    return _provenancewritefields(table, limits, nothing, false)
end

function _writefieldsencoded(table::Table, limits::Limits, ::_LiveByteBudget,
        encoding, dictionary::Bool)
    return _provenancewritefields(table, limits, encoding, dictionary)
end

function _writekeyvaluemetadata(table::Table, limits::Limits,
        budget::_LiveByteBudget)
    metadata = table.metadata.key_value_metadata
    metadata === nothing && return nothing
    _checklimit(:container_elements, length(metadata),
        limits.max_container_elements)
    for item in metadata
        _checklimit(:string_bytes, ncodeunits(item.key),
            limits.max_string_bytes)
        item.value === nothing || _checklimit(:string_bytes,
            ncodeunits(item.value), limits.max_string_bytes)
    end
    _reservearray!(budget, Metadata.KeyValue, length(metadata))
    return copy(metadata)
end

function _readfilemetadata(file::File, limits::Limits,
    budget::_LiveByteBudget)
    file.footer.encrypted && throw(FormatError("encrypted footers are not supported yet"))
    readercharge = _reserveobjects!(budget)
    reader = Thrift.Reader(file.footer.bytes; limits=limits, budget=budget)
    metadata = try
        value = Thrift.decode(reader, Metadata.FileMetaData)
        value.encryption_algorithm === nothing || throw(FormatError(
            "plaintext-footer encryption is not supported yet"))
        Thrift.remaining(reader) == 0 || throw(FormatError(
            "file footer has trailing bytes"))
        value
    catch
        charge = _materializedsum(readercharge,
            Thrift.materializedcharge(reader))
        _release!(budget, charge)
        rethrow()
    end
    _release!(budget, readercharge)
    return metadata
end

function _readfilemetadata(file::File, limits::Limits)
    return _readfilemetadata(file, limits, _LiveByteBudget(limits))
end

function _tablecolumntype(node::SchemaNode)
    physical = _physicaleltype(node.element.type_)
    return _logicaleltype(node, physical)
end

function _tablecolumn(node::SchemaNode)
    length(node.path) == 1 || throw(FormatError("nested columns are not supported yet"))
    T = _tablecolumntype(node)
    node.max_repetition_level == 0 || throw(FormatError("repeated columns are not supported yet"))
    node.max_definition_level in (0, 1) ||
        throw(FormatError("nested definition levels are not supported yet"))
    optional = node.max_definition_level == 1
    value_type = optional ? Union{Missing,T} : T
    if node.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
            _logicalkind(node) === nothing
        return FixedByteArrayVector(value_type, node.element.type_length)
    end
    return value_type[]
end

function _tablelogicalpayloadbytes(values::AbstractVector)
    objects = Int64(0)
    payload = Int64(0)
    for value in values
        ismissing(value) && continue
        value isa AbstractVector{UInt8} || continue
        objects = _materializedsum(objects, _MATERIALIZED_OBJECT_BYTES)
        payload = _materializedsum(payload, length(value))
    end
    return _materializedsum(objects, payload)
end

function _tablevalues(node::SchemaNode, values::Vector, limits::Limits,
    budget::_LiveByteBudget)
    kind = _logicalkind(node)
    kind === nothing && return values
    physical = _physicaleltype(node.element.type_)
    logical = _logicaleltype(node, physical)
    outputtype = Missing <: eltype(values) ? Union{Missing,logical} : logical
    _reservearray!(budget, outputtype, length(values))
    _reserve!(budget, _tablelogicalpayloadbytes(values))
    kind === :decimal && _reserveobjects!(budget,
        count(!ismissing, values))
    return _logicalvalues(node, values; limits=limits)
end

function _tablevalues(node::SchemaNode, values::Vector, limits::Limits)
    return _tablevalues(node, values, limits, _LiveByteBudget(limits))
end

function _tablevalues(node::SchemaNode, values::Vector)
    return _tablevalues(node, values, Limits())
end

function _readtablefield(file::File, metadata::Metadata.FileMetaData, schema::Schema,
    rowindex::Int, node::SchemaNode, rows::Int64, limits::Limits,
    budget::_LiveByteBudget)
    values = readcolumn(file, metadata, schema, rowindex, node.column_index;
        limits=limits, budget=budget)
    length(values) == rows || throw(FormatError(
        "flat column $(node.column_index) has $(length(values)) values for $rows rows"))
    return _tablevalues(node, values, limits, budget)
end

function _tablenames(schema::Schema, limits::Limits,
    budget::_LiveByteBudget)
    charge = _reservearray!(budget, String, length(schema.root.children))
    try
        names = String[]
        sizehint!(names, length(schema.root.children))
        for node in schema.root.children
            push!(names, node.element.name)
        end
        return _internschemanames(names, limits, budget)
    finally
        _release!(budget, charge)
    end
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

function _bloomfilterrange(md::Metadata.ColumnMetaData,
        footeroffset::Int64)
    offset = md.bloom_filter_offset
    length = md.bloom_filter_length
    length !== nothing && offset === nothing && throw(FormatError(
        "bloom-filter length is present without its offset"))
    offset === nothing && return nothing
    offset >= 4 || throw(FormatError(
        "bloom-filter offset $offset is inside the file header"))
    if length === nothing
        offset < footeroffset || throw(FormatError(
            "bloom-filter offset extends past the footer"))
        return (Int64(offset), Int64(1))
    end
    length > 0 || throw(FormatError(
        "bloom-filter length must be positive, got $length"))
    stop = _pageindexrangeend(offset, Int64(length),
        "bloom-filter range overflows Int64")
    stop <= footeroffset || throw(FormatError(
        "bloom-filter range extends past the footer"))
    return (Int64(offset), Int64(length))
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
            md = _chunkmetadata(chunk, leaf)
            _chunkrange(md, footeroffset)
            _bloomfilterrange(md, footeroffset)
        end
    end
    return
end

function _tablechunkmetrics(metadata::Metadata.FileMetaData, schema::Schema,
    leaf::SchemaNode)
    entries = Int64(0)
    payload = Int64(0)
    index = Int(leaf.column_index)
    for (rowindex, group) in enumerate(metadata.row_groups)
        index <= length(group.columns) || throw(FormatError(
            "row group $rowindex has no column chunk for leaf $index"))
        md = _chunkmetadata(group.columns[index], leaf)
        entries = _materializedsum(entries, md.num_values)
        payload = _materializedsum(payload, md.total_uncompressed_size)
    end
    return entries, payload
end

function _tablevaluepayload(value)
    ismissing(value) && return Int64(0)
    value isa AbstractString && return Int64(ncodeunits(value))
    value isa AbstractVector{UInt8} && return Int64(length(value))
    value isa JSONValue && return Int64(length(value.bytes))
    value isa BSONValue && return Int64(length(value.bytes))
    value isa AbstractVector || return Int64(0)
    bytes = Int64(0)
    for child in value
        bytes = _materializedsum(bytes, _tablevaluepayload(child))
    end
    return bytes
end

function _tablefieldpayload(values::AbstractVector)
    bytes = Int64(0)
    for value in values
        bytes = _materializedsum(bytes, _tablevaluepayload(value))
    end
    return bytes
end

function _tablefieldpayloadbaseline(metadata::Metadata.FileMetaData,
    schema::Schema, rowindex::Int, node::SchemaNode)
    node.element.type_ in (Metadata.Type.BYTE_ARRAY,
        Metadata.Type.FIXED_LEN_BYTE_ARRAY) || return Int64(0)
    chunk = metadata.row_groups[rowindex].columns[Int(node.column_index)]
    md = _chunkmetadata(chunk, node)
    return Int64(md.total_uncompressed_size)
end

function _reserveprimitivecolumn!(budget::_LiveByteBudget,
    metadata::Metadata.FileMetaData, schema::Schema, node::SchemaNode,
    rows::Int)
    T = _tablecolumntype(node)
    value_type = node.max_definition_level == 1 ? Union{Missing,T} : T
    _reservearray!(budget, value_type, rows)
    if node.element.type_ == Metadata.Type.FIXED_LEN_BYTE_ARRAY &&
            _logicalkind(node) === nothing
        _reserveobjects!(budget)
    end
    if node.element.type_ in (Metadata.Type.BYTE_ARRAY,
            Metadata.Type.FIXED_LEN_BYTE_ARRAY)
        entries, payload = _tablechunkmetrics(metadata, schema, node)
        _reserve!(budget, _materializedproduct(entries,
            _MATERIALIZED_OBJECT_BYTES))
        _reserve!(budget, payload)
    end
    _logicalkind(node) === :decimal && _reserveobjects!(budget, rows)
    return
end

function _tablecolumns(metadata::Metadata.FileMetaData, schema::Schema,
    rows::Int, budget::_LiveByteBudget, bytes::Int64)
    nodes = schema.root.children
    _reservearray!(budget, Any, length(nodes))
    columns = Any[]
    sizehint!(columns, length(nodes))
    for node in nodes
        _reserveprimitivecolumn!(budget, metadata, schema, node, rows)
        column = _tablecolumn(node)
        sizehint!(column, _claimcapacity(rows, bytes))
        push!(columns, column)
    end
    return columns
end

# Decoding runs one task per column when the source allows concurrent reads, Julia has
# more than one thread, and no `max_materialized_bytes` cap needs exact shared
# accounting; each task then charges a private, unlimited budget.
function _parallelread(file::File, limits::Limits, count::Int)
    return count > 1 && Threads.nthreads() > 1 && concurrentreads(file.source) &&
        limits.max_materialized_bytes == typemax(Int64)
end

struct _ReadFailure
    error::Any
end

# `map(f, indices)`, on one task per index when `parallel`. A task's error is rethrown
# as itself rather than wrapped in a TaskFailedException.
function _readmap(f, indices, parallel::Bool)
    parallel || return map(f, indices)
    tasks = map(indices) do index
        errormonitor(Threads.@spawn try
            f(index)
        catch err
            _ReadFailure(err)
        end)
    end
    results = map(fetch, tasks)
    for result in results
        result isa _ReadFailure && throw(result.error)
    end
    return results
end

function _readrowgroups!(columns::Vector, file::File, metadata::Metadata.FileMetaData,
    schema::Schema, rows::Int, limits::Limits, budget::_LiveByteBudget)
    total = Int64(0)
    for (rowindex, group) in enumerate(metadata.row_groups)
        group.num_rows >= 0 || throw(FormatError("row group $rowindex has a negative row count"))
        group.num_rows <= typemax(Int) || throw(FormatError("row group $rowindex row count overflows"))
        length(group.columns) == length(schema.leaves) ||
            throw(FormatError("row group $rowindex has $(length(group.columns)) columns for $(length(schema.leaves)) schema leaves"))
        total = try
            Base.checked_add(total, group.num_rows)
        catch err
            err isa OverflowError || rethrow()
            throw(FormatError("row group row count overflows Int64"))
        end
        total <= rows || throw(FormatError(
            "row groups contain more rows than the validated table count"))
        _checklimit(:container_elements, total, limits.max_container_elements)
    end
    total == rows || throw(FormatError(
        "row groups contain $total rows but $rows were validated"))
    fields = schema.root.children
    chunks = vec([(fieldindex, rowindex) for rowindex in eachindex(metadata.row_groups),
        fieldindex in eachindex(fields)])
    parallel = _parallelread(file, limits, length(chunks))
    pieces = _readmap(chunks, parallel) do (fieldindex, rowindex)
        return _readtablechunk(file, metadata, schema, rowindex, fields[fieldindex],
            limits, parallel ? _LiveByteBudget(limits) : budget)
    end
    for ((fieldindex, _), values) in zip(chunks, pieces)
        append!(columns[fieldindex], values)
    end
    return
end

# One field's values from one row group; the charge for them stays on `budget`.
function _readtablechunk(file::File, metadata::Metadata.FileMetaData,
        schema::Schema, rowindex::Int, node::SchemaNode, limits::Limits,
        budget::_LiveByteBudget)
    before = _budgetused(budget)
    group = metadata.row_groups[rowindex]
    values = _readtablefield(file, metadata, schema, rowindex, node,
        group.num_rows, limits, budget)
    baseline = _tablefieldpayloadbaseline(metadata, schema, rowindex, node)
    retained = max(Int64(0), _tablefieldpayload(values) - baseline)
    transient = _budgetused(budget) - before
    retained <= transient || throw(AssertionError(
        "final variable-width values exceed their materialization charge"))
    transient == retained || _release!(budget, transient - retained)
    return values
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
    for (position, name) in zip(positions, names)
        range = elementranges[position]
        push!(selected, _withfields(elements[first(range)]; name=name))
        append!(selected, @view elements[(first(range) + 1):last(range)])
        append!(leaves, leafranges[position])
    end
    groups = map(enumerate(metadata.row_groups)) do (rowindex, group)
        length(group.columns) == leafcount || throw(FormatError(
            "row group $rowindex has $(length(group.columns)) columns for " *
            "$leafcount schema leaves"))
        return _withfields(group; columns=group.columns[leaves],
            sorting_columns=nothing)
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

function _isflattableplan(plan::_NestedSchemaPlan)
    for child in plan.root.children
        child isa _NestedLeafPlan || return false
        child.source.max_repetition_level == 0 || return false
    end
    return true
end

"""
    Parquet.Table(source; scan=nothing, limits=Parquet.Limits())

Read a Parquet file from a path, a byte vector, or an `IO` into Tables.jl columns.

`scan` takes a `Tables.Scan` request. Only the top-level columns it selects or its
filter references are read; the column chunks of every other column are skipped
unread. The filter, row bounds, and renames are then applied to the columns read, and
the result keeps their stored Parquet schema (renamed), so writing it preserves their
annotations. Type overrides are not supported.
"""
function Table(input; scan::Union{Nothing,Tables.Scan}=nothing,
        limits::Limits=Limits())
    budget = _LiveByteBudget(limits)
    _reserveobjects!(budget, 2)
    file = File(input; limits=limits, budget=budget)
    try
        metadata = _readfilemetadata(file, limits, budget)
        metadata.num_rows >= 0 || throw(FormatError("file metadata has a negative row count"))
        metadata.num_rows <= typemax(Int) || throw(FormatError("file row count overflows Int"))
        _checklimit(:container_elements, metadata.num_rows, limits.max_container_elements)
        schema = Schema(metadata; limits=limits, budget=budget)
        bound = nothing
        if scan !== nothing
            sourcenames = _tablenames(schema, limits, budget)
            bound = Tables.resolve(scan, sourcenames)
            any(column -> column.type !== nothing, bound.columns) && throw(ArgumentError(
                "Parquet.Table does not apply scan type overrides; convert the " *
                "columns after reading"))
            positions = sort!(unique!(vcat(Int[column.index
                for column in bound.columns], bound.filtercols)))
            metadata, schema = _selectfields(metadata, schema, positions,
                String[String(sourcenames[position]) for position in positions],
                limits)
        end
        rows = _validatetablerowgroups(metadata, schema, limits)
        _checklimit(:container_elements, rows, limits.max_container_elements)
        _validatetablechunkranges(metadata, schema, file.footer.offset)
        indexpreflight = _preflightoffsetindexdeclarations(file, metadata,
            schema, limits)
        indexpreflight = _validatepageindexdeclarationoverlaps!(file,
            metadata, schema, indexpreflight, budget)
        nested = _nestedplan(schema; limits=limits, budget=budget)
        nested.plan_count > 0 || throw(AssertionError(
            "nested schema plan has no root"))
        _validateoffsetindexes!(file, metadata, schema, limits, budget,
            indexpreflight)
        names = _tablenames(schema, limits, budget)
        if _isflattableplan(nested)
            columns = _tablecolumns(metadata, schema, rows, budget,
                _checkedsourcelength(file.source))
            _readrowgroups!(columns, file, metadata, schema, rows, limits,
                budget)
        else
            root = _readnestedroot(file, metadata, schema, nested, limits,
                budget)
            columns = root.children
        end
        length(columns) == length(names) || throw(AssertionError(
            "table column count does not match its names"))
        _reserveobjects!(budget, 2)
        named = NamedTuple{Tuple(names)}(Tuple(columns))
        if bound !== nothing
            named, rows, metadata, schema = _scanresidual(named, rows, bound,
                metadata, schema, limits)
        end
        table = Table(file, metadata, schema, named, rows, false)
        finalizer(close!, table)
        return table
    catch
        try
            close!(file)
        catch
        end
        rethrow()
    end
end

# Applies the filter, row bounds, order, and renames of a resolved scan to the columns
# read for it, and restricts the schema to the output columns.
function _scanresidual(columns::NamedTuple, rows::Int, bound::Tables.BoundScan,
        metadata::Metadata.FileMetaData, schema::Schema, limits::Limits)
    readpositions = sort!(unique!(vcat(Int[column.index for column in bound.columns],
        bound.filtercols)))
    positions = Int[searchsortedfirst(readpositions, column.index)
        for column in bound.columns]
    names = Symbol[column.name for column in bound.columns]
    readnames = keys(columns)
    bound.filter === nothing && bound.limit === nothing && iszero(bound.offset) &&
        positions == eachindex(readnames) && Tuple(names) == readnames &&
        return columns, rows, metadata, schema
    residual = Tables.Scan(; select=[readnames[position] => name
        for (position, name) in zip(positions, names)], filter=bound.filter,
        limit=bound.limit, offset=bound.offset, validate=bound.validate)
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

function close!(table::Table)
    (@atomicswap table.closed = true) && return
    close!(table.file)
    return
end

function Base.close(table::Table)
    close!(table)
    return
end

function Base.length(table::Table)
    return table.rows
end

function Tables.istable(::Type{<:Table})
    return true
end

function Tables.columnaccess(::Type{<:Table})
    return true
end

function Tables.columns(table::Table)
    return table.columns
end

function Tables.columnnames(table::Table)
    return keys(table.columns)
end

function Tables.schema(table::Table)
    names = Tuple(keys(table.columns))
    types = Tuple(eltype(column) for column in values(table.columns))
    return Tables.Schema(names, types)
end

function Tables.rowcount(table::Table)
    return length(table)
end
