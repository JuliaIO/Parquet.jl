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

function _writefields(table::Table, limits::Limits,
        budget::_LiveByteBudget)
    return _provenancewritefields(table, limits, budget, nothing, false)
end

function _writefieldsencoded(table::Table, limits::Limits,
        budget::_LiveByteBudget, encoding, dictionary::Bool)
    return _provenancewritefields(table, limits, budget, encoding, dictionary)
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

function _isstringcolumn(node::SchemaNode)
    return _logicalkind(node) === :string
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
    return
end

function _tablecolumns(metadata::Metadata.FileMetaData, schema::Schema,
    rows::Int, budget::_LiveByteBudget)
    nodes = schema.root.children
    _reservearray!(budget, Any, length(nodes))
    columns = Any[]
    sizehint!(columns, length(nodes))
    for node in nodes
        _reserveprimitivecolumn!(budget, metadata, schema, node, rows)
        column = _tablecolumn(node)
        sizehint!(column, rows)
        push!(columns, column)
    end
    return columns
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
        for (fieldindex, node) in enumerate(schema.root.children)
            before = _budgetused(budget)
            values = _readtablefield(file, metadata, schema, rowindex, node,
                group.num_rows, limits, budget)
            baseline = _tablefieldpayloadbaseline(metadata, schema, rowindex,
                node)
            retained = max(Int64(0), _tablefieldpayload(values) - baseline)
            append!(columns[fieldindex], values)
            transient = _budgetused(budget) - before
            retained <= transient || throw(AssertionError(
                "final variable-width values exceed their materialization charge"))
            transient == retained || _release!(budget, transient - retained)
        end
    end
    total == rows || throw(FormatError(
        "row groups contain $total rows but $rows were validated"))
    return
end

function _isflattableplan(plan::_NestedSchemaPlan)
    for child in plan.root.children
        child isa _NestedLeafPlan || return false
        child.source.max_repetition_level == 0 || return false
    end
    return true
end

function Table(input; limits::Limits=Limits())
    budget = _LiveByteBudget(limits)
    _reserveobjects!(budget, 2)
    file = File(input; limits=limits, budget=budget)
    try
        metadata = _readfilemetadata(file, limits, budget)
        metadata.num_rows >= 0 || throw(FormatError("file metadata has a negative row count"))
        metadata.num_rows <= typemax(Int) || throw(FormatError("file row count overflows Int"))
        _checklimit(:container_elements, metadata.num_rows, limits.max_container_elements)
        schema = Schema(metadata; limits=limits, budget=budget)
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
            columns = _tablecolumns(metadata, schema, rows, budget)
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
