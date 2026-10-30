# Filter pushdown for reads: which row groups and rows a filter can match, from column
# statistics, page indexes (ColumnIndex + OffsetIndex), and dictionary pages; and the read
# of a flat table that decodes the filter columns first and every other column only for
# the rows that matched. Pruning only skips data that cannot match: the filter still runs
# on every row read, and statistics count only under Parquet's sort-order rules
# (`_statisticsfacts`); metadata outside them is ignored.

# What a predicate can evaluate to over some rows, as bits.
const _CAN_TRUE = 0x01
const _CAN_FALSE = 0x02
const _CAN_MISSING = 0x04
const _CAN_ANY = 0x07

_has(can::UInt8, bits::UInt8) = !iszero(can & bits)

function _andcan(a::UInt8, b::UInt8)
    missing_ = (_has(a, _CAN_MISSING) && _has(b, _CAN_TRUE | _CAN_MISSING)) ||
        (_has(b, _CAN_MISSING) && _has(a, _CAN_TRUE | _CAN_MISSING))
    return (a & b & _CAN_TRUE) | ((a | b) & _CAN_FALSE) | (missing_ ? _CAN_MISSING : 0x00)
end

function _orcan(a::UInt8, b::UInt8)
    missing_ = (_has(a, _CAN_MISSING) && _has(b, _CAN_FALSE | _CAN_MISSING)) ||
        (_has(b, _CAN_MISSING) && _has(a, _CAN_FALSE | _CAN_MISSING))
    return ((a | b) & _CAN_TRUE) | (a & b & _CAN_FALSE) | (missing_ ? _CAN_MISSING : 0x00)
end

_notcan(a::UInt8) = ((a & _CAN_TRUE) << 1) | ((a & _CAN_FALSE) >> 1) | (a & _CAN_MISSING)

# A filter as a tree over its leaf predicates (comparisons, `colin`, `isnull`, string
# predicates). `field` is the leaf column's index among the read's top-level fields, or 0
# for a column the file does not have (`validate=false`), which reads as all missing.
abstract type _Pred end

struct _PredLeaf <: _Pred
    id::Int
    expr::Tables.ScanExpr
    field::Int
    values::Any
end

struct _PredAll <: _Pred
    args::Vector{_Pred}
end

struct _PredAny <: _Pred
    args::Vector{_Pred}
end

struct _PredNot <: _Pred
    arg::_Pred
end

struct _PredConst <: _Pred
    can::UInt8
end

_can(pred::_PredLeaf, leaves::Vector{UInt8}) = leaves[pred.id]
_can(pred::_PredNot, leaves::Vector{UInt8}) = _notcan(_can(pred.arg, leaves))
_can(pred::_PredConst, ::Vector{UInt8}) = pred.can

function _can(pred::_PredAll, leaves::Vector{UInt8})
    can = _CAN_TRUE
    for arg in pred.args
        can = _andcan(can, _can(arg, leaves))
    end
    return can
end

function _can(pred::_PredAny, leaves::Vector{UInt8})
    can = _CAN_FALSE
    for arg in pred.args
        can = _orcan(can, _can(arg, leaves))
    end
    return can
end

# The members of a `colin` set. A large set of integers, strings, or dates is sorted, so
# a range check is a binary search; NaN members match NaN values under `isequal` sets.
struct _InValues
    values::Vector{Any}
    missing::Bool
    nan::Bool
    sorted::Bool
end

function _invalues(values)
    members = Any[]
    hasmissing = hasnan = false
    for value in values
        if ismissing(value)
            hasmissing = true
        else
            hasnan |= value isa Number && isnan(value)
            push!(members, value)
        end
    end
    sorted = false
    if length(members) > 16 && all(value -> value isa Union{Integer,AbstractString,
            Dates.Date,Dates.DateTime}, members)
        try
            sort!(members)
            sorted = true
        catch err
            err isa InterruptException && rethrow()
        end
    end
    return _InValues(members, hasmissing, hasnan, sorted)
end

## Binding a filter to column types

# `bound` with each comparison checked against the element type of its column (`types`;
# columns it lacks are not checked).
function _bindscan(bound::Tables.BoundScan, types::AbstractDict{Symbol,Type})
    bound.filter === nothing && return bound
    # The filter's literals and the columns' types are known at run time.
    filter = _barrier(_bindfilter, bound.filter, types)::Tables.ScanExpr
    return Tables.BoundScan(bound.columns, filter, bound.filtercols, bound.limit,
        bound.offset, bound.validate)
end

# The element type a filter on the top-level field `node` compares with, or `nothing`
# for a nested field.
function _filtertype(node::SchemaNode, limits::Limits, options::_ReadOptions)
    isempty(node.children) && node.max_repetition_level == 0 || return nothing
    convert, L = _valueconverter(node, limits, options)
    convert isa _StringPayloads{true} && return options.stringtype
    convert isa _StringPayloads{false} && return DataStrings.DataBytes
    return L
end

function _bindfilter(expr, types::AbstractDict{Symbol,Type})
    if expr isa Union{Tables.AndExpr,Tables.OrExpr}
        args = Tables.ScanExpr[_bindfilter(arg, types) for arg in expr.args]
        return expr isa Tables.AndExpr ? Tables.AndExpr(args) : Tables.OrExpr(args)
    elseif expr isa Tables.NotExpr
        return Tables.NotExpr(_bindfilter(expr.arg, types))
    elseif expr isa Union{Tables.Cmp,Tables.In,Tables.StrPred}
        ref = expr.lhs.ref
        T = ref isa Symbol ? get(types, ref, nothing) : nothing
        T === nothing && return expr
        S = Base.nonmissingtype(T)
        S === Union{} && return expr
        if expr isa Tables.StrPred
            _filterkind(S) in (nothing, :string) || throw(ArgumentError(
                "the filter matches text in column $ref, which holds $S values"))
            return expr
        elseif expr isa Tables.Cmp
            return Tables.Cmp(expr.op, expr.lhs, _barrier(_bindliteral, ref, S, expr.rhs))
        end
        return _barrier(_bindin, expr, ref, S)::Tables.ScanExpr
    end
    return expr
end

function _bindin(expr::Tables.In, ref::Symbol, ::Type{S}) where {S}
    bound = Any[_bindliteral(ref, S, value) for value in expr.values]
    return all(((a, b),) -> a === b, zip(bound, expr.values)) ? expr :
        Tables.In(expr.lhs, bound)
end

# What a column of `T` values compares with: values of the same kind, or anything for a
# kind not listed.
_filterkind(::Type{T}) where {T} =
    T <: Real ? :number :
    T <: AbstractString ? :string :
    T <: AbstractChar ? :char :
    T <: Durations.ZonedTimestamp ? :zoned :
    T <: Union{Dates.Date,Dates.DateTime,Durations.Timestamp} ? :local :
    T <: Dates.Time ? :time :
    T <: Dates.Period ? :period : nothing

# A filter literal for a column of `S` values: a `Date`, `DateTime`, or `Timestamp`
# compared with a UTC timestamp is that time in UTC.
function _bindliteral(name::Symbol, ::Type{S}, value) where {S}
    ismissing(value) && return value
    column, literal = _filterkind(S), _filterkind(typeof(value))
    if column === :zoned && literal === :local
        utc = value isa Durations.Timestamp ? repr(value) :
            "Durations.Timestamp{Millisecond}($(repr(value)))"
        S.parameters[2] === :UTC || throw(ArgumentError("the filter compares column " *
            "$name, which holds times in zone $(S.parameters[2]), with $(repr(value)), " *
            "which has no zone; compare with a time in that zone, such as $S($utc, " *
            "Dates.UTC) for $(repr(value)) in UTC"))
        stamp = value isa Durations.Timestamp ? value :
            Durations.Timestamp{Dates.Millisecond}(value)
        return Durations.ZonedTimestamp{_timestampperiod(stamp),:UTC}(stamp, Dates.UTC)
    end
    column === nothing || literal === nothing || column === literal ||
        throw(ArgumentError("the filter compares column $name, which holds $S values, " *
            "with $(repr(value))::$(typeof(value))"))
    return value
end

_timestampperiod(::Durations.Timestamp{P}) where {P} = P

function _preparepredicate(expr, names, leaves::Vector{_PredLeaf})
    if expr isa Tables.AndExpr
        return _PredAll(_Pred[_preparepredicate(arg, names, leaves) for arg in expr.args])
    elseif expr isa Tables.OrExpr
        return _PredAny(_Pred[_preparepredicate(arg, names, leaves) for arg in expr.args])
    elseif expr isa Tables.NotExpr
        return _PredNot(_preparepredicate(expr.arg, names, leaves))
    elseif expr isa Tables.AlwaysTrue
        return _PredConst(_CAN_TRUE)
    elseif expr isa Tables.AlwaysFalse
        return _PredConst(_CAN_FALSE)
    elseif expr isa Union{Tables.Cmp,Tables.In,Tables.IsNull,Tables.StrPred}
        ref = expr.lhs.ref
        field = ref isa Symbol ? something(findfirst(==(ref), names), 0) : 0
        values = nothing
        if expr isa Tables.In
            values = try
                _invalues(expr.values)
            catch err
                err isa InterruptException && rethrow()
                return _PredConst(_CAN_ANY)
            end
        end
        leaf = _PredLeaf(length(leaves) + 1, expr, field, values)
        push!(leaves, leaf)
        return leaf
    end
    return _PredConst(_CAN_ANY)
end

# The non-null values of some rows lie in `lo:hi` (both `nothing` when unknown); `nulls`
# and `values` say whether a null or a non-null value can occur, `nan` whether a NaN can
# (statistics leave NaNs out of their bounds).
struct _ValueRange
    lo::Any
    hi::Any
    nulls::Bool
    values::Bool
    nan::Bool
end

_possible(result::Union{Nothing,Bool}) = result !== false
_negate(result::Union{Nothing,Bool}) = result === nothing ? nothing : !result

# `op(x, value)` as the filter evaluates it, or `nothing` when that throws or is not a Bool.
function _holds(op, x, value)
    result = try
        # Statistics and filter values have types known at run time.
        _barrier(op, x, value)
    catch err
        err isa InterruptException && rethrow()
        return nothing
    end
    return result isa Bool ? result : nothing
end

function _cmpcan(op::Tables.ComparisonOperator, range::_ValueRange, value)
    ismissing(value) && return _CAN_MISSING
    lo, hi = range.lo, range.hi
    if lo === nothing
        canbetrue = canbefalse = true
    elseif op == Tables.OP_EQ || op == Tables.OP_NE
        inside = _possible(_holds(<=, lo, value)) && _possible(_holds(>=, hi, value))
        constant = _holds(==, lo, value) === true && _holds(==, hi, value) === true
        canbetrue, canbefalse = op == Tables.OP_EQ ? (inside, !constant) : (!constant, inside)
    elseif op == Tables.OP_LT
        canbetrue = _possible(_holds(<, lo, value))
        canbefalse = _possible(_negate(_holds(<, hi, value)))
    elseif op == Tables.OP_LE
        canbetrue = _possible(_holds(<=, lo, value))
        canbefalse = _possible(_negate(_holds(<=, hi, value)))
    elseif op == Tables.OP_GT
        canbetrue = _possible(_holds(>, hi, value))
        canbefalse = _possible(_negate(_holds(>, lo, value)))
    else
        canbetrue = _possible(_holds(>=, hi, value))
        canbefalse = _possible(_negate(_holds(>=, lo, value)))
    end
    # A NaN compares false with everything, except under `!=`.
    if range.nan
        op == Tables.OP_NE ? (canbetrue = true) : (canbefalse = true)
    end
    return (canbetrue ? _CAN_TRUE : 0x00) | (canbefalse ? _CAN_FALSE : 0x00)
end

function _incan(members::_InValues, range::_ValueRange)
    can = _CAN_FALSE | (members.missing ? _CAN_MISSING : 0x00)
    range.nan && members.nan && return can | _CAN_TRUE
    lo, hi = range.lo, range.hi
    lo === nothing && return can | _CAN_TRUE
    values = members.values
    if members.sorted
        index = try
            searchsortedfirst(values, lo)
        catch err
            err isa InterruptException && rethrow()
            return can | _CAN_TRUE
        end
        index <= length(values) && _possible(_holds(>=, hi, values[index])) &&
            return can | _CAN_TRUE
        return can
    end
    # ponytail: a large unsortable set is not checked; sort mixed numbers if one shows up.
    length(values) > 1024 && return can | _CAN_TRUE
    for value in values
        _possible(_holds(<=, lo, value)) && _possible(_holds(>=, hi, value)) &&
            return can | _CAN_TRUE
    end
    return can
end

function _strcan(expr::Tables.StrPred, range::_ValueRange)
    lo, hi = range.lo, range.hi
    (expr.kind == Tables.STR_STARTSWITH && lo isa String && hi isa String) ||
        return _CAN_TRUE | _CAN_FALSE
    lo, hi = lo::String, hi::String
    prefix = codeunits(expr.s)
    lowprefix = view(codeunits(lo), 1:min(length(prefix), ncodeunits(lo)))
    # Values with the prefix sort from the prefix itself up to the prefix followed by
    # 0xff bytes, so some lie in lo:hi when hi >= prefix and lo's start <= prefix.
    canbetrue = !(hi < expr.s) && _comparestatisticbytes(lowprefix, prefix) <= 0
    canbefalse = !(startswith(lo, expr.s) && startswith(hi, expr.s))
    return (canbetrue ? _CAN_TRUE : 0x00) | (canbefalse ? _CAN_FALSE : 0x00)
end

# What leaf `leaf` can evaluate to over rows whose values `range` describes.
function _leafcan(leaf::_PredLeaf, range::_ValueRange)
    expr = leaf.expr
    if expr isa Tables.IsNull
        can = (range.nulls ? _CAN_TRUE : 0x00) | (range.values ? _CAN_FALSE : 0x00)
        return expr.negated ? _notcan(can) : can
    end
    can = range.nulls ? _CAN_MISSING : 0x00
    range.values || return can
    expr isa Tables.Cmp && return can | _cmpcan(expr.op, range, expr.rhs)
    expr isa Tables.In && return can | _incan(leaf.values::_InValues, range)
    expr isa Tables.StrPred && return can | _strcan(expr, range)
    return _CAN_ANY
end

# A leaf on a column the file does not have: every value is missing.
function _absentcan(leaf::_PredLeaf)
    expr = leaf.expr
    expr isa Tables.IsNull && return expr.negated ? _CAN_FALSE : _CAN_TRUE
    return _CAN_MISSING
end

# A top-level flat column a filter refers to: its leaf, value conversion, and whether
# NaNs can occur.
struct _PruneField
    node::SchemaNode
    convert::Any
    floating::Bool
    type::Type
end

# What pruning needs about one read: the file, its plan, the prepared filter, and the
# read's fields (`nothing` for nested ones).
struct _PruneContext{F<:File}
    file::F
    plan::_ReadPlan
    pred::_Pred
    leaves::Vector{_PredLeaf}
    fields::Vector{Union{Nothing,_PruneField}}
    budget::_ByteBudget
end

function _prunecontext(file::File, filter, budget::_ByteBudget)
    plan = file.plan
    schema, limits = plan.schema, plan.limits
    leaves = _PredLeaf[]
    pred = _preparepredicate(filter, _tablenames(schema, limits), leaves)
    fields = Union{Nothing,_PruneField}[_prunefield(node, plan)
        for node in schema.root.children]
    return _PruneContext(file, plan, pred, leaves, fields, budget)
end

function _prunefield(node::SchemaNode, plan::_ReadPlan)
    (isempty(node.children) && node.max_repetition_level == 0) || return nothing
    convert, L = _valueconverter(node, plan.limits, plan.options)
    return _PruneField(node, convert, _leafstatisticsemantics(node.element).floating,
        Base.nonmissingtype(_flatoutputtype(L, node)))
end

# The value a statistics bound encodes (PLAIN, without a length prefix), as the column
# reads it, or `nothing` when it does not convert.
function _boundvalue(field::_PruneField, raw::Vector{UInt8})
    return try
        _decodebound(field, raw)
    catch err
        err isa InterruptException && rethrow()
        nothing
    end
end

function _decodebound(field::_PruneField, raw::Vector{UInt8})
    convert = field.convert
    # FIXED_LEN_BYTE_ARRAY statistics order bytes, not the integers they hold.
    convert isa _Int128Value && return nothing
    physical = field.node.element.type_
    physical == Metadata.Type.BOOLEAN && return raw[1] != 0x00
    physical == Metadata.Type.INT32 &&
        return convert(reinterpret(Int32, _readstatisticuint32(raw)))
    physical == Metadata.Type.INT64 &&
        return convert(reinterpret(Int64, _readstatisticuint64(raw)))
    physical == Metadata.Type.FLOAT &&
        return convert(reinterpret(Float32, _readstatisticuint32(raw)))
    physical == Metadata.Type.DOUBLE &&
        return convert(reinterpret(Float64, _readstatisticuint64(raw)))
    if physical in (Metadata.Type.BYTE_ARRAY, Metadata.Type.FIXED_LEN_BYTE_ARRAY)
        convert isa _StringPayloads{true} && return String(copy(raw))
        convert isa _StringPayloads && return copy(raw)
        return convert(_byteview(copy(raw)))
    end
    return nothing
end

# The values of a column chunk (or of one page, whose statistics and row count `md`
# carries) under the trust rules of `_statisticsfacts`.
function _statisticsrange(ctx::_PruneContext, field::_PruneField,
        md::Metadata.ColumnMetaData)
    node = field.node
    rows = md.num_values
    optional = node.max_definition_level > 0
    facts = try
        _statisticsfacts(ctx.plan.schema, Int(node.column_index), ctx.plan.metadata.created_by,
            ctx.plan.metadata.column_orders, md; limits=ctx.plan.limits)
    catch err
        err isa Union{FormatError,LimitError,ArgumentError} || rethrow()
        nothing
    end
    facts === nothing && return _ValueRange(nothing, nothing, optional, rows > 0,
        field.floating)
    # A required column has no nulls, whatever its statistics say.
    nullcount = optional && facts.null_count.state === :known ?
        facts.null_count.value::Int64 : nothing
    nulls = optional && (nullcount === nothing || nullcount > 0)
    values = rows > 0 && (nullcount === nothing || nullcount < rows)
    nan = field.floating && (facts.nan_count.state !== :known ||
        facts.nan_count.value::Int64 > 0)
    lo = hi = nothing
    if values && facts.lower.state === :known && facts.upper.state === :known
        lo = _boundvalue(field, facts.lower.raw::Vector{UInt8})
        hi = _boundvalue(field, facts.upper.raw::Vector{UInt8})
        (lo === nothing || hi === nothing) && (lo = hi = nothing)
    end
    return _ValueRange(lo, hi, nulls, values, nan)
end

function _chunkcolumnmetadata(group::Metadata.RowGroup, node::SchemaNode)
    index = Int(node.column_index)
    1 <= index <= length(group.columns) || return nothing
    md = group.columns[index].meta_data
    md === nothing && return nothing
    (md.type_ == node.element.type_ && md.path_in_schema == node.path) || return nothing
    return md
end

# What each leaf can evaluate to over row group `index`, from its column statistics.
function _groupcans(ctx::_PruneContext, index::Int)
    group = ctx.plan.metadata.row_groups[index]
    cans = Vector{UInt8}(undef, length(ctx.leaves))
    ranges = Dict{Int,_ValueRange}()
    for leaf in ctx.leaves
        if leaf.field == 0
            cans[leaf.id] = _absentcan(leaf)
            continue
        end
        field = ctx.fields[leaf.field]
        if field === nothing
            cans[leaf.id] = leaf.expr isa Tables.IsNull ? _CAN_TRUE | _CAN_FALSE : _CAN_ANY
            continue
        end
        range = get!(ranges, leaf.field) do
            md = _chunkcolumnmetadata(group, field.node)
            md === nothing ? _ValueRange(nothing, nothing,
                field.node.max_definition_level > 0, true, field.floating) :
                _statisticsrange(ctx, field, md)
        end
        cans[leaf.id] = _leafcan(leaf, range)
    end
    return cans
end

## Bloom filters and dictionary pages

# The hashes of the physical values a column chunk stores for `values`, the members of
# an `==` or `colin` leaf, or `nothing` when one of them has a stored form this cannot
# compute (a bloom filter then says nothing).
function _bloomkeys(field::_PruneField, values)
    keys = UInt64[]
    for value in values
        ismissing(value) && continue
        physical = try
            _physicalkeys(field, value)
        catch err
            err isa InterruptException && rethrow()
            return nothing
        end
        physical === nothing && return nothing
        for key in physical
            push!(keys, _bloomhash(key))
        end
    end
    return keys
end

# The stored values that read as values equal to `value` (two for a float zero, none for
# a float that no column value equals), or `nothing`.
function _physicalkeys(field::_PruneField, value)
    element = field.node.element
    convert = field.convert
    if convert isa _StringPayloads
        convert isa _StringPayloads{true} && value isa AbstractString || return nothing
        return (codeunits(String(value)::String),)
    end
    physical = element.type_
    if physical in (Metadata.Type.FLOAT, Metadata.Type.DOUBLE)
        value isa Union{AbstractFloat,Integer} || return nothing
        F = physical == Metadata.Type.FLOAT ? Float32 : Float64
        isnan(value) && return ()
        stored = F(value)
        stored == value || return ()
        return iszero(stored) ? (zero(F), -zero(F)) : (stored,)
    end
    typed = field.type(value)
    typed == value || return nothing
    if physical == Metadata.Type.INT32
        convert === identity && return (Int32(typed),)
        convert isa _IntegerValue && return (typed isa UInt32 ? reinterpret(Int32, typed) :
            Int32(typed),)
        convert isa _DateValue && return (_toparquetdate(typed),)
        convert isa _DecimalValue && return (_toparquetdecimal(element, typed)::Int32,)
    elseif physical == Metadata.Type.INT64
        convert === identity && return (Int64(typed),)
        convert isa _IntegerValue && return (typed isa UInt64 ? reinterpret(Int64, typed) :
            Int64(typed),)
        convert isa _TimestampValue &&
            return (_timestampticks(typed, _temporallogicalkind(element).unit, element),)
        convert isa _DecimalValue && return (_toparquetdecimal(element, typed)::Int64,)
        convert isa _PeriodValue && return (Int64(Dates.value(typed)),)
    elseif physical == Metadata.Type.FIXED_LEN_BYTE_ARRAY
        convert isa _DecimalValue && return (_toparquetdecimal(element, typed)::Vector{UInt8},)
        typed isa UUIDs.UUID && return (_uuidbytes(typed),)
    end
    return nothing
end

# The bitset of a column chunk's bloom filter, or `nothing` without a usable one.
function _bloomfilter(ctx::_PruneContext, md::Metadata.ColumnMetaData)
    offset = md.bloom_filter_offset
    offset === nothing && return nothing
    file = ctx.file
    available = file.footer.offset - offset
    (offset >= 4 && available > 0) || return nothing
    total = _checkedsourcelength(file.source)
    return try
        length = md.bloom_filter_length
        if length === nothing
            window = _contiguous(_readrangeexact(file.source, total, offset,
                min(available, Int64(256))))
            reader = Thrift.Reader(window; limits=ctx.plan.limits)
            header = Thrift.decode(reader, Metadata.BloomFilterHeader)
            length = Int64(Thrift.consumed(reader)) + Int64(header.numBytes)
        end
        0 < length <= min(available, Int64(_BLOOM_MAX_BYTES) + 256) || return nothing
        _charge!(ctx.budget, length)
        _bloomwords(_contiguous(_readrangeexact(file.source, total, offset, Int64(length))))
    catch err
        err isa Union{FormatError,LimitError} || rethrow()
        nothing
    end
end

# Whether every data page of a column chunk is dictionary encoded, by its page encoding
# statistics or, without them, its list of encodings: one with only dictionary and level
# encodings (RLE, BIT_PACKED) has no other data pages, except for BOOLEAN values, which
# RLE can encode.
function _alldictionarypages(md::Metadata.ColumnMetaData)
    stats = md.encoding_stats
    if stats === nothing
        md.type_ == Metadata.Type.BOOLEAN && return false
        any(_isdictionaryencoding, md.encodings) || return false
        return all(encoding -> _isdictionaryencoding(encoding) ||
            encoding in (Metadata.Encoding.RLE, Metadata.Encoding.BIT_PACKED), md.encodings)
    end
    data = false
    for entry in stats
        entry.page_type in (Metadata.PageType.DATA_PAGE, Metadata.PageType.DATA_PAGE_V2) ||
            continue
        entry.count > 0 || continue
        _isdictionaryencoding(entry.encoding) || return false
        data = true
    end
    return data
end

# Dictionary pages larger than this are not decoded to prune: a bloom filter answers
# equality on high-cardinality columns more cheaply.
const _DICTIONARY_CHECK_BYTES = 256 * 1024

# The values of a column chunk's dictionary page, when every data page uses it and the
# page is small, or `nothing`.
function _dictionaryvalues(ctx::_PruneContext, field::_PruneField,
        md::Metadata.ColumnMetaData)
    _alldictionarypages(md) || return nothing
    node, limits = field.node, ctx.plan.limits
    return try
        start, stop = _chunkrange(md, ctx.file.footer.offset)
        start < stop || return nothing
        header, _ = readpageheader(ctx.file.source, start, stop, limits)
        (pagekind(header) === :dictionary &&
            header.uncompressed_page_size <= _DICTIONARY_CHECK_BYTES) || return nothing
        frame = readpage(ctx.file.source, start, stop, limits;
            checksum=ctx.plan.options.checksum)
        P = _physicaleltype(md.type_)
        entries = _validatedpageentrycount(frame, limits)
        _charge!(ctx.budget, _pagecharge(P, frame, entries))
        convert, L = _valueconverter(node, limits, ctx.plan.options)
        values = Base.nonmissingtype(_flatoutputtype(L, node))[]
        # The value types are known at run time.
        _barrier(_dictionarypage!, values, P, convert, frame, md, node, _PageBuffers(),
            limits)
        convert isa _StringPayloads ?
            _payloadvector(node, false, values, convert.buffers) : values
    catch err
        err isa Union{FormatError,LimitError} || rethrow()
        nothing
    end
end

# Narrows what the leaves can evaluate to over row group `group` with its bloom filters
# (`==`, `colin`) and dictionary pages (comparisons, `colin`, string predicates). Each
# is read only for a leaf whose ruling out would rule out the whole group.
function _refinecans!(cans::Vector{UInt8}, ctx::_PruneContext, group::Metadata.RowGroup)
    for leaf in ctx.leaves
        _has(cans[leaf.id], _CAN_TRUE) || continue
        leaf.field == 0 && continue
        field = ctx.fields[leaf.field]
        field === nothing && continue
        expr = leaf.expr
        expr isa Union{Tables.Cmp,Tables.In,Tables.StrPred} || continue
        trial = copy(cans)
        trial[leaf.id] &= ~_CAN_TRUE
        _has(_can(ctx.pred, trial), _CAN_TRUE) && continue
        md = _chunkcolumnmetadata(group, field.node)
        md === nothing && continue
        if (expr isa Tables.Cmp && expr.op == Tables.OP_EQ) || expr isa Tables.In
            members = expr isa Tables.In ? (leaf.values::_InValues).values : (expr.rhs,)
            nanmember = expr isa Tables.In && (leaf.values::_InValues).nan
            keys = nanmember ? nothing : _bloomkeys(field, members)
            words = keys === nothing ? nothing : _bloomfilter(ctx, md)
            if words !== nothing && !any(key -> _bloomcontains(words, key), keys)
                cans[leaf.id] = trial[leaf.id]
                return
            end
        end
        values = _dictionaryvalues(ctx, field, md)
        values === nothing && continue
        mask, _ = _evalmask(expr, NamedTuple{(expr.lhs.ref,)}((values,)), length(values))
        if !any(mask)
            cans[leaf.id] = trial[leaf.id]
            return
        end
    end
    return
end

# Row groups among `groups` whose statistics allow a row to match the file's filter.
function _statisticsgroups(file::File, groups)
    filter = file.plan.bound === nothing ? nothing : file.plan.bound.filter
    filter === nothing && return collect(Int, groups)
    ctx = _prunecontext(file, filter, _ByteBudget(file.plan.limits))
    return Int[index for index in groups if _has(_can(ctx.pred, _groupcans(ctx, index)),
        _CAN_TRUE)]
end

## Page indexes

# One column chunk's data pages from its OffsetIndex: each page's first row (0-based, in
# the row group) and location.
struct _ChunkPages
    starts::Vector{Int}
    locations::Vector{Metadata.PageLocation}
end

# The byte range (start, stop) of an index structure a column chunk points to, or
# `nothing` when the pointer is absent, too long, or does not lie in the file body.
function _indexrange(file::File, offset::Union{Nothing,Int64}, length::Union{Nothing,Int32},
        limits::Limits)
    (offset === nothing || length === nothing) && return nothing
    (offset >= 4 && length > 0 && length <= limits.max_page_index_bytes &&
        offset <= file.footer.offset - length) || return nothing
    return (offset, offset + Int64(length))
end

# The bytes of an index structure the column chunk points to, or `nothing`
# (`_indexrange`).
function _indexbytes(file::File, offset::Union{Nothing,Int64}, length::Union{Nothing,Int32},
        limits::Limits)
    range = _indexrange(file, offset, length, limits)
    range === nothing && return nothing
    total = _checkedsourcelength(file.source)
    return _contiguous(_readrangeexact(file.source, total, range[1], range[2] - range[1]))
end

function _decodeindex(bytes, ::Type{T}, limits::Limits, budget::_ByteBudget) where {T}
    bytes === nothing && return nothing
    return try
        Thrift.decode(Thrift.Reader(bytes; limits=limits, budget=budget), T)
    catch err
        err isa Union{FormatError,LimitError} || rethrow()
        nothing
    end
end

# The chunk's OffsetIndex, or `nothing` when it is absent or inconsistent with the chunk
# (`rows` rows between the byte offsets `start` and `stop`).
function _chunkpages(file::File, chunk::Metadata.ColumnChunk, rows::Int, start::Int64,
        stop::Int64, limits::Limits, budget::_ByteBudget)
    rows > 0 || return nothing
    index = _decodeindex(_indexbytes(file, chunk.offset_index_offset,
        chunk.offset_index_length, limits), Metadata.OffsetIndex, limits, budget)
    index === nothing && return nothing
    locations = index.page_locations
    isempty(locations) && return nothing
    starts = Vector{Int}(undef, length(locations))
    previousrow, previousend = -1, start
    for (position, location) in enumerate(locations)
        row = location.first_row_index
        (position == 1 ? row == 0 : row > previousrow) && row < rows || return nothing
        location.compressed_page_size > 0 && location.offset >= previousend &&
            location.offset <= stop - location.compressed_page_size || return nothing
        starts[position] = Int(row)
        previousrow, previousend = row, location.offset + location.compressed_page_size
    end
    return _ChunkPages(starts, locations)
end

# What each page of the chunk can make leaves on its column evaluate to, from the chunk's
# ColumnIndex: one `_ValueRange` per page, or `nothing` without a usable index.
function _pageranges(ctx::_PruneContext, field::_PruneField, chunk::Metadata.ColumnChunk,
        md::Metadata.ColumnMetaData, pages::_ChunkPages, rows::Int)
    limits = ctx.plan.limits
    # Polars marks float pages that hold a NaN as all null (see `_statisticspolars`).
    field.floating && _statisticspolars(ctx.plan.metadata.created_by) && return nothing
    index = _decodeindex(_indexbytes(ctx.file, chunk.column_index_offset,
        chunk.column_index_length, limits), Metadata.ColumnIndex, limits, ctx.budget)
    index === nothing && return nothing
    count = length(pages.starts)
    (length(index.null_pages) == count && length(index.min_values) == count &&
        length(index.max_values) == count) || return nothing
    nullcounts, nancounts = index.null_counts, index.nan_counts
    (nullcounts === nothing || length(nullcounts) == count) &&
        (nancounts === nothing || length(nancounts) == count) || return nothing
    optional = field.node.max_definition_level > 0
    ranges = Vector{_ValueRange}(undef, count)
    for page in 1:count
        pagerows = (page == count ? rows : pages.starts[page + 1]) - pages.starts[page]
        if optional && index.null_pages[page]
            ranges[page] = _ValueRange(nothing, nothing, true, false, false)
            continue
        end
        statistics = Metadata.Statistics(min_value=index.min_values[page],
            max_value=index.max_values[page],
            null_count=nullcounts === nothing || !optional ? nothing : nullcounts[page],
            nan_count=nancounts === nothing ? nothing : nancounts[page])
        pagemd = _withfields(md; num_values=Int64(pagerows), statistics=statistics)
        ranges[page] = _statisticsrange(ctx, field, pagemd)
    end
    return ranges
end

# Rows of a row group of `rows` rows whose filter result can be `true`: `cans` is what
# each leaf can evaluate to over the whole group, and `pages[leaf]` the first row and
# summary of each page of a leaf with a page index.
function _candidaterows(pred::_Pred, cans::Vector{UInt8},
        pages::Vector{Union{Nothing,Tuple{Vector{Int},Vector{UInt8}}}}, rows::Int)
    breaks = Int[0]
    for page in pages
        page === nothing || append!(breaks, page[1])
    end
    sort!(unique!(breaks))
    current = copy(cans)
    positions = ones(Int, length(pages))
    ranges = UnitRange{Int}[]
    for (index, start) in enumerate(breaks)
        stop = index == length(breaks) ? rows : breaks[index + 1]
        start < stop || continue
        for (leaf, page) in enumerate(pages)
            page === nothing && continue
            starts, pagecans = page
            while positions[leaf] < length(starts) && starts[positions[leaf] + 1] <= start
                positions[leaf] += 1
            end
            current[leaf] = pagecans[positions[leaf]] & cans[leaf]
        end
        _has(_can(pred, current), _CAN_TRUE) || continue
        if !isempty(ranges) && last(ranges[end]) + 1 == start
            ranges[end] = first(ranges[end]):(stop - 1)
        else
            push!(ranges, start:(stop - 1))
        end
    end
    return ranges
end

# A row group and the rows of it a read decodes (0-based, sorted, disjoint ranges).
struct _GroupRows
    group::Int
    rows::Vector{UnitRange{Int}}
end

_rowcount(ranges::Vector{UnitRange{Int}}) = sum(length, ranges; init=0)

# The row groups among `groups` the read's filter can match, each with the rows it can
# match: statistics, bloom filters, and dictionary pages prune whole groups; page indexes
# and page statistics prune pages of the filter columns. Groups are pruned on up to
# `ntasks` tasks.
function _filterparts(file::File, groups, filter, budget::_ByteBudget)
    indices = collect(Int, groups)
    ctx = _prefetchindexes(_prunecontext(file, filter, budget), indices)
    parts = _readmap(indices, _readtasks(file, file.plan.ntasks, length(indices))) do index, _
        return _filterpart(ctx, index)
    end
    return _GroupRows[part for part in parts if part !== nothing]
end

# `file` reading through a `_CoalescedSource` that fetches `ranges` (byte offsets, start
# and stop), each merged run with one request on first use, when its source is not in
# memory.
function _plannedfile(file::File, ranges::Vector{Tuple{Int64,Int64}}, budget::_ByteBudget)
    (file.source isa MemorySource || isempty(ranges)) && return file
    return File(_CoalescedSource(file.source, ranges, budget), file.footer, file.metadata, file.plan,
        false, false)
end

# For a source not in memory, the context with the page indexes of the filter columns of
# the row groups statistics leave, fetched together.
function _prefetchindexes(ctx::_PruneContext, indices::Vector{Int})
    file, limits = ctx.file, ctx.plan.limits
    file.source isa MemorySource && return ctx
    fields = unique!(Int[leaf.field for leaf in ctx.leaves
        if leaf.field > 0 && ctx.fields[leaf.field] !== nothing])
    ranges = Tuple{Int64,Int64}[]
    for index in indices
        _has(_can(ctx.pred, _groupcans(ctx, index)), _CAN_TRUE) || continue
        columns = ctx.plan.metadata.row_groups[index].columns
        for field in fields
            position = Int(ctx.fields[field].node.column_index)
            1 <= position <= length(columns) || continue
            chunk = columns[position]
            for range in (
                    _indexrange(file, chunk.column_index_offset, chunk.column_index_length,
                        limits),
                    _indexrange(file, chunk.offset_index_offset, chunk.offset_index_length,
                        limits))
                range === nothing || push!(ranges, range)
            end
        end
    end
    isempty(ranges) && return ctx
    return _PruneContext(_plannedfile(file, ranges, ctx.budget), ctx.plan, ctx.pred, ctx.leaves,
        ctx.fields, ctx.budget)
end

# The file to decode the rows `wanted` of each `(node, part, wanted)` item from, and each
# item's page locations (`_chunkpages`). For a source not in memory, the file reads
# through one fetch plan: a chunk with an OffsetIndex needs its dictionary page and the
# pages holding wanted rows, one without needs all of it, and the offset indexes are
# fetched together first. A source in memory is returned as is, with the page locations
# `missing` (`_decodepart` finds them).
function _planfetch(file::File, metadata::Metadata.FileMetaData, items, limits::Limits,
        budget::_ByteBudget)
    located = Union{Missing,Nothing,_ChunkPages}[missing for _ in items]
    file.source isa MemorySource && return file, located
    chunks = map(items) do (node, part, _)
        group = metadata.row_groups[part.group]
        chunk = group.columns[Int(node.column_index)]
        return chunk, Int(group.num_rows),
            _chunkrange(_chunkmetadata(chunk, node), file.footer.offset)
    end
    indexranges = Tuple{Int64,Int64}[]
    for (chunk, _, _) in chunks
        range = _indexrange(file, chunk.offset_index_offset, chunk.offset_index_length,
            limits)
        range === nothing || push!(indexranges, range)
    end
    indexfile = _plannedfile(file, indexranges, budget)
    pages = _readmap(chunks, _readtasks(file, file.plan.ntasks, length(chunks))) do (chunk, rows, (start, stop)), _
        return _chunkpages(indexfile, chunk, rows, start, stop, limits, budget)
    end
    ranges = Tuple{Int64,Int64}[]
    for (index, ((_, _, wanted), (_, rows, (start, stop)))) in enumerate(zip(items, chunks))
        located[index] = chunkpages = pages[index]
        if chunkpages === nothing
            push!(ranges, (start, stop))
            continue
        end
        locations = chunkpages.locations
        first(locations).offset > start && push!(ranges, (start, first(locations).offset))
        next = 1
        for (page, location) in enumerate(locations)
            stoprow = page == length(locations) ? rows : chunkpages.starts[page + 1]
            while next <= length(wanted) && last(wanted[next]) < chunkpages.starts[page]
                next += 1
            end
            next <= length(wanted) && first(wanted[next]) < stoprow &&
                push!(ranges, (location.offset, location.offset + location.compressed_page_size))
        end
    end
    return _plannedfile(file, ranges, budget), located
end

function _filterpart(ctx::_PruneContext, index::Int)
    group = ctx.plan.metadata.row_groups[index]
    rows = Int(group.num_rows)
    rows > 0 || return nothing
    cans = _groupcans(ctx, index)
    _has(_can(ctx.pred, cans), _CAN_TRUE) || return nothing
    _refinecans!(cans, ctx, group)
    _has(_can(ctx.pred, cans), _CAN_TRUE) || return nothing
    pages = Union{Nothing,Tuple{Vector{Int},Vector{UInt8}}}[nothing for _ in ctx.leaves]
    ranges = Dict{Int,Union{Nothing,Tuple{Vector{Int},Vector{_ValueRange}}}}()
    for leaf in ctx.leaves
        leaf.field == 0 && continue
        field = ctx.fields[leaf.field]
        field === nothing && continue
        pageranges = get!(ranges, leaf.field) do
            _fieldpageranges(ctx, field, group, rows)
        end
        pageranges === nothing && continue
        starts, valueranges = pageranges
        pages[leaf.id] = (starts, UInt8[_leafcan(leaf, range) for range in valueranges])
    end
    candidates = all(isnothing, pages) ? [0:(rows - 1)] :
        _candidaterows(ctx.pred, cans, pages, rows)
    return isempty(candidates) ? nothing : _GroupRows(index, candidates)
end

# The first row and value range of each page of a flat column chunk: from its page index
# (ColumnIndex and OffsetIndex), or else from the statistics in its data page headers,
# which pyarrow writes by default (in memory only: walking a remote chunk's headers would
# fetch it).
function _fieldpageranges(ctx::_PruneContext, field::_PruneField, group::Metadata.RowGroup,
        rows::Int)
    md = _chunkcolumnmetadata(group, field.node)
    md === nothing && return nothing
    md.num_values == rows || return nothing
    chunk = group.columns[Int(field.node.column_index)]
    start, stop = try
        _chunkrange(md, ctx.file.footer.offset)
    catch err
        err isa FormatError || rethrow()
        return nothing
    end
    if chunk.column_index_offset === nothing || chunk.offset_index_offset === nothing
        ctx.file.source isa MemorySource || return nothing
        return _headerpageranges(ctx, field, md, rows, start, stop)
    end
    pages = _chunkpages(ctx.file, chunk, rows, start, stop, ctx.plan.limits, ctx.budget)
    pages === nothing && return nothing
    valueranges = _pageranges(ctx, field, chunk, md, pages, rows)
    valueranges === nothing && return nothing
    return pages.starts, valueranges
end

function _headerpageranges(ctx::_PruneContext, field::_PruneField,
        md::Metadata.ColumnMetaData, rows::Int, start::Int64, stop::Int64)
    src, limits = ctx.file.source, ctx.plan.limits
    starts, ranges = Int[], _ValueRange[]
    optional = field.node.max_definition_level > 0
    position, row = start, 0
    try
        while position < stop
            header, headerlength = readpageheader(src, position, stop, limits)
            kind = validatepageheader(header)
            frameend = _pageframeend(position, headerlength, header.compressed_page_size)
            frameend <= stop || return nothing
            if kind === :data_v1 || kind === :data_v2
                data = kind === :data_v1 ? header.data_page_header :
                    header.data_page_header_v2
                count = Int(data.num_values)
                0 <= count <= rows - row || return nothing
                push!(starts, row)
                push!(ranges, data.statistics === nothing ?
                    _ValueRange(nothing, nothing, optional, count > 0, field.floating) :
                    _statisticsrange(ctx, field, _withfields(md; num_values=Int64(count),
                        statistics=data.statistics)))
                row += count
            end
            position = frameend
        end
    catch err
        err isa Union{FormatError,LimitError} || rethrow()
        return nothing
    end
    row == rows || return nothing
    return starts, ranges
end

# Keeps the rows of `selected` (row ranges per part, in read order) inside the window
# that skips `offset` rows and then keeps at most `limit`.
function _windowrows!(selected::Vector{Vector{UnitRange{Int}}}, offset::Int, limit)
    skip, keep = offset, limit
    for (index, ranges) in enumerate(selected)
        kept = UnitRange{Int}[]
        for range in ranges
            keep === nothing || keep > 0 || break
            if skip >= length(range)
                skip -= length(range)
                continue
            end
            range = (first(range) + skip):last(range)
            skip = 0
            if keep !== nothing
                length(range) > keep && (range = first(range):(first(range) + keep - 1))
                keep -= length(range)
            end
            push!(kept, range)
        end
        selected[index] = kept
    end
    return selected
end

# The parts of `parts` inside the row window, without parts left empty.
function _windowparts(parts::Vector{_GroupRows}, offset::Int, limit)
    selected = _windowrows!(Vector{UnitRange{Int}}[part.rows for part in parts], offset, limit)
    return _GroupRows[_GroupRows(part.group, rows) for (part, rows) in zip(parts, selected)
        if !isempty(rows)]
end

## Reading selected rows of flat columns

# Decodes the data pages of a flat column chunk that hold any of `wanted` (0-based,
# sorted, disjoint row ranges of its row group) into `sink`, in order; other data pages
# are skipped without decompressing. Without `gather`, the pages' values accumulate in
# the sink; returns the row ranges of the pages decoded and how many of a STRING
# converter's buffers hold the dictionary page. With a `_PageGather` or `_PageFilter`,
# each page's wanted rows go to it as soon as the page is decoded, and the sink only ever
# holds one page. With the chunk's OffsetIndex (`pages`), the walk goes from the
# dictionary page to each wanted page directly, reads each page only within the bytes the
# index gives it (so a remote read touches only the planned ranges), and each page read
# must start and end where the index says.
function _readselectedchunk!(sink, ::Type{P}, ::Type{L}, convert, src::AbstractSource,
        md::Metadata.ColumnMetaData, node::SchemaNode, start::Int64, stop::Int64,
        limits::Limits, budget::_ByteBudget, buffers::_PageBuffers,
        wanted::Vector{UnitRange{Int}}, pages::Union{Nothing,_ChunkPages},
        gather=nothing; checksum::UInt8=_CRC_ALL) where {P,L}
    decoded = UnitRange{Int}[]
    isempty(wanted) && return decoded, 0
    total = Int(md.num_values)
    dictionary = _scratch(buffers.dictionary, L)
    hasdictionary = false
    kept = 0
    lastrow = last(wanted[end])
    position, row, page, frames, next = start, 0, 0, Int64(0), 1
    if pages !== nothing && start == first(pages.locations).offset
        # No dictionary page: straight to the first page that holds a wanted row.
        page = _pagestoskip(pages, wanted[next], 0, total)
        row, position = pages.starts[page + 1], pages.locations[page + 1].offset
    end
    while position < stop && row <= lastrow
        limit = stop
        if pages !== nothing
            if page < length(pages.starts) && position == pages.locations[page + 1].offset
                location = pages.locations[page + 1]
                limit = min(stop, location.offset + location.compressed_page_size)
            elseif page == 0
                limit = min(stop, first(pages.locations).offset)
            end
        end
        header, headerlength = readpageheader(src, position, limit, limits)
        kind = validatepageheader(header)
        frameend = _pageframeend(position, headerlength, header.compressed_page_size)
        frameend <= limit || throw(FormatError(limit < stop ?
            "the offset index of column $(join(node.path, '.')) does not match its pages" :
            "page frame extends past the column chunk"))
        frames = _nextpageframecount(frames, limits)
        if kind === :data_v1 || kind === :data_v2
            count = Int(kind === :data_v1 ? header.data_page_header.num_values :
                header.data_page_header_v2.num_values)
            0 <= count <= total - row || throw(FormatError(
                "data pages carry more values than declared by the column chunk"))
            page += 1
            pages === nothing || (page <= length(pages.starts) &&
                pages.starts[page] == row && pages.locations[page].offset == position &&
                frameend == limit &&
                (page == length(pages.starts) ? total : pages.starts[page + 1]) - row ==
                    count) ||
                throw(FormatError("the offset index of column $(join(node.path, '.')) " *
                    "does not match its pages"))
            while next <= length(wanted) && last(wanted[next]) < row
                next += 1
            end
            if next <= length(wanted) && first(wanted[next]) < row + count
                frame = readpage(src, position, limit, limits; checksum=checksum)
                _charge!(budget, _pagecharge(P, frame, Int64(count)))
                if kind === :data_v1
                    _datapagev1!(sink, P, convert, frame, md, node, dictionary,
                        hasdictionary, total - row, buffers, limits)
                else
                    _datapagev2!(sink, P, convert, frame, md, node, dictionary,
                        hasdictionary, total - row, buffers, limits)
                end
                if gather === nothing
                    push!(decoded, row:(row + count - 1))
                else
                    _gatherpage!(gather, sink, convert, row, count, kept)
                end
            end
            row += count
            if pages !== nothing && page < length(pages.starts)
                while next <= length(wanted) && last(wanted[next]) < row
                    next += 1
                end
                next > length(wanted) && break
                target = _pagestoskip(pages, wanted[next], page, total)
                if target > page
                    page = target
                    row = pages.starts[target + 1]
                    frameend = pages.locations[target + 1].offset
                end
            end
        elseif kind === :dictionary
            (page == 0 && !hasdictionary) || throw(FormatError(
                "dictionary page follows a data page in the column chunk"))
            frame = readpage(src, position, limit, limits; checksum=checksum)
            entries = _validatedpageentrycount(frame, limits)
            _charge!(budget, _pagecharge(P, frame, entries))
            _dictionarypage!(dictionary, P, convert, frame, md, node, buffers, limits)
            hasdictionary = true
            kept = convert isa _StringPayloads ? length(convert.buffers) : 0
            if pages !== nothing
                # Straight to the first page that holds a wanted row.
                page = _pagestoskip(pages, wanted[next], 0, total)
                row = pages.starts[page + 1]
                frameend = pages.locations[page + 1].offset
            end
        end
        position = frameend
    end
    row > lastrow || throw(FormatError(
        "column chunk ended before requested row $(lastrow + 1)"))
    gather === nothing && _rowcount(decoded) != sink.filled && throw(AssertionError(
        "selected pages decoded $(sink.filled) values for $(_rowcount(decoded)) rows"))
    return decoded, kept
end

# How many pages, counting from page `page` (0-based), end at or before the first row of
# `range`: those to skip to reach the page holding it.
function _pagestoskip(pages::_ChunkPages, range::UnitRange{Int}, page::Int, total::Int)
    count = length(pages.starts)
    while page < count &&
            (page + 1 == count ? total : pages.starts[page + 2]) <= first(range)
        page += 1
    end
    return page
end

# The destination of a column's wanted rows, `dest[(offset + 1):]` onward, filled page by
# page: `next` is the first range of `wanted` not yet complete. STRING payloads end up
# pointing into `buffers` (see `_keepstrings!`).
mutable struct _PageGather{O}
    dest::Vector{O}
    offset::Int
    wanted::Vector{UnitRange{Int}}
    next::Int
    buffers::Vector{Vector{UInt8}}
end

_PageGather(dest::Vector, offset::Int, wanted::Vector{UnitRange{Int}}) =
    _PageGather(dest, offset, wanted, 1, Vector{UInt8}[])

# Moves the wanted rows of the page just decoded (the sink's first `count` values, rows
# `row` onward) to the gather's destination and empties the sink. `dictionary` is how
# many of a STRING converter's buffers hold its dictionary page.
function _gatherpage!(gather::_PageGather, sink, convert, row::Int, count::Int,
        dictionary::Int)
    sink.filled == count || throw(AssertionError("page sink holds $(sink.filled) of $count values"))
    stop = row + count - 1
    wanted = gather.wanted
    first_ = gather.offset + 1
    index = gather.next
    while index <= length(wanted) && last(wanted[index]) < row
        index += 1
    end
    while index <= length(wanted) && first(wanted[index]) <= stop
        low, high = max(first(wanted[index]), row), min(last(wanted[index]), stop)
        _growto!(gather.dest, gather.offset + high - low + 1)
        copyto!(gather.dest, gather.offset + 1, sink.output, low - row + 1, high - low + 1)
        gather.offset += high - low + 1
        last(wanted[index]) <= stop || break
        index += 1
    end
    gather.next = index
    sink.filled = 0
    convert isa _StringPayloads || return
    keptpage = _keepstrings!(gather.dest, first_:gather.offset, convert.buffers,
        dictionary, gather.buffers)
    if length(convert.buffers) > dictionary
        # Unless kept, the page's own buffers are no longer pointed into; the next page
        # decompresses into one.
        keptpage || (convert.spare = convert.buffers[dictionary + 1])
        resize!(convert.buffers, dictionary)
    end
    return
end

# Makes the STRING payloads `dest[range]`, which point into `source` (the first
# `dictionary` of them a dictionary page's), point into `kept` instead, whose first
# buffers are the dictionary's and whose next is a buffer of copied values. Payloads into
# the other buffers keep those buffers when they use at least half of their bytes, and
# are copied otherwise, so a few rows never hold whole pages alive. Returns whether the
# other buffers were kept.
function _keepstrings!(dest::Vector{DataStrings.StringPayload}, range::UnitRange{Int},
        source::Vector{Vector{UInt8}}, dictionary::Int, kept::Vector{Vector{UInt8}})
    if isempty(kept)
        append!(kept, view(source, 1:dictionary))
        push!(kept, UInt8[])
    end
    length(source) > dictionary || return false
    used = 0
    for index in range
        payload = dest[index]
        length = Int(DataStrings.payloadlength(payload))
        length > DataStrings.INLINE_MAX && DataStrings.payloadbufidx(payload) >= dictionary &&
            (used += length)
    end
    used == 0 && return false
    if 2 * used >= sum(Base.length, view(source, (dictionary + 1):Base.length(source)))
        base = Base.length(kept) - dictionary
        append!(kept, view(source, (dictionary + 1):Base.length(source)))
        for index in range
            payload = dest[index]
            DataStrings.payloadlength(payload) > DataStrings.INLINE_MAX || continue
            buffer = Int(DataStrings.payloadbufidx(payload))
            buffer >= dictionary || continue
            dest[index] = DataStrings.StringPayload(payload.a,
                (payload.b & 0xffffffff00000000) | UInt64(buffer + base))
        end
        return true
    end
    copies = kept[dictionary + 1]
    position = Base.length(copies) + 1
    resize!(copies, Base.length(copies) + used)
    for index in range
        payload = dest[index]
        length = Int(DataStrings.payloadlength(payload))
        length > DataStrings.INLINE_MAX || continue
        buffer = Int(DataStrings.payloadbufidx(payload))
        buffer >= dictionary || continue
        copyto!(copies, position, source[buffer + 1], Int(DataStrings.payloadpos(payload)),
            length)
        dest[index] = DataStrings.view_payload(copies, position, length, dictionary,
            position - 1)
        position += length
    end
    return false
end

# A filter on one column, evaluated on each page as it decodes (`_readselectedchunk!`):
# the rows of `wanted` where it is true collect in `rows` and, when the column is read
# for the output (`keep`), their values in `values`, with STRING values pointing into
# `buffers` (see `_keepstrings!`).
mutable struct _PageFilter
    filter::Tables.ScanExpr
    name::Symbol
    node::SchemaNode
    wanted::Vector{UnitRange{Int}}
    keep::Bool
    rows::Vector{UnitRange{Int}}
    values::Any
    buffers::Vector{Vector{UInt8}}
end

_PageFilter(filter, name::Symbol, node::SchemaNode, wanted::Vector{UnitRange{Int}},
    keep::Bool) = _PageFilter(filter, name, node, wanted, keep, UnitRange{Int}[], nothing,
    Vector{UInt8}[])

function _gatherpage!(filter::_PageFilter, sink, convert, row::Int, count::Int,
        dictionary::Int)
    sink.filled == count || throw(AssertionError("page sink holds $(sink.filled) of $count values"))
    page = sink.output
    length(page) == count || resize!(page, count)
    column = convert isa _StringPayloads ?
        _payloadvector(filter.node, filter.node.max_definition_level > 0, page,
            convert.buffers) : page
    mask, _ = _evalmask(filter.filter, NamedTuple{(filter.name,)}((column,)), count)
    filter.values === nothing && (filter.values = similar(page, 0))
    values = filter.values
    first_ = length(values) + 1
    for range in _maskranges(mask, [row:(row + count - 1)])
        for wanted in filter.wanted
            low, high = max(first(range), first(wanted)), min(last(range), last(wanted))
            low <= high || continue
            push!(filter.rows, low:high)
            filter.keep && append!(values, view(page, (low - row + 1):(high - row + 1)))
        end
    end
    sink.filled = 0
    convert isa _StringPayloads || return
    keptpage = filter.keep && _keepstrings!(values, first_:length(values), convert.buffers,
        dictionary, filter.buffers)
    if length(convert.buffers) > dictionary
        keptpage || (convert.spare = convert.buffers[dictionary + 1])
        resize!(convert.buffers, dictionary)
    end
    return
end

# Values decoded from the pages of one column chunk: `values` holds the rows of `rows`
# in order; `pages` keeps the page buffers STRING payloads point into, the first
# `dictionary` of them the dictionary page's.
struct _DecodedPart{V<:AbstractVector}
    values::V
    rows::Vector{UnitRange{Int}}
    pages::Union{Nothing,Vector{Vector{UInt8}}}
    dictionary::Int
end

# Decodes the pages of a flat column chunk of part `part` that hold rows of `wanted`.
# With a `_PageGather`, the wanted rows go straight to its destination and the result is
# `nothing`; with a `_PageFilter`, the result holds the rows and values it kept. `pages`
# is the chunk's page locations when `_planfetch` found them for `file`.
function _decodepart(file::File, metadata::Metadata.FileMetaData, part::_GroupRows,
        wanted::Vector{UnitRange{Int}}, node::SchemaNode, limits::Limits,
        budget::_ByteBudget, buffers::_PageBuffers, options::_ReadOptions,
        gather::Union{Nothing,_PageGather,_PageFilter}=nothing; pages=missing)
    group = metadata.row_groups[part.group]
    try
        return _decodechunk(file, group, wanted, node, limits, budget, buffers, options,
            gather, pages)
    catch err
        err isa FormatError || rethrow()
        throw(_chunkerror(err, file, group, node))
    end
end

function _decodechunk(file::File, group::Metadata.RowGroup, wanted::Vector{UnitRange{Int}},
        node::SchemaNode, limits::Limits, budget::_ByteBudget, buffers::_PageBuffers,
        options::_ReadOptions, gather, pages)
    chunk = group.columns[Int(node.column_index)]
    md = _chunkmetadata(chunk, node)
    md.num_values == group.num_rows || throw(FormatError(
        "flat column $(node.column_index) has $(md.num_values) values for " *
        "$(group.num_rows) rows"))
    start, stop = _chunkrange(md, file.footer.offset)
    convert, L = _valueconverter(node, limits, options)
    pages === missing &&
        (pages = _chunkpages(file, chunk, Int(group.num_rows), start, stop, limits, budget))
    # A planned source holds the pages the offset index leads to; without an index the
    # chunk is read whole.
    src = file.source
    src isa MemorySource || pages !== nothing || (src = _ChunkSource(src, start, stop, budget))
    return _barrier(_decodechunk, _flatoutputtype(L, node), convert, src, md, node, start,
        stop, limits, budget, buffers, wanted, pages, gather,
        options.checksum)::Union{Nothing,_DecodedPart}
end

function _decodechunk(::Type{O}, convert, src::AbstractSource, md::Metadata.ColumnMetaData,
        node::SchemaNode, start::Int64, stop::Int64, limits::Limits, budget::_ByteBudget,
        buffers::_PageBuffers, wanted::Vector{UnitRange{Int}},
        pages::Union{Nothing,_ChunkPages}, gather, checksum::UInt8) where {O}
    L = Base.nonmissingtype(O)
    # A gather keeps one page at a time in a scratch vector of the task (keyed apart from
    # the sinks' own scratch, which is keyed by element type).
    values = gather === nothing ?
        sizehint!(O[], _claimcapacity(_rowcount(wanted), stop - start)) :
        empty!(_scratch(buffers.dense, O, Tuple{O}))
    sink = _FlatSink{O,L,node.max_definition_level == 0}(values, 0,
        _scratch(buffers.dense, L), UInt16(node.max_definition_level))
    rows, dictionary = _barrier(_readselectedchunk!, sink, _physicaleltype(md.type_), L,
        convert, src, md, node, start, stop, limits, budget, buffers, wanted, pages,
        gather; checksum=checksum)::Tuple{Vector{UnitRange{Int}},Int}
    if gather isa _PageFilter
        # The kept values (only those that matched) and the buffers they point into,
        # all of which stay.
        kept = something(gather.values, O[])::Vector{O}
        return _DecodedPart(kept, gather.rows,
            convert isa _StringPayloads ? gather.buffers : nothing, length(gather.buffers))
    end
    gather === nothing || return nothing
    return _DecodedPart(values, rows,
        convert isa _StringPayloads ? convert.buffers : nothing, dictionary)
end

# Copies the rows of `wanted` from `values`, whose rows are the ranges `rows` in order,
# to `dest[(offset + 1):]`. `wanted` must lie inside `rows`.
function _gatherrows!(dest::AbstractVector, offset::Int, values::AbstractVector,
        rows::Vector{UnitRange{Int}}, wanted::Vector{UnitRange{Int}})
    source, base = 1, 0
    for range in wanted
        row = first(range)
        while row <= last(range)
            while row > last(rows[source])
                base += length(rows[source])
                source += 1
            end
            first(rows[source]) <= row || throw(AssertionError("wanted row $row was not decoded"))
            count = min(last(range), last(rows[source])) - row + 1
            copyto!(dest, offset + 1, values, base + row - first(rows[source]) + 1, count)
            offset += count
            row += count
        end
    end
    return offset
end

function _alignedvalues(decoded::_DecodedPart, wanted::Vector{UnitRange{Int}})
    # The decoded pages cover `wanted`, so equal counts mean the same rows.
    _rowcount(decoded.rows) == _rowcount(wanted) && return decoded.values
    values = similar(decoded.values, _rowcount(wanted))
    _gatherrows!(values, 0, decoded.values, decoded.rows, wanted)
    return values
end

# The values of `decoded` for the rows of `wanted`, as a filter sees them.
_filtervalues(decoded::_DecodedPart, wanted::Vector{UnitRange{Int}}, node::SchemaNode) =
    _filtercolumn(_alignedvalues(decoded, wanted), decoded, node)

# The column a filter sees: STRING payloads as a StringVector over their pages.
function _filtercolumn(values::AbstractVector, decoded::_DecodedPart, node::SchemaNode)
    values isa Vector{DataStrings.StringPayload} || return values
    return _payloadvector(node, node.max_definition_level > 0, values, decoded.pages)
end

## Evaluating a filter on decoded columns

const _CMPOPS = (==, !=, <, <=, >, >=)

# Rows of `columns` (`rows` of them) where the resolved filter `expr` is `true`, with the
# semantics of `Tables.filtermask`: a comparison with a missing value is missing, `&`,
# `|`, and `!` follow three-valued logic, and only `true` keeps a row. A column the read
# does not have (`validate=false`) is all missing. Returns (true rows, false rows).
function _evalmask(expr, columns::NamedTuple, rows::Int)
    if expr isa Tables.AndExpr || expr isa Tables.OrExpr
        all_ = expr isa Tables.AndExpr
        t, f = all_ ? (trues(rows), falses(rows)) : (falses(rows), trues(rows))
        for arg in expr.args
            argt, argf = _evalmask(arg, columns, rows)
            if all_
                t .&= argt
                f .|= argf
            else
                t .|= argt
                f .&= argf
            end
        end
        return t, f
    elseif expr isa Tables.NotExpr
        t, f = _evalmask(expr.arg, columns, rows)
        return f, t
    elseif expr isa Tables.AlwaysTrue
        return trues(rows), falses(rows)
    elseif expr isa Tables.AlwaysFalse
        return falses(rows), trues(rows)
    elseif expr isa Union{Tables.Cmp,Tables.In,Tables.IsNull,Tables.StrPred}
        ref = expr.lhs.ref
        column = ref isa Symbol && haskey(columns, ref) ? columns[ref] : nothing
        if column === nothing
            expr isa Tables.IsNull || return falses(rows), falses(rows)
            return expr.negated ? (falses(rows), trues(rows)) : (trues(rows), falses(rows))
        end
        # The column and the value are known only at run time: the masks compute behind
        # `_barrier`, compiled for their concrete types.
        t, f = BitVector(undef, rows), BitVector(undef, rows)
        if expr isa Tables.IsNull
            _barrier(_nullmask!, t, f, column, expr.negated)
        elseif expr isa Tables.Cmp
            value = _barrier(_columnvalue, Base.nonmissingtype(eltype(column)), expr.rhs)
            _barrier(_leafmask!, t, f, column, Base.Fix2(_CMPOPS[Int(expr.op)], value))
        elseif expr isa Tables.In
            _barrier(_leafmask!, t, f, column, Base.Fix2(in, expr.values))
        else
            predicate = expr.kind == Tables.STR_STARTSWITH ? startswith :
                expr.kind == Tables.STR_ENDSWITH ? endswith : contains
            _barrier(_leafmask!, t, f, column, Base.Fix2(predicate, expr.s))
        end
        return t, f
    end
    throw(ArgumentError("cannot evaluate filter node $(typeof(expr))"))
end

# A comparison's value as the column's element type when it converts to an equal value,
# so each row compares within one type (a Timestamp column with a DateTime, an Int32
# column with an Int64); otherwise the value itself.
function _columnvalue(::Type{T}, value) where {T}
    value isa T && return value
    converted = try
        T(value)
    catch err
        err isa InterruptException && rethrow()
        return value
    end
    return converted isa T && _holds(==, converted, value) === true ? converted : value
end

# The masks and the column have the same indices. (Two-argument `eachindex` would check
# that too, but its error path goes through code that loading CategoricalArrays
# invalidates.)
function _leafmask!(t::BitVector, f::BitVector, column::AbstractVector, predicate::F) where {F}
    _checkmaskaxes(t, column)
    @inbounds for index in eachindex(t)
        value = column[index]
        result = ismissing(value) ? missing : predicate(value)
        t[index] = result === true
        f[index] = result === false
    end
    return
end

function _nullmask!(t::BitVector, f::BitVector, column::AbstractVector, negated::Bool)
    _checkmaskaxes(t, column)
    @inbounds for index in eachindex(t)
        null = ismissing(column[index])
        t[index] = null != negated
        f[index] = null == negated
    end
    return
end

_checkmaskaxes(mask::BitVector, column::AbstractVector) = axes(column) == axes(mask) ||
    throw(DimensionMismatch("a filter column has $(length(column)) values for " *
        "$(length(mask)) rows"))

# The rows (0-based ranges of the group) where `mask`, over the rows of `rows` in order,
# is true.
function _maskranges(mask::AbstractVector{Bool}, rows::Vector{UnitRange{Int}})
    ranges = UnitRange{Int}[]
    index = 0
    for range in rows
        runstart = -1
        for row in range
            index += 1
            if @inbounds mask[index]
                runstart < 0 && (runstart = row)
            elseif runstart >= 0
                push!(ranges, runstart:(row - 1))
                runstart = -1
            end
        end
        runstart >= 0 && push!(ranges, runstart:last(range))
    end
    return ranges
end

# Columns of a flat table for the rows of `parts`: with `filtered`, the filter columns
# are decoded first, only for the rows pruning left, and the other columns only for the
# rows where the filter is true, inside the window (`offset`, `limit`). Without it, every
# column is decoded for the rows of `parts`. Returns the columns of every read field
# (fields that are only filtered on hold empty placeholders) and the row count.
function _readselectedflat(file::File, plan::_ReadPlan, parts::Vector{_GroupRows},
        filtered::Bool, offset::Int, limit, budget::_ByteBudget)
    metadata, schema, limits, options = plan.metadata, plan.schema, plan.limits, plan.options
    fields = schema.root.children
    names = _tablenames(schema, limits)
    bound = plan.bound
    positions = bound === nothing ? collect(eachindex(fields)) : sort!(unique!(vcat(
        Int[column.index for column in bound.columns], bound.filtercols)))
    fieldof(position) = searchsortedfirst(positions, position)
    outfields = bound === nothing ? collect(eachindex(fields)) :
        sort!(unique!(Int[fieldof(column.index) for column in bound.columns]))
    decoded = Dict{Tuple{Int,Int},_DecodedPart}()
    selected = Vector{UnitRange{Int}}[part.rows for part in parts]
    if filtered
        filterfields = sort!(unique!(Int[fieldof(position) for position in bound.filtercols]))
        _filterselected!(decoded, selected, file, plan, parts, filterfields, outfields,
            names, offset, limit, budget)
        _windowrows!(selected, offset, limit)
    end
    counts = Int[_rowcount(rows) for rows in selected]
    starts = cumsum(counts) .- counts
    total = sum(counts; init=0)
    _checklimit(:container_elements, total, limits.max_container_elements)
    _chargeflatallocation!(fields, limits, budget, options) do index
        index in outfields || return 0
        return filtered || _partsbacked(metadata, parts, counts, fields[index]) ? total :
            min(total, 1 << 20)
    end
    columns = AbstractVector[]
    pages = [Vector{Vector{UInt8}}[Vector{UInt8}[] for _ in parts] for _ in fields]
    tasks = Tuple{Int,Int}[]
    for (fieldindex, node) in enumerate(fields)
        _, L = _valueconverter(node, limits, options)
        O = _flatoutputtype(L, node)
        if !(fieldindex in outfields)
            push!(columns, O[])
        elseif filtered || _partsbacked(metadata, parts, counts, node)
            # Row counts that came from decoded values, or that the chunks' bytes can
            # hold, are allocated up front and filled in parallel.
            push!(columns, Vector{O}(undef, total))
            append!(tasks, (fieldindex, index) for index in eachindex(parts)
                if counts[index] > 0)
        else
            # Otherwise the column grows as its pages decode, on one task.
            push!(columns, sizehint!(O[], min(total, 1 << 20)))
            push!(tasks, (fieldindex, 0))
        end
    end
    # Larger chunks first, so the tasks still running at the end are short ones.
    chunkbytes(fieldindex, index) = haskey(decoded, (fieldindex, index)) ? Int64(0) :
        _chunkbytes(metadata, parts[index], fields[fieldindex])
    sort!(tasks; rev=true, by=((fieldindex, task),) -> task == 0 ?
        sum(index -> chunkbytes(fieldindex, index), eachindex(parts); init=Int64(0)) :
        chunkbytes(fieldindex, task))
    # The pages the output columns decode (the filter phase decoded the filter columns).
    decodes = [(fieldindex, index) for fieldindex in outfields for index in eachindex(parts)
        if counts[index] > 0 && !haskey(decoded, (fieldindex, index))]
    file, located = _planfetch(file, metadata,
        [(fields[fieldindex], parts[index], selected[index]) for (fieldindex, index) in decodes],
        limits, budget)
    locatedof = Dict(zip(decodes, located))
    _readmap(tasks, _readtasks(file, plan.ntasks, length(tasks))) do (fieldindex, task), buffers
        for index in (task == 0 ? eachindex(parts) : (task:task))
            counts[index] > 0 || continue
            _barrier(_readselectedpart!, columns[fieldindex], pages[fieldindex], decoded,
                file, metadata, parts, selected, starts, counts, fieldindex, index,
                fields[fieldindex], limits, budget, buffers, options,
                get(locatedof, (fieldindex, index), missing))
        end
        return nothing
    end
    for fieldindex in outfields
        column = columns[fieldindex]
        length(column) == total || throw(FormatError("column " *
            "$(join(fields[fieldindex].path, '.')) has fewer rows than its row groups"))
        column isa Vector{DataStrings.StringPayload} || continue
        columns[fieldindex] = _payloadcolumn(column, fields[fieldindex], pages[fieldindex],
            starts, options.stringtype)
    end
    return columns, total
end

# The compressed size of the chunk of `node` in `part`'s row group.
function _chunkbytes(metadata::Metadata.FileMetaData, part::_GroupRows, node::SchemaNode)
    md = metadata.row_groups[part.group].columns[Int(node.column_index)].meta_data
    return md === nothing ? Int64(0) : md.total_compressed_size
end

# Whether every chunk of `node` in `parts` has the bytes to hold the rows read from it
# (`_claimcapacity`), so a column of `counts` rows can be allocated before decoding.
function _partsbacked(metadata::Metadata.FileMetaData, parts::Vector{_GroupRows},
        counts::Vector{Int}, node::SchemaNode)
    for (part, count) in zip(parts, counts)
        md = metadata.row_groups[part.group].columns[Int(node.column_index)].meta_data
        md === nothing && return false
        count <= _claimcapacity(count, md.total_compressed_size) || return false
    end
    return true
end

# Fills the rows of part `index` of an output column: from the filter phase's decoded
# values when the column was filtered on, else by decoding the pages that hold them
# (`located` is the chunk's page locations from `_planfetch`, or `missing`).
function _readselectedpart!(column::Vector, pages::Vector{Vector{Vector{UInt8}}},
        decoded::Dict{Tuple{Int,Int},_DecodedPart}, file::File,
        metadata::Metadata.FileMetaData, parts::Vector{_GroupRows},
        selected::Vector{Vector{UnitRange{Int}}}, starts::Vector{Int}, counts::Vector{Int},
        fieldindex::Int, index::Int, node::SchemaNode, limits::Limits, budget::_ByteBudget,
        buffers::_PageBuffers, options::_ReadOptions, located)
    piece = get(decoded, (fieldindex, index), nothing)
    if piece === nothing
        gather = _PageGather(column, starts[index], selected[index])
        _decodepart(file, metadata, parts[index], selected[index], node, limits, budget,
            buffers, options, gather; pages=located)
        gather.offset == starts[index] + counts[index] || throw(FormatError(
            "column $(join(node.path, '.')) has fewer rows than its row group"))
        pages[index] = gather.buffers
        return
    end
    _growto!(column, starts[index] + counts[index])
    _barrier(_gatherrows!, column, starts[index], piece.values, piece.rows, selected[index])
    if piece.pages !== nothing
        kept = Vector{UInt8}[]
        _keepstrings!(column, (starts[index] + 1):(starts[index] + counts[index]),
            piece.pages, piece.dictionary, kept)
        pages[index] = kept
    end
    return
end

# Decodes the filter columns of `parts` for their candidate rows and sets `selected[i]`
# to the rows of part `i` where the filter is true; `decoded` keeps the filter columns'
# values for the output. A filter on one column is evaluated page by page as the column
# decodes, and only the matching values are kept. With a `limit`, parts are filtered
# `ntasks` at a time until enough rows matched; later parts select nothing.
function _filterselected!(decoded::Dict{Tuple{Int,Int},_DecodedPart},
        selected::Vector{Vector{UnitRange{Int}}}, file::File, plan::_ReadPlan,
        parts::Vector{_GroupRows}, filterfields::Vector{Int}, outfields::Vector{Int}, names,
        offset::Int, limit, budget::_ByteBudget)
    fields = plan.schema.root.children
    wanted = limit === nothing ? nothing : Base.checked_add(offset, limit)
    size = wanted === nothing ? length(parts) : max(1, plan.ntasks)
    found = 0
    first = 1
    while first <= length(parts)
        last = min(length(parts), first + size - 1)
        batch = first:last
        tasks = [(fieldindex, index) for index in batch for fieldindex in filterfields]
        batchfile, located = _planfetch(file, plan.metadata,
            [(fields[fieldindex], parts[index], parts[index].rows)
                for (fieldindex, index) in tasks], plan.limits, budget)
        # The tasks' variables are their closures' arguments: a closure that assigned a
        # variable of this function would share it among all tasks.
        work = collect(zip(tasks, located))
        if length(filterfields) == 1
            results = _readmap(work, _readtasks(file, plan.ntasks, length(work))) do ((fieldindex, index), pages), buffers
                filter = _PageFilter(plan.bound.filter, names[fieldindex], fields[fieldindex],
                    parts[index].rows, fieldindex in outfields)
                return _decodepart(batchfile, plan.metadata, parts[index], parts[index].rows,
                    fields[fieldindex], plan.limits, budget, buffers, plan.options, filter;
                    pages=pages)
            end
            fieldindex = only(filterfields)
            for (index, result) in zip(batch, results)
                result = result::_DecodedPart
                selected[index] = result.rows
                decoded[(fieldindex, index)] = result
            end
        else
            results = _readmap(work, _readtasks(file, plan.ntasks, length(work))) do ((fieldindex, index), pages), buffers
                return _decodepart(batchfile, plan.metadata, parts[index], parts[index].rows,
                    fields[fieldindex], plan.limits, budget, buffers, plan.options;
                    pages=pages)
            end
            for (task, result) in zip(tasks, results)
                decoded[task] = result
            end
            _readmap(batch, _readtasks(file, plan.ntasks, length(batch))) do index, _
                rows = parts[index].rows
                columns = NamedTuple{Tuple(Symbol[names[fieldindex]
                    for fieldindex in filterfields])}(Tuple(Any[_barrier(_filtervalues,
                        decoded[(fieldindex, index)], rows, fields[fieldindex])
                    for fieldindex in filterfields]))
                mask, _ = _evalmask(plan.bound.filter, columns, _rowcount(rows))
                selected[index] = _maskranges(mask, rows)
                return nothing
            end
        end
        found += sum(index -> _rowcount(selected[index]), batch; init=0)
        first = last + 1
        if wanted !== nothing && found >= wanted
            for index in first:length(parts)
                selected[index] = UnitRange{Int}[]
            end
            break
        end
    end
    return
end
