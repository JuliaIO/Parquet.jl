function _writecolumnorder(element::Metadata.SchemaElement)
    semantics = _leafstatisticsemantics(element)
    if semantics.floating
        return Metadata.ColumnOrder(
            IEEE_754_TOTAL_ORDER=Metadata.IEEE754TotalOrder())
    end
    return Metadata.ColumnOrder(TYPE_ORDER=Metadata.TypeDefinedOrder())
end

function _writecolumnorders(schema::Schema, budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        count = length(schema.leaves)
        _reservearray!(budget, Metadata.ColumnOrder, count)
        _reserveobjects!(budget, 2 * count)
        orders = Metadata.ColumnOrder[]
        sizehint!(orders, count)
        for leaf in schema.leaves
            push!(orders, _writecolumnorder(leaf.element))
        end
        return orders
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end

function _writeunsignedlittle!(output::Vector{UInt8}, value::T) where
    {T<:Unsigned}
    length(output) == sizeof(T) || throw(AssertionError(
        "writer statistics scratch has the wrong width"))
    @inbounds for index in eachindex(output)
        output[index] = UInt8(value & T(0xff))
        value >>= 8
    end
    return output
end

function _writefixedstatistic!(output::Vector{UInt8},
        element::Metadata.SchemaElement, value)
    physical = element.type_
    if physical == Metadata.Type.BOOLEAN
        length(output) == 1 || throw(AssertionError(
            "BOOLEAN statistics scratch has the wrong width"))
        value isa Bool || throw(ArgumentError(
            "BOOLEAN writer statistics value is not Bool"))
        output[1] = value ? 0x01 : 0x00
    elseif physical == Metadata.Type.INT32
        value isa Int32 || throw(ArgumentError(
            "INT32 writer statistics value is not Int32"))
        _writeunsignedlittle!(output, reinterpret(UInt32, value))
    elseif physical == Metadata.Type.INT64
        value isa Int64 || throw(ArgumentError(
            "INT64 writer statistics value is not Int64"))
        _writeunsignedlittle!(output, reinterpret(UInt64, value))
    elseif physical == Metadata.Type.FLOAT
        value isa Float32 || throw(ArgumentError(
            "FLOAT writer statistics value is not Float32"))
        _writeunsignedlittle!(output, reinterpret(UInt32, value))
    elseif physical == Metadata.Type.DOUBLE
        value isa Float64 || throw(ArgumentError(
            "DOUBLE writer statistics value is not Float64"))
        _writeunsignedlittle!(output, reinterpret(UInt64, value))
    else
        length(value) == length(output) || throw(ArgumentError(
            "fixed writer statistics value has the wrong width"))
        @inbounds for index in eachindex(output)
            output[index] = value[index]
        end
    end
    return output
end

function _writevariablestatistic(value)
    value isa AbstractString && return codeunits(value)
    value isa AbstractVector{UInt8} && return value
    throw(ArgumentError(
        "BYTE_ARRAY writer statistics value is not a byte sequence"))
end

function _writefloatbits(element::Metadata.SchemaElement, value)
    physical = element.type_
    physical == Metadata.Type.FLOAT && return reinterpret(UInt32, value::Float32)
    physical == Metadata.Type.DOUBLE && return reinterpret(UInt64, value::Float64)
    length(value) == 2 || throw(ArgumentError(
        "FLOAT16 writer statistics value has the wrong width"))
    return UInt16(value[1]) | (UInt16(value[2]) << 8)
end

function _writecountnans(element::Metadata.SchemaElement, values)
    count = Int64(0)
    for value in values
        _statisticisnan(_writefloatbits(element, value)) &&
            (count = Base.checked_add(count, Int64(1)))
    end
    return count
end

function _writestatisticsmetadata(nulls::Int64, nans::Union{Nothing,Int64},
        lower::Union{Nothing,Vector{UInt8}}, upper::Union{Nothing,Vector{UInt8}},
        budget::_LiveByteBudget)
    _reserveobjects!(budget)
    exact = lower === nothing ? nothing : true
    return Metadata.Statistics(
        null_count=nulls,
        min_value=lower,
        max_value=upper,
        is_min_value_exact=exact,
        is_max_value_exact=exact,
        nan_count=nans,
    )
end

function _writefixedstatistics(element::Metadata.SchemaElement, values,
        comparison::Symbol, nulls::Int64, nans::Union{Nothing,Int64},
        width::Int, limit::Int64, budget::_LiveByteBudget)
    isempty(values) && return _writestatisticsmetadata(
        nulls, nans, nothing, nothing, budget)
    width > limit && return _writestatisticsmetadata(
        nulls, nans, nothing, nothing, budget)
    _reservearray!(budget, UInt8, width)
    _reservearray!(budget, UInt8, width)
    scratchcharge = _reservearray!(budget, UInt8, width)
    lower = Vector{UInt8}(undef, width)
    upper = Vector{UInt8}(undef, width)
    scratch = Vector{UInt8}(undef, width)
    skipnans = nans !== nothing && nans < length(values)
    initialized = false
    for value in values
        skipnans && _statisticisnan(_writefloatbits(element, value)) && continue
        _writefixedstatistic!(scratch, element, value)
        if !initialized
            copyto!(lower, scratch)
            copyto!(upper, scratch)
            initialized = true
            continue
        end
        lowerorder = _comparestatisticvalues(element, scratch, lower, comparison)
        upperorder = _comparestatisticvalues(element, scratch, upper, comparison)
        lowerorder === nothing && throw(AssertionError(
            "writer statistics comparator rejected a defined order"))
        upperorder === nothing && throw(AssertionError(
            "writer statistics comparator rejected a defined order"))
        lowerorder < 0 && copyto!(lower, scratch)
        upperorder > 0 && copyto!(upper, scratch)
    end
    initialized || throw(AssertionError(
        "writer statistics found no value for a defined order"))
    _release!(budget, scratchcharge)
    return _writestatisticsmetadata(nulls, nans, lower, upper, budget)
end

function _writevariablestatistics(element::Metadata.SchemaElement, values,
        comparison::Symbol, nulls::Int64, limit::Int64,
        budget::_LiveByteBudget)
    isempty(values) && return _writestatisticsmetadata(
        nulls, nothing, nothing, nothing, budget)
    lowerindex = firstindex(values)
    upperindex = lowerindex
    for index in Iterators.drop(eachindex(values), 1)
        raw = _writevariablestatistic(values[index])
        lower = _writevariablestatistic(values[lowerindex])
        upper = _writevariablestatistic(values[upperindex])
        _comparestatisticvalues(element, raw, lower, comparison) < 0 &&
            (lowerindex = index)
        _comparestatisticvalues(element, raw, upper, comparison) > 0 &&
            (upperindex = index)
    end
    lowerraw = _writevariablestatistic(values[lowerindex])
    upperraw = _writevariablestatistic(values[upperindex])
    (length(lowerraw) > limit || length(upperraw) > limit) &&
        return _writestatisticsmetadata(
            nulls, nothing, nothing, nothing, budget)
    _reservearray!(budget, UInt8, length(lowerraw))
    _reservearray!(budget, UInt8, length(upperraw))
    lower = collect(lowerraw)
    upper = collect(upperraw)
    return _writestatisticsmetadata(nulls, nothing, lower, upper, budget)
end

function _writecolumnstatistics(leaf::WriteLeafPlan,
        element::Metadata.SchemaElement, limit::Int64,
        budget::_LiveByteBudget)
    start = _budgetused(budget)
    try
        entries = Int64(_columnentrycount(leaf.column))
        dense = Int64(length(leaf.column.values))
        dense <= entries || throw(AssertionError(
            "writer dense statistics count exceeds its entry count"))
        nulls = entries - dense
        semantics = _leafstatisticsemantics(element)
        nans = semantics.floating ?
            _writecountnans(element, leaf.column.values) : nothing
        comparison = semantics.floating ? :ieee_total_order : semantics.comparison
        comparison === :undefined && return _writestatisticsmetadata(
            nulls, nans, nothing, nothing, budget)
        width = _statisticplainwidth(element)
        width === nothing && return _writevariablestatistics(element,
            leaf.column.values, comparison, nulls, limit, budget)
        return _writefixedstatistics(element, leaf.column.values, comparison,
            nulls, nans, width, limit, budget)
    catch
        used = _budgetused(budget)
        used > start && _release!(budget, used - start)
        rethrow()
    end
end
