# Physical leaf streams use one repetition and definition level per entry, while
# storing only values whose definition reaches the leaf maximum.

function _validateleafstream(repetition::AbstractVector{<:Integer},
    definition::AbstractVector{<:Integer}, values::Vector, maxrepetition::Integer,
    maxdefinition::Integer, expectedrows)
    maxrepetition >= 0 || throw(FormatError("negative maximum repetition level"))
    maxdefinition >= maxrepetition ||
        throw(FormatError("maximum repetition level exceeds maximum definition level"))
    length(repetition) == length(definition) ||
        throw(FormatError("leaf stream repetition and definition counts differ"))
    rows = 0
    present = 0
    for index in eachindex(repetition, definition)
        repetitionlevel = repetition[index]
        definitionlevel = definition[index]
        repetitionlevel <= maxrepetition ||
            throw(FormatError("repetition level $repetitionlevel exceeds the maximum $maxrepetition"))
        definitionlevel <= maxdefinition ||
            throw(FormatError("definition level $definitionlevel exceeds the maximum $maxdefinition"))
        repetitionlevel <= definitionlevel ||
            throw(FormatError("repetition level $repetitionlevel exceeds definition level $definitionlevel"))
        iszero(repetitionlevel) && (rows += 1)
        definitionlevel == maxdefinition && (present += 1)
    end
    isempty(repetition) || iszero(first(repetition)) ||
        throw(FormatError("column chunk starts with repetition level $(first(repetition))"))
    present == length(values) ||
        throw(FormatError("leaf stream has $(length(values)) dense values for $present present entries"))
    if expectedrows !== nothing
        expectedrows >= 0 || throw(FormatError("negative expected row count"))
        rows == expectedrows ||
            throw(FormatError("leaf stream has $rows rows but $expectedrows were expected"))
    end
    return
end

# Levels are at most a schema's maximum level, which is an Int16.
struct LeafStream{T}
    repetition::Vector{UInt16}
    definition::Vector{UInt16}
    values::Vector{T}

    function LeafStream{T}(repetition::AbstractVector{<:Integer},
        definition::AbstractVector{<:Integer}, values::Vector{T},
        maxrepetition::Integer, maxdefinition::Integer;
        expected_rows=nothing) where {T}
        _validateleafstream(repetition, definition, values, maxrepetition, maxdefinition,
            expected_rows)
        return new{T}(_levelvector(repetition), _levelvector(definition), values)
    end
end

function _levelvector(levels::Vector{UInt16})
    return levels
end

function _levelvector(levels::AbstractVector{<:Integer})
    return Vector{UInt16}(levels)
end

function LeafStream(repetition::AbstractVector{<:Integer},
    definition::AbstractVector{<:Integer}, values::Vector{T},
    maxrepetition::Integer, maxdefinition::Integer;
    expected_rows=nothing) where {T}
    return LeafStream{T}(repetition, definition, values, maxrepetition, maxdefinition;
        expected_rows=expected_rows)
end

function Base.length(stream::LeafStream)
    return length(stream.repetition)
end

function Base.isempty(stream::LeafStream)
    return isempty(stream.repetition)
end
