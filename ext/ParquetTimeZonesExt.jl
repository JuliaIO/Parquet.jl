module ParquetTimeZonesExt

using Parquet
using Dates: Millisecond
using Durations: ZonedTimestamp
using TimeZones: ZonedDateTime, timezone

# A column of ZonedDateTime values in one time zone writes as a UTC-adjusted millisecond
# TIMESTAMP whose zone the ARROW:schema keeps, so it reads back as ZonedTimestamp values
# in that zone (Durations converts each value).
function Parquet._writeextensioncolumn(name::String,
        values::AbstractVector{<:Union{Missing,ZonedDateTime}})
    # A column of only missing (Vector{Missing}) stays a null column.
    Base.nonmissingtype(eltype(values)) === ZonedDateTime || return values
    first = findfirst(!ismissing, values)
    T = first === nothing ? ZonedTimestamp{Millisecond,:UTC} :
        typeof(ZonedTimestamp{Millisecond}(values[first]))
    zone = first === nothing ? nothing : timezone(values[first])
    output = Vector{Missing <: eltype(values) ? Union{Missing,T} : T}(undef, length(values))
    for (index, value) in zip(eachindex(output), values)
        if value === missing
            output[index] = missing
        else
            timezone(value) == zone || throw(ArgumentError("column $(repr(name)) holds " *
                "ZonedDateTime values in time zones $(zone) and $(timezone(value)); a " *
                "Parquet column stores one zone, so convert them to one first (for " *
                "example astimezone.(column, tz\"UTC\"))"))
            output[index] = T(value)
        end
    end
    return output
end

end
