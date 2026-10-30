module ParquetSentinelArraysExt

using Parquet
using SentinelArrays: ChainedVector

function Parquet._segments(column::ChainedVector, first::Int, last::Int)
    segments = Tuple{eltype(column.arrays),Int,Int}[]
    start = 1
    for array in column.arrays
        stop = start + length(array) - 1
        low, high = max(first, start), min(last, stop)
        offset = firstindex(array) - start
        low <= high && push!(segments, (array, low + offset, high + offset))
        start = stop + 1
    end
    return segments
end

end
