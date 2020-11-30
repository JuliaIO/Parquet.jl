"""
    read_parquet(path)

Returns the table contained in the parquet file in an Tables.jl compatible format.

One can easily convert the returned object to any Tables.jl compatible table e.g.
DataFrames.DataFrame via

```
using DataFrames
df = DataFrame(read_parquet(path; copycols=false))
```
"""
function read_parquet(path)
    parquetfile = Parquet.File(path);

    # read all the chunks
    chunks = [chunk for chunk in BatchedColumnsCursor(parquetfile)]

    tmp_result_arr = Any[]

    column_names = keys(chunks[1])

    for key in column_names
        # combine all chunks' key into one column
        one_column = reduce(vcat, chunk[key] for chunk in chunks)
        push!(tmp_result_arr, one_column)
    end

    NamedTuple{column_names}(tmp_result_arr)
end
