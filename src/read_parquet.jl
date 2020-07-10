using Base.Threads: @spawn
using Base.Iterators: drop
using ProgressMeter: @showprogress, Progress, next!
using NamedTupleTools: namedtuple

read_parquet(path, cols::Vector{Symbol}; kwargs...) = read_parquet(path, String.(cols); kwargs...)

read_parquet(path; kwargs...) = read_parquet(path, String[]; kwargs...)

function read_parquet(path, cols::Vector{String}; multithreaded=true, verbose = false)
	"""function for reading parquet"""

    par = ParFile(path)
	nc = ncols(par)

    colnames = [sch.name for sch in  drop(par.schema.schema, 1)]

    close(par)

	if length(cols) == 0
		colnums = collect(1:nc)
	else
		colnums = [findfirst(==(c), colnames) for c in cols]
	end

	results = Vector{Any}(undef, length(colnums))

    filemetadata = metadata(path)

    symbol_col_names = collect(Symbol(col) for col in colnames[colnums])

    p = Progress(length(colnums))
    if multithreaded
		for (i, j) in enumerate(colnums)
            results[i] = @spawn begin
                # next!(p)
                res = read_column(path, filemetadata, j)
                res
            end
        end
        results = fetch.(results)
    else

		for (i, j) in enumerate(colnums)
            results[i] = read_column(path, filemetadata, j)
            next!(p)
		end
	end


    return namedtuple(symbol_col_names, results)
end
