using Base.Threads: @spawn
using Base.Iterators: drop
using ProgressMeter: @showprogress
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

	if multithreaded
		for (i, j) in enumerate(colnums)
			results[i] = @spawn read_column(path, filemetadata, j)
		end
	else
		@showprogress for (i, j) in enumerate(colnums)
			results[i] = read_column(path, filemetadata, j)
		end
	end

	symbol_col_names = collect(Symbol(col) for col in colnames[colnums])

    if multithreaded
        @showprogress for i in 1:length(results)
            results[i] = fetch(results[i])
        end
    end

    return namedtuple(symbol_col_names, results)
end
