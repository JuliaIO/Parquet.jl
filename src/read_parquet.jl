using Base.Threads: @spawn
using Base.Iterators: drop
using ProgressMeter: @showprogress
using NamedTupleTools: namedtuple

read_parquet(path, cols::Vector{Symbol}; kwargs...) = read_parquet(path, String.(cols); kwargs...)

read_parquet(path; kwargs...) = read_parquet(path, String[]; kwargs...)

function read_parquet(path, cols::Vector{String}; multithreaded=true, verbose = false)
	"""function for reading parquet"""

	if multithreaded
		# use a bounded channel to limit
	    c1 = Channel{Bool}(Threads.nthreads())
	    atexit(()->close(c1))
	end

	nc = ncols(ParFile(path))

	colnames = [sch.name for sch in  drop(ParFile(path).schema.schema, 1)]

	if length(cols) == 0
		colnums = collect(1:nc)
	else
		colnums = [findfirst(==(c), colnames) for c in cols]
	end

	results = Vector{Any}(undef, length(colnums))

	filemetadata = metadata(path)

	if multithreaded
		@showprogress for (i, j) in enumerate(colnums)
			put!(c1, true)
			results[i] = @spawn begin
				res = read_column(path, filemetadata, j)
				take!(c1)
				res
			end
		end
	else
		@showprogress for (i, j) in enumerate(colnums)
			results[i] = read_column(path, filemetadata, j)
		end
	end

	symbol_col_names = collect(Symbol(col) for col in colnames[colnums])

	if multithreaded
		fnl_results = collect(fetch(result) for result in results)
		return namedtuple(symbol_col_names, fnl_results)
	else
		return namedtuple(symbol_col_names, results)
	end
end
