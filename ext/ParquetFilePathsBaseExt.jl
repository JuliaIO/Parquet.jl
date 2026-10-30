module ParquetFilePathsBaseExt

import Parquet
using FilePathsBase: SystemPath

# SystemPath denotes local filesystem paths; keep the mmap and atomic-write paths.
Parquet.source(path::SystemPath; kwargs...) = Parquet.source(string(path); kwargs...)
Parquet._sourcepath(path::SystemPath) = string(path)
Parquet._isdataset(path::SystemPath) = Parquet._isdataset(string(path))
Parquet._datasetinputs(path::SystemPath) = Parquet._datasetinputs(string(path))

Parquet.Writer(path::SystemPath; kwargs...) = Parquet.Writer(string(path); kwargs...)
function Parquet.Writer(f::Function, path::SystemPath; kwargs...)
    Parquet.Writer(f, string(path); kwargs...)
    return path
end

function Parquet.write(path::SystemPath, table; kwargs...)
    Parquet.write(string(path), table; kwargs...)
    return path
end
Parquet.write(path::SystemPath; kwargs...) = table -> Parquet.write(path, table; kwargs...)

end
