module ParquetCloudStoreExt

import CloudStore, Parquet

# A CloudStore object read by ranged requests. CloudStore checks each range against the
# object's ETag, so a read of an object replaced mid-read fails instead of mixing versions.
struct ObjectSource <: Parquet.AbstractSource
    object::CloudStore.Object
end

Parquet.source(object::CloudStore.Object; budget=nothing) = ObjectSource(object)
Parquet.sourcelength(src::ObjectSource) = Int64(length(src.object))
Parquet.concurrentreads(::ObjectSource) = true

function Parquet.readrange(src::ObjectSource, offset::Integer, count::Integer)
    bytes = Vector{UInt8}(undef, count)
    count > 0 && copyto!(bytes, 1, src.object, offset + 1, count)
    return bytes
end

end
