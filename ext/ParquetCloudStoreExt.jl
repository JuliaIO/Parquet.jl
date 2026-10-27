module ParquetCloudStoreExt

import CloudStore, Parquet

# A CloudStore object read by ranged requests. CloudStore checks each range against the
# object's ETag, so a read of an object replaced mid-read fails instead of mixing versions.
struct ObjectSource <: Parquet.AbstractSource
    object::CloudStore.Object
end

Parquet.source(object::CloudStore.Object; budget=nothing) = ObjectSource(object)
Parquet._sourcepath(object::CloudStore.Object) = object.key
Parquet.sourcelength(src::ObjectSource) = Int64(length(src.object))
Parquet.concurrentreads(::ObjectSource) = true

function Parquet.readrange(src::ObjectSource, offset::Integer, count::Integer)
    bytes = Vector{UInt8}(undef, count)
    count > 0 && copyto!(bytes, 1, src.object, offset + 1, count)
    return bytes
end

# The objects under `prefix`, without directory markers and with names below it that
# start with `.` or `_` skipped, in key order.
function Parquet.Dataset(store::CloudStore.API.AbstractStore, prefix::AbstractString;
        credentials=nothing, kw...)
    prefix = String(prefix)
    objects = CloudStore.Object[]
    for object in CloudStore.list(store; prefix=prefix, credentials=credentials)
        endswith(object.key, '/') && continue
        relative = object.key[(ncodeunits(prefix) + 1):end]
        any(Parquet._hiddenname, split(relative, '/'; keepempty=false)) && continue
        push!(objects, object)
    end
    sort!(objects; by=object -> object.key)
    isempty(objects) && throw(ArgumentError("no objects found under $(repr(prefix))"))
    return Parquet._dataset(collect(Any, objects),
        Union{Nothing,String}[object.key for object in objects], prefix; kw...)
end

end
