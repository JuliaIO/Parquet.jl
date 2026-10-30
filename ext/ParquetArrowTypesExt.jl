module ParquetArrowTypesExt

using Parquet: JSONValue, BSONValue
import ArrowTypes

# Arrow.write stores a JSON column as the canonical `arrow.json` extension over its text
# (Arrow reads it back as strings), and a BSON column as plain binary.
ArrowTypes.ArrowType(::Type{JSONValue}) = String
ArrowTypes.toarrow(value::JSONValue) = String(value.bytes)
ArrowTypes.arrowname(::Type{JSONValue}) = Symbol("arrow.json")

ArrowTypes.ArrowType(::Type{BSONValue}) = Vector{UInt8}
ArrowTypes.toarrow(value::BSONValue) = Vector{UInt8}(value.bytes)

end
