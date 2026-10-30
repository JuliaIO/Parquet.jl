# Every leaf, floats included, uses the type-defined order, as pyarrow writes: readers
# that predate IEEE_754_TOTAL_ORDER (pyarrow 25 among them) ignore bounds in that order.
_writecolumnorders(schema::Schema) =
    [Metadata.ColumnOrder(TYPE_ORDER=Metadata.TypeDefinedOrder()) for _ in schema.leaves]
