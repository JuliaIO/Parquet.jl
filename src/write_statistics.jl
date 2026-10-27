function _writecolumnorder(element::Metadata.SchemaElement)
    semantics = _leafstatisticsemantics(element)
    if semantics.floating
        return Metadata.ColumnOrder(
            IEEE_754_TOTAL_ORDER=Metadata.IEEE754TotalOrder())
    end
    return Metadata.ColumnOrder(TYPE_ORDER=Metadata.TypeDefinedOrder())
end

function _writecolumnorders(schema::Schema)
    orders = Metadata.ColumnOrder[]
    sizehint!(orders, length(schema.leaves))
    for leaf in schema.leaves
        push!(orders, _writecolumnorder(leaf.element))
    end
    return orders
end
