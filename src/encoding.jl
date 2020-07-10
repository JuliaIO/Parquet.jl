# obtain the encoding of the page
using Thrift: isfilled

function page_encoding(page::Page)
    if isfilled(page.hdr, :data_page_header)
        return page.hdr.data_page_header.encoding
    elseif isfilled(page.hdr, :data_page_header_v2)
        return page.hdr.data_page_header_v2.encoding
    elseif isfilled(page.hdr, :dictionary_page_header)
        return page.hdr.dictionary_page_header.encoding
    else
        error("not supported page")
    end
end
