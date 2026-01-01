#include "meta_writer.h"

#include <cstdio>
#include <cstring>


struct MetaHeader
{
    char     magic_[4];
    uint32_t column_count_;
    uint64_t row_count_;
};


struct ColumnMetaDisk
{
    uint8_t  type_;
    uint8_t  nullable_;
    uint16_t pad_;
    uint32_t column_id_;
};


bool MetaWriter::write(const char* path,
                       const Schema& schema,
                       uint64_t row_count)
{
    FILE* f = std::fopen(path, "wb");
    if (!f)
    {
        return false;
    }


    MetaHeader header;
    std::memcpy(header.magic_, "META", 4);
    header.column_count_ = static_cast<uint32_t>(schema.columns_.size());
    header.row_count_    = row_count;

    std::fwrite(&header, sizeof(header), 1, f);

    for (uint32_t i = 0; i < header.column_count_; ++i)
    {
        const ColumnDef& col = schema.columns_[i];

        ColumnMetaDisk meta;
        meta.type_       = static_cast<uint8_t>(col.type_);
        meta.nullable_   = col.nullable_ ? 1 : 0;
        meta.pad_        = 0;
        meta.column_id_  = i;

        std::fwrite(&meta, sizeof(meta), 1, f);
    }

    std::fclose(f);
    return true;
}