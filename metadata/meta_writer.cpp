#include "meta_writer.h"

#include <cstdio>
#include <cstring>


struct MetaHeader
{
    char     magic[4];
    uint32_t column_count;
    uint64_t row_count;
};


struct ColumnMetaDisk
{
    uint8_t  type;
    uint8_t  nullable;
    uint16_t pad;
    uint32_t column_id;
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
    std::memcpy(header.magic, "META", 4);
    header.column_count = static_cast<uint32_t>(schema.columns_.size());
    header.row_count    = row_count;

    std::fwrite(&header, sizeof(header), 1, f);

    for (uint32_t i = 0; i < header.column_count; ++i)
    {
        const ColumnDef& col = schema.columns_[i];

        ColumnMetaDisk meta;
        meta.type       = static_cast<uint8_t>(col.type_);
        meta.nullable   = col.nullable_ ? 1 : 0;
        meta.pad        = 0;
        meta.column_id  = i;

        std::fwrite(&meta, sizeof(meta), 1, f);
    }

    std::fclose(f);
    return true;
}