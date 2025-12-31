#include "meta_reader.h"

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
}


bool MetaReader::load(const char* path)
{
    FILE* f = std::fopen(path, "rb");
    if (!f)
    {
        return false;
    }


    MetaHeader header;
    if(std::fread(&header, sizeof(header), 1, f) != 1)
    {
        std::fclose(f);
        return false;
    }


    if (std::memcmp(header.magic, "META", 4) != 0)          //reject if not a meta file
    {
        std::fclose(f);
        return false;
    }


    meta_.column_count_ = header.column_count;
    meta_.row_count_    = header.row_count;
    meta_.columns.clear();
    meta_.columns.reserve(header.column_count);

    for (uint32_t i = 0; i < header.column_count; ++i)
    {
        ColumnMetaDisk disk;
        std::fread(&disk, sizeof(disk), 1, f);
        
        ColumnMeta col;
        col.type     = static_cast<ColumnType>(disk.type);
        col.nullable = disk.nullable != 0;
        col.column_id = disk.column_id;

        meta_columns.push_back(col);
    }

    std::fclose(f);
    return true;

}