#include "meta_reader.h"

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
    uint32_t column_id;
};


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


    if (std::memcmp(header.magic_, "META", 4) != 0)          //reject if not a meta file
    {
        std::fclose(f);
        return false;
    }


    meta_.column_count_ = header.column_count_;
    meta_.row_count_    = header.row_count_;
    meta_.columns_.clear();
    meta_.columns_.reserve(header.column_count_);

    for (uint32_t i = 0; i < header.column_count_; ++i)
    {
        ColumnMetaDisk disk;
        std::fread(&disk, sizeof(disk), 1, f);
        
        ColumnMeta col;
        col.type_     = static_cast<ColumnType>(disk.type_);
        col.nullable_ = disk.nullable_ != 0;
        col.column_id_ = disk.column_id;

        meta_.columns_.push_back(col);
    }

    std::fclose(f);
    return true;

}


const TableMeta& MetaReader::meta() const
{
    return meta_;
}