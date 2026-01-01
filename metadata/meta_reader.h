#pragma once

#include <cstdint>
#include <vector>

#include "ingestion/schema.h"


struct ColumnMeta
{
    ColumnType type_;
    bool       nullable_;
    uint32_t   column_id_;
};


struct TableMeta
{
    uint32_t column_count_;
    uint64_t row_count_;
    std::vector<ColumnMeta> columns_;
};


class MetaReader
{
    public:
        bool load(const char* path);
        const TableMeta& meta() const;
    
    private:
        TableMeta meta_;
};