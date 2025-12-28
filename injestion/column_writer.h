#pragma once

#include <cstdint>
#include <vector>

#include "schema.h"
#include "column_loader.h"


struct ColumnFileHeader
{
    char     magic[4];
    uint32_t version;
    uint8_t  type;
    uint64_t row_count;
};


class ColumnWriter
{
    public:
        explicit ColumnWriter(const char* out_dir);

        bool write_all(const Schema& schema,
                       const std::vector<ColumnStorage>& columns,
                       uint64_t row_count);
    
    private:
        const char* out_dir_;

        bool write_column(uint32_t idx,
                          const ColumnDef& def,
                          const ColumnStorage& col,
                          uint64_t row_count);
};