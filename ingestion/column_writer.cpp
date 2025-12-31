
#include "column_writer.h"

#include <cstdio>
#include <cstring>


ColumnWriter::ColumnWriter(const char* out_dir) : out_dir_(out_dir)
{
}


bool ColumnWriter::write_all(const Schema& schema,
                             const std::vector<ColumnStorage>& columns,
                             uint64_t row_count)
{
    for (uint32_t i = 0; i < columns.size(); ++i)
    {
        if (!write_column(i, schema.columns_[i], columns[i], row_count))
        {
            return false;
        }
    }
    return true;
}


bool ColumnWriter::write_column(uint32_t idx,
                                const ColumnDef& def,
                                const ColumnStorage& col,
                                uint64_t row_count)
{
    char path[256];
    std::snprintf(path, sizeof(path), "%s/col_%u.col", out_dir_, idx);

    FILE* f = std::fopen(path, "wb");
    if (!f)
    {
        return false;
    }

    ColumnFileHeader header;
    std::memcpy(header.magic, "COL1", 4);
    header.version   = 1;
    header.type      = static_cast<uint8_t>(def.type_);
    header.row_count = row_count;

    std::fwrite(&header, sizeof(header), 1, f);

    if (def.type_ == ColumnType::INT64)
    {
        size_t v = 0;
        for (uint64_t i = 0; i < row_count; ++i)
        {
            if (col.nulls_[i] == 0)
            {
                std::fwrite(&col.i64_[v], sizeof(int64_t), 1, f);
                ++v;
            }
        }
    }
    else
    {
        size_t v = 0;
        for (uint64_t i = 0; i < row_count; ++i)
        {
            if (col.nulls_[i] == 0)
            {
                std::fwrite(&col.f64_[v], sizeof(double), 1, f);
                ++v;
            }
        }
    }

    std::fwrite(col.nulls_.data(), 1, row_count, f);

    std::fclose(f);
    return true;
}