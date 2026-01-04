#include "column_scan.h"

#include <cstring>

ColumnScan::ColumnScan(ColumnReader& reader)
    : reader_(reader),
      rows_emitted_(0)
{
}

bool ColumnScan::next_i64(Batch<int64_t>& batch)
{
    if (reader_.type() != ColumnType::INT64)
    {
        return false;
    }

    uint64_t remaining = reader_.row_count() - rows_emitted_;
    if (remaining == 0)
    {
        return false;
    }

    uint32_t emit =
        remaining > BATCH_SIZE ? BATCH_SIZE
                               : static_cast<uint32_t>(remaining);

    // ---- get nulls from memory (NOT file) ----
    std::memcpy(batch.nulls_,
                reader_.nulls() + rows_emitted_,
                emit);

    // count non-nulls
    uint32_t value_count = 0;
    for (uint32_t i = 0; i < emit; ++i)
    {
        if (batch.nulls_[i] == 0)
        {
            ++value_count;
        }
    }

    // read dense values sequentially
    if (!reader_.read_i64(batch.values_, value_count))
    {
        return false;
    }

    batch.count_ = emit;
    rows_emitted_ += emit;
    return true;
}

bool ColumnScan::next_f64(Batch<double>& batch)
{
    if (reader_.type() != ColumnType::DOUBLE)
    {
        return false;
    }

    uint64_t remaining = reader_.row_count() - rows_emitted_;
    if (remaining == 0)
    {
        return false;
    }

    uint32_t emit =
        remaining > BATCH_SIZE ? BATCH_SIZE
                               : static_cast<uint32_t>(remaining);

    std::memcpy(batch.nulls_,
                reader_.nulls() + rows_emitted_,
                emit);

    uint32_t value_count = 0;
    for (uint32_t i = 0; i < emit; ++i)
    {
        if (batch.nulls_[i] == 0)
        {
            ++value_count;
        }
    }

    if (!reader_.read_f64(batch.values_, value_count))
    {
        return false;
    }

    batch.count_ = emit;
    rows_emitted_ += emit;
    return true;
}
