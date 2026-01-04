#pragma once

#include "column_reader.h"
#include "batch.h"

class ColumnScan
{
    public: 
        explicit ColumnScan(ColumnReader& reader);

        bool next_i64(Batch<int64_t>& batch);
        bool next_f64(Batch<double>& batch);

    private:
        ColumnReader& reader_;
        uint64_t rows_emitted_ = 0;
};