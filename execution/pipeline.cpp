#include "execution/pipeline.h"

Pipeline::Pipeline(ColumnReader& reader, double threshold)
    : reader_(reader),
      scan_(reader),
      filter_(threshold),
      count_(),
      sum_()
{
}

void Pipeline::run()
{
    if (reader_.type() == ColumnType::INT64)
    {
        int64_t in_values[BATCH_SIZE];
        uint8_t in_nulls[BATCH_SIZE];
        Batch<int64_t> in_batch { in_values, in_nulls, 0 };

        int64_t out_values[BATCH_SIZE];
        uint8_t out_nulls[BATCH_SIZE];
        Batch<int64_t> out_batch { out_values, out_nulls, 0 };

        while (scan_.next_i64(in_batch))
        {
            filter_.apply_i64(in_batch, out_batch);
            count_.consume(out_batch);
            sum_.consume(out_batch);
        }
    }
    else
    {
        double  in_values[BATCH_SIZE];
        uint8_t in_nulls[BATCH_SIZE];
        Batch<double> in_batch { in_values, in_nulls, 0 };

        double  out_values[BATCH_SIZE];
        uint8_t out_nulls[BATCH_SIZE];
        Batch<double> out_batch { out_values, out_nulls, 0 };

        while (scan_.next_f64(in_batch))
        {
            filter_.apply_f64(in_batch, out_batch);
            count_.consume(out_batch);
            sum_.consume(out_batch);
        }
    }
}

uint64_t Pipeline::count() const
{
    return count_.result();
}

double Pipeline::sum() const
{
    return sum_.result();
}
