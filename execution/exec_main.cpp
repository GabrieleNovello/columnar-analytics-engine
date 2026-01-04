#include <iostream>

#include "metadata/meta_reader.h"
#include "execution/column_reader.h"
#include "execution/column_scan.h"
#include "execution/aggregate.h"
#include "execution/batch.h"

int main(int argc, char** argv)
{
    if (argc != 3)
    {
        std::cerr << "Usage: exec <meta_file> <column_file>\n";
        return 1;
    }

    MetaReader meta;
    if (!meta.load(argv[1]))
    {
        std::cerr << "Failed to load meta\n";
        return 1;
    }

    ColumnReader reader(argv[2]);
    if (!reader.open())
    {
        std::cerr << "Failed to open column file\n";
        return 1;
    }

    ColumnScan scan(reader);

    CountAggregate count;
    SumAggregate   sum;

    if (reader.type() == ColumnType::INT64)
    {
        int64_t values[BATCH_SIZE];
        uint8_t nulls[BATCH_SIZE];
        Batch<int64_t> batch { values, nulls, 0 };

        while (scan.next_i64(batch))
        {
            count.consume(batch);
            sum.consume(batch);
        }
    }
    else
    {
        double values[BATCH_SIZE];
        uint8_t nulls[BATCH_SIZE];
        Batch<double> batch { values, nulls, 0 };

        while (scan.next_f64(batch))
        {
            count.consume(batch);
            sum.consume(batch);
        }
    }

    std::cout << "COUNT = " << count.result() << "\n";
    std::cout << "SUM   = " << sum.result() << "\n";

    return 0;
}
