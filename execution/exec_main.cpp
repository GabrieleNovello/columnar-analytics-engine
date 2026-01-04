#include <iostream>
#include <cstdlib>
#include <cerrno>
#include <cstring>

#include "metadata/meta_reader.h"
#include "execution/column_reader.h"
#include "execution/column_scan.h"
#include "execution/aggregate.h"
#include "execution/batch.h"
#include "execution/predicate_filter.h"

static bool parse_double(const char* s, double& out)
{
    errno = 0;
    char* end = nullptr;
    double v = std::strtod(s, &end);
    if (errno != 0 || end == s || *end != '\0')
    {
        return false;
    }
    out = v;
    return true;
}

int main(int argc, char** argv)
{
    if (argc != 4)
    {
        std::cerr << "Usage: exec <meta_file> <column_file> <threshold>\n";
        return 1;
    }

    double threshold = 0.0;
    if (!parse_double(argv[3], threshold))
    {
        std::cerr << "Invalid threshold\n";
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
    PredicateFilter filter(threshold);

    CountAggregate count;
    SumAggregate   sum;

    if (reader.type() == ColumnType::INT64)
    {
        int64_t in_values[BATCH_SIZE];
        uint8_t in_nulls[BATCH_SIZE];
        Batch<int64_t> in_batch { in_values, in_nulls, 0 };

        int64_t out_values[BATCH_SIZE];
        uint8_t out_nulls[BATCH_SIZE];
        Batch<int64_t> out_batch { out_values, out_nulls, 0 };

        while (scan.next_i64(in_batch))
        {
            filter.apply_i64(in_batch, out_batch);
            count.consume(out_batch);
            sum.consume(out_batch);
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

        while (scan.next_f64(in_batch))
        {
            filter.apply_f64(in_batch, out_batch);
            count.consume(out_batch);
            sum.consume(out_batch);
        }
    }

    std::cout << "COUNT = " << count.result() << "\n";
    std::cout << "SUM   = " << sum.result() << "\n";
    return 0;
}
