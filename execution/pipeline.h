#pragma once

#include <cstdint>

#include "execution/column_reader.h"
#include "execution/column_scan.h"
#include "execution/predicate_filter.h"
#include "execution/aggregate.h"
#include "execution/batch.h"

class Pipeline
{
public:
    Pipeline(ColumnReader& reader, double threshold);

    void run();

    uint64_t count() const;
    double   sum() const;

private:
    ColumnReader&   reader_;
    ColumnScan      scan_;
    PredicateFilter filter_;
    CountAggregate  count_;
    SumAggregate    sum_;
};