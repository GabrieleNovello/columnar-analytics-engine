#pragma once

#include <cstdint>

#include "execution/batch.h"

class PredicateFilter
{
public:
    explicit PredicateFilter(double threshold);

    void set_threshold(double threshold);

    // Filters keep rows where value > threshold, and drop NULLs
    // Input batch uses: 
        // - nulls_[i] for row i
        // - values_ is dense for non-nulls.
    // Output batch uses same format

    void apply_i64(const Batch<int64_t>& in, Batch<int64_t>& out) const;
    void apply_f64(const Batch<double>&  in, Batch<double>&  out) const;

private:
    double threshold_;
};
