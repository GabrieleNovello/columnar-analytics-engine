#include "execution/predicate_filter.h"

PredicateFilter::PredicateFilter(double threshold)
    : threshold_(threshold)
{
}

void PredicateFilter::set_threshold(double threshold)
{
    threshold_ = threshold;
}

void PredicateFilter::apply_i64(const Batch<int64_t>& in, Batch<int64_t>& out) const
{
    uint32_t vin  = 0; // index into dense input values
    uint32_t vout = 0; // index into dense output values
    uint32_t rout = 0; // output row count (and index into out.nulls_)

    for (uint32_t i = 0; i < in.count_; ++i)
    {
        if (in.nulls_[i] != 0)
        {
            continue; // drop NULL
        }

        const int64_t v = in.values_[vin];
        ++vin;

        if (static_cast<double>(v) > threshold_)
        {
            out.values_[vout] = v;
            ++vout;

            out.nulls_[rout] = 0;
            ++rout;
        }
    }

    out.count_ = rout;
}

void PredicateFilter::apply_f64(const Batch<double>& in, Batch<double>& out) const
{
    uint32_t vin  = 0;
    uint32_t vout = 0;
    uint32_t rout = 0;

    for (uint32_t i = 0; i < in.count_; ++i)
    {
        if (in.nulls_[i] != 0)
        {
            continue; // drop NULL
        }

        const double v = in.values_[vin];
        ++vin;

        if (v > threshold_)
        {
            out.values_[vout] = v;
            ++vout;

            out.nulls_[rout] = 0;
            ++rout;
        }
    }

    out.count_ = rout;
}
