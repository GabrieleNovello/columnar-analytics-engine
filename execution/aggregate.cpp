#include "aggregate.h"

CountAggregate::CountAggregate()
    : count_(0)
{
}

void CountAggregate::consume(const Batch<int64_t>& batch)
{
    for (uint32_t i = 0; i < batch.count_; ++i)
    {
        if (batch.nulls_[i] == 0)
        {
            ++count_;
        }
    }
}

void CountAggregate::consume(const Batch<double>& batch)
{
    for (uint32_t i = 0; i < batch.count_; ++i)
    {
        if (batch.nulls_[i] == 0)
        {
            ++count_;
        }
    }
}

uint64_t CountAggregate::result() const
{
    return count_;
}

// --------------------------

SumAggregate::SumAggregate()
    : sum_(0.0)
{
}

void SumAggregate::consume(const Batch<int64_t>& batch)
{
    uint32_t v = 0;

    for (uint32_t i = 0; i < batch.count_; ++i)
    {
        if (batch.nulls_[i] == 0)
        {
            sum_ += static_cast<double>(batch.values_[v]);
            ++v;
        }
    }
}

void SumAggregate::consume(const Batch<double>& batch)
{
    uint32_t v = 0;

    for (uint32_t i = 0; i < batch.count_; ++i)
    {
        if (batch.nulls_[i] == 0)
        {
            sum_ += batch.values_[v];
            ++v;
        }
    }
}

double SumAggregate::result() const
{
    return sum_;
}
