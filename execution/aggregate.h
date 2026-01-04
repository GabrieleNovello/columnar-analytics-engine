#pragma once

#include <cstdint>
#include "batch.h"

class CountAggregate
{
public:
    CountAggregate();

    void consume(const Batch<int64_t>& batch);
    void consume(const Batch<double>& batch);

    uint64_t result() const;

private:
    uint64_t count_;
};

class SumAggregate
{
public:
    SumAggregate();

    void consume(const Batch<int64_t>& batch);
    void consume(const Batch<double>& batch);

    double result() const;

private:
    double sum_;
};
