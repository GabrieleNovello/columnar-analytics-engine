#pragma once

#include <cstdint>

static constexpr uint32_t BATCH_SIZE = 1024;

template <typename T>
struct Batch
{
    T* values_;
    uint8_t* nulls_;
    uint32_t count_;
};