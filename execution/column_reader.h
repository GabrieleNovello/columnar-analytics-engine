#pragma once

#include <cstdio>
#include <cstdint>
#include <vector>

#include "ingestion/schema.h"

struct ColumnFileHeader;

class ColumnReader
{
public:
    explicit ColumnReader(const char* path);
    ~ColumnReader();

    bool open();
    void close();

    ColumnType type() const;
    uint64_t   row_count() const;

    // value streaming
    bool read_i64(int64_t* out, uint32_t count);
    bool read_f64(double* out, uint32_t count);

    // null access (memory)
    const uint8_t* nulls() const;

private:
    FILE*      file_;
    ColumnType type_;
    uint64_t   row_count_;

    std::vector<uint8_t> nulls_;
};
