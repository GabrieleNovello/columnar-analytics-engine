#pragma once

#include "ingestion/schema.h"
#include <cstdint>


class MetaWriter
{
    public:
        bool write(const char* path,
                   const Schema& schema,
                   uint64_t row_count);
};