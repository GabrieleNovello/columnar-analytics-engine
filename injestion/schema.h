#pragma once

#include<cstdint>
#include <string>
#include <vector>

enum class ColumnType : uint8_t
{
    INT64,
    DOUBLE
};

struct ColumnDef                       
{
    std::string name_;
    ColumnType  type_;
    bool        nullable_;
};

struct Schema
{
    std::vector<ColumnDef> columns_;
};

/* column access
    - columns[n] :          CSV field n / column file n
    - columns[n].name :     validation
    - columns[n].type :     parser + writer
    - columns[n].nullable : empty token validity 
*/