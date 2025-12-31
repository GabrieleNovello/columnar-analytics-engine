#pragma once

#include <cstdint>
#include <vector>
#include <string>

#include "csv_reader.h"
#include "schema.h"



struct ParseError
{
    uint64_t    row_;
    uint32_t    column_;
    std::string token_;
};

enum class LoadStatus
{
    OK,
    PARSE_ERROR
};

struct ColumnStorage
{
    ColumnType type_;
    std::vector<int64_t> i64_;
    std::vector<double>  f64_;
    std::vector<uint8_t> nulls_;
};


class ColumnLoader
{
    public:
        explicit ColumnLoader(const Schema& schema);
        
        LoadStatus load(CSVReader& reader);

        uint64_t row_count() const;
        const ParseError& error() const;

        const std::vector<ColumnStorage>& columns() const;
    
    private:
        const Schema& schema_;
        std::vector<ColumnStorage> columns_;

        uint64_t   row_count_;
        ParseError error_;

        bool parse_int64(const char* token, int64_t& out);
        bool parse_double(const char* token, double& out);
};