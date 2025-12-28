#include "column_loader.h"

#include <cstdlib>
#include <cerrno>
#include <cstring>


ColumnLoader::ColumnLoader(const Schema& schema) : schema_(schema), row_count_(0)
{
    columns_.reserve(schema_.columns_.size());

    for (const auto& col : schema_.columns_)
    {
        ColumnStorage storage;
        storage.type_ = col.type_;
        columns_.push_back(std::move(storage));
    }
}


LoadStatus ColumnLoader::load(CSVReader& reader)
{
    CSVRow row;
    
    while (reader.next_row(row))
    {
        if (row.count_ != schema_.columns_.size())
        {
            error_ = { row_count_, 0, "column count mismatch"};
            return LoadStatus::PARSE_ERROR;
        }

        for (uint32_t i = 0; i < row.count_; ++i)
        {
            const auto& col_def = schema_.columns_[i];
            auto& col_storage   = columns_[i];
            const char* token   = row.fields_[i];

            bool is_null = (token[0] == '\0');

            if (is_null)
            {
                if (!col_def.nullable_)
                {
                    error_ = { row_count_, i, token};
                    return LoadStatus::PARSE_ERROR;
                }
                col_storage.nulls_.push_back(1);
                continue;
            }

            col_storage.nulls_.push_back(0);

            if (col_storage.type_ == ColumnType::INT64)
            {
                int64_t v;
                if (!parse_int64(token, v))
                {
                    error_ = {row_count_, i, token };
                    return LoadStatus::PARSE_ERROR;
                }
                
                col_storage.i64_.push_back(v);
            }
            else
            {
                double v;
                if (!parse_double(token, v))
                {
                    error_ = { row_count_, i, token};
                    return LoadStatus::PARSE_ERROR;
                }

                col_storage.f64_.push_back(v);
            }
        }
        
        ++row_count_;

    }

    return LoadStatus::OK;
}


uint64_t ColumnLoader::row_count() const
{
    return row_count_;
}


const ParseError& ColumnLoader::error() const
{
    return error_;
}


const std::vector<ColumnStorage>& ColumnLoader::columns() const
{
    return columns_;
}


bool ColumnLoader::parse_int64(const char* token, int64_t& out)
{
    errno = 0;
    char* end = nullptr;

    long long v = std::strtoll(token, &end, 10);

    if (errno != 0 || *end != '\0')
    {
        return false;
    }

    out = static_cast<int64_t>(v);
    return true;
}


bool ColumnLoader::parse_double(const char* token, double& out)
{
    errno = 0;
    char* end = nullptr;

    double v = std::strtod(token, &end);

    if (errno != 0 || *end != '\0')
    {
        return false;
    }

    out = v;
    return true;
}
