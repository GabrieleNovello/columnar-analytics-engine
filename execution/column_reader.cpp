#include "column_reader.h"

#include <cstring>

struct ColumnFileHeader
{
    char     magic[4];
    uint32_t version;
    uint8_t  type;
    uint64_t row_count;
};

ColumnReader::ColumnReader(const char* path)
    : file_(nullptr),
      type_(ColumnType::INT64),
      row_count_(0)
{
    file_ = std::fopen(path, "rb");
}

ColumnReader::~ColumnReader()
{
    close();
}

bool ColumnReader::open()
{
    if (!file_)
    {
        return false;
    }

    ColumnFileHeader header;
    if (std::fread(&header, sizeof(header), 1, file_) != 1)
    {
        return false;
    }

    if (std::memcmp(header.magic, "COL1", 4) != 0)
    {
        return false;
    }

    type_      = static_cast<ColumnType>(header.type);
    row_count_ = header.row_count;

    // ---- LOAD NULL BITMAP (END OF FILE) ----
    nulls_.resize(row_count_);

    long values_size = 0;
    if (type_ == ColumnType::INT64)
    {
        values_size = sizeof(int64_t) * row_count_;
    }
    else
    {
        values_size = sizeof(double) * row_count_;
    }

    std::fseek(file_, sizeof(ColumnFileHeader) + values_size, SEEK_SET);
    std::fread(nulls_.data(), 1, row_count_, file_);

    // rewind to start of values
    std::fseek(file_, sizeof(ColumnFileHeader), SEEK_SET);

    return true;
}

void ColumnReader::close()
{
    if (file_)
    {
        std::fclose(file_);
        file_ = nullptr;
    }
}

ColumnType ColumnReader::type() const
{
    return type_;
}

uint64_t ColumnReader::row_count() const
{
    return row_count_;
}

bool ColumnReader::read_i64(int64_t* out, uint32_t count)
{
    return std::fread(out, sizeof(int64_t), count, file_) == count;
}

bool ColumnReader::read_f64(double* out, uint32_t count)
{
    return std::fread(out, sizeof(double), count, file_) == count;
}

const uint8_t* ColumnReader::nulls() const
{
    return nulls_.data();
}
