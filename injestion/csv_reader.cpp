#include "csv_reader.h"

#include <iostream>
#include <cstdlib>
#include <cstring>

CSVReader::CSVReader(const char* path) : file_(nullptr)
{
    file_ = fopen(path, "r");
    if (!file_)
    {
        std::cerr<<"File at path  \"" << path << "\" could not be opened.";         // Will add proper error handlng logic later
        std::abort();
    }
}

CSVReader::~CSVReader()
{
    if(file_)
    {
        fclose(file_);
    }
}

bool CSVReader::next_row(CSVRow& row)
{
    if (!fgets(buffer_, BUFFER_SIZE, file_))
    {
        return false; 
    }

    return tokenize(buffer_, row);
}

bool CSVReader::tokenize(char* line, CSVRow& row)
{
    static constexpr uint32_t MAX_FIELDS = 256;
    static const char* fields[MAX_FIELDS];

    uint32_t count = 0;

    char*token = std::strtok(line, ",\n");
    while (token && count < MAX_FIELDS)
    {
        fields[count++] = token;
        token = std::strtok(nullptr, ",\n");
    }

    row.fields_ = fields;
    row.count_  = count;
    return true;
}