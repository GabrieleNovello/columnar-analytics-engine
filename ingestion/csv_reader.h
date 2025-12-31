#pragma once

#include <cstdio>
#include <cstdint>


struct CSVRow
{
    const char** fields_;
    uint32_t     count_;
};


class CSVReader
{
    public:
        bool next_row(CSVRow& row);

        explicit CSVReader(const char* path);
        ~CSVReader();
    
    private:
        static constexpr size_t BUFFER_SIZE = 16 * 1024;

        FILE* file_;
        char buffer_[BUFFER_SIZE];

        bool tokenize(char *line, CSVRow&row);
};

