#include <iostream>
#include <cstdlib>
#include <cerrno>

#include "metadata/meta_reader.h"
#include "execution/column_reader.h"
#include "execution/pipeline.h"

static bool parse_double(const char* s, double& out)
{
    errno = 0;
    char* end = nullptr;
    double v = std::strtod(s, &end);
    if (errno != 0 || end == s || *end != '\0')
    {
        return false;
    }
    out = v;
    return true;
}

int main(int argc, char** argv)
{
    if (argc != 4)
    {
        std::cerr << "Usage: exec <meta_file> <column_file> <threshold>\n";
        return 1;
    }

    double threshold = 0.0;
    if (!parse_double(argv[3], threshold))
    {
        std::cerr << "Invalid threshold\n";
        return 1;
    }

    MetaReader meta;
    if (!meta.load(argv[1]))
    {
        std::cerr << "Failed to load meta\n";
        return 1;
    }

    ColumnReader reader(argv[2]);
    if (!reader.open())
    {
        std::cerr << "Failed to open column file\n";
        return 1;
    }

    Pipeline pipeline(reader, threshold);
    pipeline.run();

    std::cout << "COUNT = " << pipeline.count() << "\n";
    std::cout << "SUM   = " << pipeline.sum() << "\n";
    return 0;
}
