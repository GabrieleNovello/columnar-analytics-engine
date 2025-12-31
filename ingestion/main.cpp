#include <iostream>

#include "schema.h"
#include "csv_reader.h"
#include "column_loader.h"
#include "column_writer.h"


int main(int argc, char** argv)
{
    if (argc != 3)
    {
        std::cerr << "Usage: ingest <csv_path> <output_dir>\n";
        return 1;
    }

    const char* csv_path = argv[1];
    const char* out_dir  = argv[2];


    // Stage 1 pipeline testing (hardcoded)

    // 1 : Define schema

    Schema schema;
    schema.columns_.push_back({"id",    ColumnType::INT64,  false});
    schema.columns_.push_back({"value", ColumnType::DOUBLE, true});

    // 2 : Open CSV
    CSVReader reader(csv_path);

    // 3 : Load columns
    ColumnLoader loader(schema);
    LoadStatus status = loader.load(reader);

    if (status != LoadStatus::OK)
    {
        const ParseError& err = loader.error();

        std::cerr << "Parse error at row " << err.row_
                  << ", column " << err.column_
                  << ", token \"" << err.token_ << "\"\n";
        return 1;
    }

    // 4 : Write columns to disk
    ColumnWriter writer(out_dir);

    if (!writer.write_all(schema, loader.columns(), loader.row_count()))
    {
        std::cerr << "Failed to write column files\n";
        return 1;
    }

    std::cout << "Ingestion complete. Rows loaded: "
              << loader.row_count() << "\n";

    return 0;

}