#include <iostream>

#include "ingestion/schema.h"
#include "ingestion/csv_reader.h"
#include "ingestion/column_loader.h"
#include "ingestion/column_writer.h"
#include "metadata/meta_writer.h"

int main(int argc, char** argv)
{
    if (argc != 3)
    {
        std::cerr << "Usage: ingest <csv_path> <output_dir>\n";
        return 1;
    }

    const char* csv_path = argv[1];
    const char* out_dir  = argv[2];

    // 1. Define schema
    Schema schema;
    schema.columns_.push_back({ "id",    ColumnType::INT64,  false });
    schema.columns_.push_back({ "value", ColumnType::DOUBLE, true  });

    // 2. Open CSV
    CSVReader reader(csv_path);

    // 3. Load columns
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

    // 4. Write column files
    ColumnWriter writer(out_dir);
    if (!writer.write_all(schema, loader.columns(), loader.row_count()))
    {
        std::cerr << "Failed to write column files\n";
        return 1;
    }

    // 5. Write table metadata (FIX)
    MetaWriter meta_writer;
    if (!meta_writer.write("out/table.meta", schema, loader.row_count()))
    {
        std::cerr << "Failed to write table.meta\n";
        return 1;
    }

    std::cout << "Ingestion complete. Rows loaded: "
              << loader.row_count() << "\n";

    return 0;
}
