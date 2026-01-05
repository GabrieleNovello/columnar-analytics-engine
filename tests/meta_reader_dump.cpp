
#include <iostream>
#include "metadata/meta_reader.h"

int main()
{
    MetaReader reader;

    if (!reader.load("data/test_table/meta.bin"))
    {
        std::cerr << "Failed to load meta.bin\n";
        return 1;
    }

    const TableMeta& meta = reader.meta();

    std::cout << "Columns: " << meta.column_count_ << "\n";
    std::cout << "Rows:    " << meta.row_count_ << "\n";

    for (const auto& col : meta.columns_)
    {
        std::cout << "col_id=" << col.column_id_
                  << "\ntype=" << static_cast<int>(col.type_)
                  << "\nnullable=" << col.nullable_
                  << "\n";
    }

    return 0;

}