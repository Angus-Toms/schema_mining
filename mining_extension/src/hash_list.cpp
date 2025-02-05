#include "duckdb.hpp"

#include <vector>
#include <string>
#include <iostream>

namespace hashList {

using hash_t = uint64_t;

hash_t combineHashes(hash_t a, hash_t b) {
    return a ^ (b + 0x9e3779b9 + (a << 6) + (a >> 2));
}

void hashListFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    auto rowCount = args.size();
    auto& atts = args.data[0]; // All attributes wrapped in list

    for (size_t row = 0; row < rowCount; ++row) {
        auto tuple = duckdb::ListValue::GetChildren(atts.GetValue(row));
        hash_t hash = 0;
        for (const auto& attr : tuple) {
            hash = combineHashes(hash, attr.Hash());
        }
        result.SetValue(row, duckdb::Value::UBIGINT(hash));
    }
}

} // namespace hashList