#include "duckdb.hpp"

#include <iostream>

namespace filt {

void filtFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    auto rowCount = args.size();
    auto searchAtt = args.data[0]; // Value we're searching for in the set of valid hashes
    auto validAtts = args.data[1]; // LIST(LIST(UBIGINT)). List of non-unique atts for each set
    auto validAttList = duckdb::ListValue::GetChildren(validAtts.GetValue(0));
    auto setOffset = args.data[2].GetValue(0).GetValue<int>();

    for (size_t row = 0; row < rowCount; ++row) {
        auto searchVal = searchAtt.GetValue(row);
        auto validVals = duckdb::ListValue::GetChildren(validAttList[setOffset]);
        bool found = std::find(validVals.begin(), validVals.end(), searchVal) != validVals.end();
        result.SetValue(row, duckdb::Value::BOOLEAN(found));
    }
}

} // namespace filt