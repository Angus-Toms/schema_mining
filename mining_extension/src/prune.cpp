#include "duckdb.hpp"

#include <vector>
#include <functional>
#include <iostream>

/* 
Calculate entropies from a distribution of and prune uniform distributions
*/

namespace prune {

void pruneFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    // Input: single tuple consisting of map from custom_sum
    // keys: INTEGER[] (attribute set)
    // values: INTEGER[] (dist)

    int rows = args.size();
    auto& map = args.data[0];
    auto pairs = duckdb::MapValue::GetChildren(map.GetValue(0));

    duckdb::vector<duckdb::Value> keys;
    duckdb::vector<duckdb::Value> entropies;

    // Find N from first dist to filter out uniform distributions
    int N = 0;
    auto firstPair = duckdb::MapValue::GetChildren(pairs[0]);
    for (const auto& val : duckdb::ListValue::GetChildren(firstPair[1])) {
        N += val.GetValue<int>();
    }

    for (const auto& kv : pairs) {
        auto pair = duckdb::MapValue::GetChildren(kv);
        if (duckdb::ListValue::GetChildren(pair[1]).size() == N) {
            // Uniform distribution, prune
            continue;
        }

        // Non-uniform distribution, get entropy
        double entropy = 0.0;
        for (const auto& val : duckdb::ListValue::GetChildren(pair[1])) {
            double count = val.GetValue<double>();
            if (count > 1) {
                entropy += count * std::log2(count);
            }
        }
        // H(A) = log2(N) - 1/N (SUM count(a) * log2(count(a)))
        entropy = std::log2(N) - (1.0 / N * entropy);

        // Save pair to result map
        keys.push_back(pair[0]);
        entropies.push_back(duckdb::Value::DOUBLE(entropy));
    }

    duckdb::Value resultMap = duckdb::Value::MAP(
        duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), // key: attr set
        duckdb::LogicalType::DOUBLE, // value: entropy
        keys,
        entropies
    );
    result.SetValue(0, resultMap);
}

} // namespace prune