#include "duckdb.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

#include <map>
#include <vector>
#include <cmath>
#include <string>
#include <iostream>
#include <functional>

namespace getEntropy {

std::vector<std::string> getCombinationIndex(const int n) {
    std::vector<std::string> result;
    std::vector<std::string> stack;

    std::function<void(int)> generateCombinations = [&](int start) {
        for (int i = start; i < n; i++) {
            stack.push_back(std::to_string(i));
            std::string index = "{";
            for (const auto& str : stack) {
                index += str + ", ";
            }
            index.pop_back();
            index.pop_back();
            index += "}";
            result.push_back(index);

            generateCombinations(i + 1);
            stack.pop_back();
        }
    };

    generateCombinations(0);
    return result;
}

void getEntropyFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    // H(A) = log2(N) - 1/N (SUM count(a) * log2(count(a)))
    // N: tuple count
    auto rows = args.size();
    auto& inData = args.data[0]; // All maps wrapped in list 
    auto maps = duckdb::ListValue::GetChildren(inData.GetValue(0));
    auto count = maps.size();

    // Make keys for result map
    int attributeCount = int(std::log2(count + 1));
    std::vector<std::string> indexCombinations = getCombinationIndex(attributeCount);

    duckdb::vector<duckdb::Value> keys(count);
    duckdb::vector<duckdb::Value> entropies(count);

    // Get N 
    int N = 0;
    for (const auto& entry : duckdb::ListValue::GetChildren(maps[0])) {
        N += duckdb::MapValue::GetChildren(entry)[1].GetValue<int>();
    }
    
    for (int i = 0; i < count; i++) {
        auto map = duckdb::ListValue::GetChildren(maps[i]);
        double entropy = 0.0;
        for (const auto& entry : map) {
            double count = duckdb::MapValue::GetChildren(entry)[1].GetValue<double>();
            if (count > 1) {
                entropy += count * std::log2(count);
            }
        }
        keys[i] = duckdb::Value(indexCombinations[i]);
        entropies[i] = duckdb::Value(std::log2(N) - 1.0/N * entropy);
    }

    duckdb::Value resultMap = duckdb::Value::MAP(
        duckdb::LogicalType::VARCHAR, // key type 
        duckdb::LogicalType::DOUBLE, // value type
        keys,
        entropies
    );
    result.SetValue(0, resultMap);
}

} // namespace getEntropy