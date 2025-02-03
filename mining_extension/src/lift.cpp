#include "duckdb.hpp"

#include <vector>
#include <string>
#include <functional>
#include <iostream>

namespace lift {

using hash_t = uint64_t;

// TODO: This can be improved if we use an XOR-based hash combiner and 
// maintain a single running hash. 
hash_t combineHashes(hash_t a, hash_t b) {
    return a ^ (b + 0x9e3779b9 + (a << 6) + (a >> 2));
}

duckdb::vector<duckdb::Value> getIndexCombinations(const std::vector<int>& indeces) {
    duckdb::vector<duckdb::Value> result;
    std::vector<int> stack;

    std::function<void(int)> generateCombinations = [&](int start) {
        for (int i = start; i < indeces.size(); i++) {
            stack.push_back(indeces[i]);
            duckdb::vector<duckdb::Value> currCombination;
            for (const auto& idx : stack) {
                currCombination.push_back(duckdb::Value::INTEGER(idx));
            }
            result.push_back(duckdb::Value::LIST(currCombination));
            generateCombinations(i + 1);
            stack.pop_back();
        }
    };
    generateCombinations(0);
    return result;
}

duckdb::vector<duckdb::Value> getHashCombinations(const duckdb::vector<duckdb::Value>& values) {
    duckdb::vector<duckdb::Value> result;
    std::vector<hash_t> stack;

    std::function<void(int)> generateCombinations = [&](int start) {
        for (int i = start; i < values.size(); i++) {
            stack.push_back(values[i].Hash());
            hash_t hash = 0;
            for (const auto& h : stack) {
                hash = combineHashes(hash, h);
            }
            result.push_back(duckdb::Value::UBIGINT(hash));
            generateCombinations(i + 1);
            stack.pop_back();
        }
    };
    generateCombinations(0);
    return result;
}

std::vector<std::string> getStringCombinations(const duckdb::vector<duckdb::Value>& values) {
    std::vector<std::string> result;
    std::vector<std::string> stack;

    std::function<void(int)> generateCombinations = [&](int start) {
        for (int i = start; i < values.size(); i++) {
            stack.push_back(values[i].ToString());
            result.push_back("");
            for (const auto& str : stack) {
                result.back() += str;
            }
            generateCombinations(i + 1);
            stack.pop_back();
        }
    };

    generateCombinations(0);
    return result;
}

void liftFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    auto count = args.size();
    auto& atts = args.data[0]; // All cols wrapped in list
    int attrCount = duckdb::ListValue::GetChildren(atts.GetValue(0)).size();
    duckdb::ListVector::Reserve(result, count);

    // Get attribute indeces 
    std::vector<int> indeces(attrCount);
    for (int i = 0; i < attrCount; i++) {
        std::string attrName = state.child_states[0]->child_states[i]->expr.GetName();
        indeces[i] = std::stoi(attrName.substr(3));
    }

    // Generate result keys (attr combinations) - same for all tuples
    duckdb::vector<duckdb::Value> keys = getIndexCombinations(indeces);

    for (int i = 0; i < count; i++) {
        auto tuple = duckdb::ListValue::GetChildren(atts.GetValue(i));
        duckdb::vector<duckdb::Value> values = getHashCombinations(tuple);
        duckdb::Value tupleMap = duckdb::Value::MAP(
            duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), // key type 
            duckdb::LogicalType::UBIGINT, // value type
            keys,
            values
        );
        result.SetValue(i, tupleMap);
    }
}

} // namespace lift