#include "duckdb.hpp"

#include <vector>
#include <string>
#include <functional>
#include <iostream>
#include <chrono>

namespace lift_exact {

using hash_t = uint64_t;

struct VectorHash {
    hash_t operator()(const std::unordered_set<int>& s) const {
        hash_t seed = 0;
        for (const auto& elem : s) {
            seed ^= std::hash<int>()(elem) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }

    hash_t operator()(const std::set<int>& s) const {
        hash_t seed = 0;
        for (const auto& elem : s) {
            seed ^= std::hash<int>()(elem) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};

struct DuckDBVectorHash {
    hash_t operator()(const duckdb::vector<duckdb::Value>& v) const {
        hash_t seed = 0;
        for (const auto& val : v) {
            seed ^= val.Hash() + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};

class SetPruner {
private:
    int attCount = 0;
    bool setsInitialised = false;
    std::vector<std::set<int>> validSets = {};

public:
    static SetPruner& GetInstance() {
        static SetPruner instance;
        return instance;
    }

    // Disallow copying to enforce singleton
    SetPruner(const SetPruner&) = delete;
    SetPruner& operator=(const SetPruner&) = delete;
    SetPruner() = default;

    void setInitialCombinations(const int attCount) {
        this->attCount = attCount;
        for (int i = 0; i < attCount; i++) {
            validSets.emplace_back(std::set<int>{i});
        }
    }

    void generateCombinations() {
        std::vector<std::set<int>> nextSets;
        size_t n = validSets.empty() ? 0 : validSets[0].size();

        // Use hash set for quick lookups
        std::unordered_set<std::set<int>, VectorHash> validSetLookup(validSets.begin(), validSets.end());

        // Iterate over all pairs of valid sets
        for (size_t i = 0; i < validSets.size(); ++i) {
            // For the first (n-1) elements to be equal, the sets must be within [attCount] of each other in the set vector
            int upperLimit = std::min(i + attCount, validSets.size());
            for (size_t j = i + 1; j < upperLimit; ++j) {
                auto& set1 = validSets[i];
                auto& set2 = validSets[j];

                // Check if first (n-1) elements match (set logic)
                if (std::includes(set1.begin(), std::prev(set1.end()), set2.begin(), std::prev(set2.end())) ||
                    std::includes(set2.begin(), std::prev(set2.end()), set1.begin(), std::prev(set1.end()))) {

                    std::set<int> candidate = set1;
                    candidate.insert(*set2.rbegin());

                    // Check if all size n subsets are valid
                    bool isValid = true;
                    for (const int& elem : candidate) {
                        std::set<int> subset = candidate;
                        subset.erase(elem);
                        if (validSetLookup.find(subset) == validSetLookup.end()) {
                            isValid = false;
                            break;
                        }
                    }
                    if (isValid) {
                        nextSets.push_back(std::move(candidate));
                    }
                }
            }
        }
        validSets = std::move(nextSets);
    }

    void performLift(duckdb::DataChunk& args, duckdb::ExpressionState& state, duckdb::Vector& result) {
        auto count = args.size();
        auto& atts = args.data[0];
        int n = duckdb::ListValue::GetChildren(atts.GetValue(0)).size();

        if (!setsInitialised) {
            setInitialCombinations(n);
            setsInitialised = true;
        } else {
            generateCombinations();
            if (validSets.empty()) {
                auto resultMap = duckdb::Value::MAP(
                    duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT),
                    duckdb::LogicalType::UBIGINT,
                    {}, {}
                );
                for (size_t i = 0; i < count; i++) {
                    result.SetValue(i, resultMap);
                }
                return;
            }
            std::cout << "Valid " << validSets[0].size() << "-sets: " << validSets.size() << "\n";
        }

        DuckDBVectorHash hasher;

        for (size_t i = 0; i < count; i++) {
            auto tuple = duckdb::ListValue::GetChildren(atts.GetValue(i));
            duckdb::vector<duckdb::Value> keys;
            duckdb::vector<duckdb::Value> values;

            for (const auto& set : validSets) {
                duckdb::vector<duckdb::Value> selectedAtts;
                duckdb::vector<duckdb::Value> attValues;
                for (const auto& idx : set) {
                    attValues.push_back(duckdb::Value::INTEGER(idx));
                    selectedAtts.push_back(tuple[idx]);
                }
                duckdb::Value key = duckdb::Value::LIST(attValues);
                keys.push_back(key);
                duckdb::Value value = duckdb::Value::UBIGINT(hasher(selectedAtts));
                values.push_back(value);
            }
            duckdb::Value tupleMap = duckdb::Value::MAP(
                duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER),
                duckdb::LogicalType::UBIGINT,
                keys,
                values
            );
            result.SetValue(i, tupleMap);
        }
    }

    void performPrune(duckdb::DataChunk& args, duckdb::ExpressionState& state, duckdb::Vector& result) {
        int rows = args.size();
        auto& map = args.data[0];
        auto combinationCount = duckdb::MapValue::GetChildren(map.GetValue(0)).size();
        if (combinationCount == 0) {
            auto resultMap = duckdb::Value::MAP(
                duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER),
                duckdb::LogicalType::INTEGER,
                {}, {}
            );
            result.SetValue(0, resultMap);
            return;
        }

        auto pairs = duckdb::MapValue::GetChildren(map.GetValue(0));

        duckdb::vector<duckdb::Value> keys;
        duckdb::vector<duckdb::Value> entropies;

        int N = 0;
        auto firstPair = duckdb::MapValue::GetChildren(pairs[0]);
        for (const auto& val : duckdb::ListValue::GetChildren(firstPair[1])) {
            N += val.GetValue<int>();
        }

        for (const auto& kv : pairs) {
            auto pair = duckdb::MapValue::GetChildren(kv);
            if (duckdb::ListValue::GetChildren(pair[1]).size() == N) {
                std::set<int> invalidSet;
                for (const auto& val : duckdb::ListValue::GetChildren(pair[0])) {
                    invalidSet.insert(val.GetValue<int>());
                }
                validSets.erase(std::remove(validSets.begin(), validSets.end(), invalidSet), validSets.end());
                continue;
            }

            double entropy = 0.0;
            for (const auto& val : duckdb::ListValue::GetChildren(pair[1])) {
                double count = val.GetValue<double>();
                if (count > 1) {
                    entropy += count * std::log2(count);
                }
            }
            entropy = std::log2(N) - (1.0 / N * entropy);

            keys.push_back(pair[0]);
            entropies.push_back(duckdb::Value::DOUBLE(entropy));
        }

        duckdb::Value resultMap = duckdb::Value::MAP(
            duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER),
            duckdb::LogicalType::DOUBLE,
            keys,
            entropies
        );

        result.SetValue(0, resultMap);
    }
};


void liftExactFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    SetPruner::GetInstance().performLift(args, state, result);
}

void pruneFunction(duckdb::DataChunk &args, duckdb::ExpressionState &state, duckdb::Vector &result) {
    SetPruner::GetInstance().performPrune(args, state, result);
}


} // namespace lift_exact