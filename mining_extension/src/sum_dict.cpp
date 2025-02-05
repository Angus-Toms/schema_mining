#include "duckdb.hpp"

#include <map>
#include <vector>
#include <set>
#include <iostream>

namespace sumDict {

using hash_t = uint64_t;

struct SumDictState {
    std::vector<std::map<hash_t, int64_t>> maps;
};

struct SumDictFunction {
    template <class STATE>
    static void Initialize(STATE &state) {
        return;
    }

    template <class STATE>
    static void Destroy(STATE &state, duckdb::AggregateInputData &aggr_input_data) {
        return;
    }

    static bool IgnoreNull() {
        return true;
    }
};

static void sumDictUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData &, idx_t inputCount, duckdb::Vector &stateVector, idx_t count) {
    // Input is a list of UBIGINT from calls to hash_list
    auto inputList = inputs[0];

    // Assuming no GROUP BY clause is passed, therefore single state 
    auto states = (SumDictState **)stateVector.GetData();
    auto& state = *states[0];

    // Create container for maps
    auto combinationCount = duckdb::MapValue::GetChildren(inputList.GetValue(0)).size();
    state.maps.resize(combinationCount);

    // Count occurrences of each hashed value 
    for (idx_t i = 0; i < count; i++) {
        auto hashes = duckdb::ListValue::GetChildren(inputList.GetValue(i));
        for (idx_t j = 0; j < hashes.size(); j++) {
            if (!hashes[j].IsNull()) {
                auto hash = hashes[j].GetValue<hash_t>();
                state.maps[j][hash]++;
            }
        }
    }
}

static void sumDictCombine(duckdb::Vector &stateVector, duckdb::Vector &combined, duckdb::AggregateInputData &, idx_t count) {
    duckdb::UnifiedVectorFormat sdata;
    stateVector.ToUnifiedFormat(count, sdata);
    auto statePtr = (SumDictState **)sdata.data;
    auto combinedPtr = duckdb::FlatVector::GetData<SumDictState *>(combined);

    for (idx_t i = 0; i < count; i++) {
        auto& state = *statePtr[sdata.sel->get_index(i)];

        if (state.maps.empty()) {
            // State maps not initialized, skip
            continue;
        }

        if (combinedPtr[i]->maps.empty()) {
            // Combined not initialized, copy over maps
            combinedPtr[i]->maps = state.maps;
        } else {
            // Both state and combined are initialized, combine
            // Can safely assume that maps are in the same order
            for (idx_t j = 0; j < state.maps.size(); j++) {
                for (const auto& [k, v] : state.maps[j]) {
                    combinedPtr[i]->maps[j][k] += v;
                }
            }    
        }
    }
}

static void sumDictFinalize(duckdb::Vector &stateVector, duckdb::AggregateInputData &, duckdb::Vector &result, idx_t count, idx_t offset) {
    // Output contains a struct with two fields:
    // 1. 'sets': A list of UBIGINTs for each value (for each att set, a set of valid un-pruned vals)
    // 2. 'entropies': A list of doubles: the entropy of each att set

    duckdb::UnifiedVectorFormat sdata;
    stateVector.ToUnifiedFormat(count, sdata);
    auto &mask = duckdb::FlatVector::Validity(result);
    auto oldLen = duckdb::ListVector::GetListSize(result);

    // Assuming no GROUP BY clause is passed, therefore single state
    auto states = (SumDictState **)sdata.data;
    auto &state = *states[sdata.sel->get_index(0)];
    auto resultCount = state.maps.size();

    // Create result value containers
    duckdb::vector<duckdb::Value> unprunedVals;
    duckdb::vector<duckdb::Value> entropies;

    // Find N (number of records) from first map to allow pruning
    auto N = 0;
    for (const auto& [k, v] : state.maps[0]) {
        N += v;
    }

    // Iterate through att. sets
    for (idx_t i = 0; i < resultCount; i++) {
        if (state.maps[i].size() == N) {
            // Entire dist. is unique, skip
            entropies.push_back(duckdb::Value::DOUBLE(0));
            unprunedVals.push_back(duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, {}));
            continue;
        } 

        // Calculate entropy and prune unique values from remaining dist
        double entropy = 0.0;
        duckdb::vector<duckdb::Value> unpruned;
        for (const auto& [k, v] : state.maps[i]) {
            entropy += (double) v * std::log2((double) v);
            if (v > 1) {
                unpruned.push_back(duckdb::Value::UBIGINT(k));
            }
        }
        entropies.push_back(duckdb::Value::DOUBLE(entropy));
        unprunedVals.push_back(duckdb::Value::LIST(duckdb::LogicalType::UBIGINT, unpruned));
    }

    // Create struct result
    duckdb::vector<std::pair<std::string, duckdb::Value>> structValues;
    
    // Specify types explicitly in case the lists are empty
    structValues.push_back(make_pair("sets", duckdb::Value::LIST(duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT), unprunedVals)));
    structValues.push_back(make_pair("entropies", duckdb::Value::LIST(duckdb::LogicalType::DOUBLE, entropies)));

    result.SetValue(0, duckdb::Value::STRUCT(structValues));
}

duckdb::unique_ptr<duckdb::FunctionData> sumDictBind(duckdb::ClientContext &context, duckdb::AggregateFunction &function, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {
    duckdb::vector<std::pair<std::string, duckdb::LogicalType>> structTypes;
    structTypes.push_back(std::make_pair("sets", duckdb::LogicalType::LIST(duckdb::LogicalType::LIST(duckdb::LogicalType::UBIGINT))));
    structTypes.push_back(std::make_pair("entropies", duckdb::LogicalType::LIST(duckdb::LogicalType::DOUBLE)));

    auto resultType = duckdb::LogicalType::STRUCT(structTypes);
    function.return_type = resultType;
    return duckdb::make_uniq<duckdb::VariableReturnBindData>(function.return_type);
}

} // namespace sumDict