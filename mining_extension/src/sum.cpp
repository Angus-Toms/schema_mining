#include "duckdb.hpp"

#include <map>
#include <iostream>
#include <vector>
#include <set>

// TODO: We don't need to save the values in the map, just the count
// all we care about is the relevent distribution

// Correction: we need the values during the update process to correctly update counts 
// after that, we can discard the particular values for the output

// The attribute sets are the same for all rows, so we only need to store them once

namespace customSum {

using hash_t = uint64_t;
using AttributeSet = std::set<int>;

void printAttributeSet(const AttributeSet& attSet) {
    std::string out = "[";
    for (const int i : attSet) {
        out += std::to_string(i) + ", ";
    }
    out.pop_back();
    out.pop_back();
    out += "]\n";
    std::cout << out;
}

struct CustomSumState {
    std::vector<AttributeSet> attributeSets;
    std::vector<std::map<hash_t, int64_t>> maps;
};

struct CustomSumFunction {
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

static void customSumUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData &, idx_t inputCount, duckdb::Vector &stateVector, idx_t count) {    
    // Input from lift function is map:
    // key: INTEGER[] (attribute set)
    // value: UBIGINT (hash)
    
    auto inputMaps = inputs[0];
    duckdb::UnifiedVectorFormat mapsData; // TODO: needed?
    duckdb::UnifiedVectorFormat sdata;
    inputMaps.ToUnifiedFormat(count, mapsData);
    stateVector.ToUnifiedFormat(count, sdata);

    // Assuming no GROUP BY clause is passed, therefore single state 
    auto states = (CustomSumState **)sdata.data;
    auto &state = *states[sdata.sel->get_index(0)];

    // Create containers for attrSets and maps 
    auto combinationCount = duckdb::MapValue::GetChildren(inputMaps.GetValue(0)).size();

    state.attributeSets.resize(combinationCount);
    state.maps.resize(combinationCount);

    // Get attribute sets from the first row (all rows have the same attribute sets)
    auto firstMap = duckdb::MapValue::GetChildren(inputMaps.GetValue(0));
    for (idx_t i = 0; i < combinationCount; i++) {
        auto attrSet = duckdb::MapValue::GetChildren(firstMap[i])[0];
        std::set<int> attIndeces;
        for (const auto& attr : duckdb::ListValue::GetChildren(attrSet)) {
            attIndeces.insert(attr.GetValue<int>());
        }
        state.attributeSets[i] = attIndeces;
    }

    // Count occurrences
    for (idx_t i = 0; i < count; i++) {
        auto tuple = duckdb::MapValue::GetChildren(inputMaps.GetValue(i)); // i-th tuple
        for (idx_t j = 0; j < combinationCount; j++) {
            auto combination = tuple[j]; //j-th pair in map
            hash_t hash = duckdb::MapValue::GetChildren(combination)[1].GetValue<hash_t>();
            state.maps[j][hash]++;
        }
    }
}

static void customSumCombine(duckdb::Vector &stateVector, duckdb::Vector &combined, duckdb::AggregateInputData &, idx_t count) {    
    duckdb::UnifiedVectorFormat sdata;
    stateVector.ToUnifiedFormat(count, sdata);
    auto statePtr = (CustomSumState **)sdata.data;
    auto combinedPtr = duckdb::FlatVector::GetData<CustomSumState *>(combined);

    for (idx_t i = 0; i < count; i++) {
        auto &state = *statePtr[sdata.sel->get_index(i)];

        if (state.maps.empty()) {
            // State maps not initialized, skip
            continue;
        }

        if (combinedPtr[i]->maps.empty()) {
            // Combined not initialized, copy over maps and att sets
            combinedPtr[i]->maps = state.maps;
            combinedPtr[i]->attributeSets = state.attributeSets;
        } else {
            // Both state and combined are initialized, combine
            // Assume that maps are in the same order
            // Attribute sets should be the same for the combining states?
            for (idx_t j = 0; j < state.maps.size(); j++) {
                for (const auto& [k, v] : state.maps[j]) {
                    combinedPtr[i]->maps[j][k] += v;
                }
            }
        }
    }
}

static void customSumFinalize(duckdb::Vector &stateVector, duckdb::AggregateInputData &, duckdb::Vector &result, idx_t count, idx_t offset) {
    // Output is map
    // key: INTEGER[] (attribute set)
    // value: INTEGER[] (dist)
    
    duckdb::UnifiedVectorFormat sdata;
    stateVector.ToUnifiedFormat(count, sdata);
    auto &mask = duckdb::FlatVector::Validity(result);
    auto oldLen = duckdb::ListVector::GetListSize(result);

    // Assuming no GROUP BY clause is passed, therefore single state
    auto states = (CustomSumState **)sdata.data;
    auto &state = *states[sdata.sel->get_index(0)];

    // Create result map 
    int resultCount = state.maps.size();
    duckdb::vector<duckdb::Value> keys(resultCount);
    duckdb::vector<duckdb::Value> values(resultCount);

    for (idx_t i = 0; i < resultCount; i++) {
        // Create duckdb::Value for key
        AttributeSet attSet = state.attributeSets[i];
        duckdb::vector<duckdb::Value> keyVec(attSet.size());
        size_t idx = 0;
        for (const int att : attSet) {
            keyVec[idx++] = duckdb::Value::INTEGER(att);
        }
        keys[i] = duckdb::Value::LIST(keyVec);

        // Create duckdb::Value for value
        std::map<hash_t, int64_t> &map = state.maps[i];
        duckdb::vector<duckdb::Value> valueVec(map.size());
        idx = 0;
        for (const auto& [_, v] : map) {
            valueVec[idx++] = duckdb::Value::INTEGER(v);
        }

        values[i] = duckdb::Value::LIST(valueVec);
    }
    
    // Set result
    duckdb::Value resultMap = duckdb::Value::MAP(
        duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), // key type
        duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), // value type
        keys, values
    );

    result.SetValue(0, resultMap);

    // Set result list size
    auto resultData = duckdb::ListVector::GetData(result);
    resultData[offset].length = duckdb::ListVector::GetListSize(result) - oldLen;
    resultData[offset].offset = oldLen;
    oldLen += resultData[offset].length;

    // Output will always be a single row?
    result.Verify(1);
}

duckdb::unique_ptr<duckdb::FunctionData> customSumBind(duckdb::ClientContext &context, duckdb::AggregateFunction &function, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {
    auto resType = duckdb::LogicalType::MAP(
        duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER), // key: attr set
        duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER) // value: dist
    );
    function.return_type = resType;
    return duckdb::make_uniq<duckdb::VariableReturnBindData>(function.return_type);
}

} // namespace customSum