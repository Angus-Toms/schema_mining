#include "duckdb.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

#include <cmath>
#include <map>
#include <vector>
#include <string>
#include <functional>

namespace sumNoLift {

using hash_t = uint64_t;

hash_t combineHashes(hash_t a, hash_t b) {
    return a ^ (b + 0x9e3779b9 + (a << 6) + (a >> 2));
}

std::vector<hash_t> getHashCombinations(const duckdb::vector<duckdb::Value>& values) {
    std::vector<hash_t> result;
    std::vector<hash_t> stack;

    std::function<void(int)> generateCombinations = [&](int start) {
        for (int i = start; i < values.size(); i++) {
            stack.push_back(values[i].Hash());
            hash_t hash = 0;
            for (const auto& h : stack) {
                hash = combineHashes(hash, h);
            }
            result.push_back(hash);
            generateCombinations(i + 1);
            stack.pop_back();
        }
    };
    generateCombinations(0);
    return result;
}

struct SumNoLiftState {
    std::vector<std::map<hash_t, int64_t>> maps;
};

struct SumNoLiftFunction {
    template <class STATE>
    static void Initialize(STATE &state) {
        state.maps = {};
    }

    template <class STATE>
    static void Destroy(STATE &state, duckdb::AggregateInputData &aggr_input_data) {
        return;
    }

    static bool IgnoreNull() {
        return true;
    }
};

static void sumNoLiftUpdate(duckdb::Vector inputs[], duckdb::AggregateInputData &, idx_t input_count, duckdb::Vector &state_vector, idx_t count) {
    auto &cols = inputs[0];
    duckdb::UnifiedVectorFormat colsData;
    duckdb::UnifiedVectorFormat sdata;

    cols.ToUnifiedFormat(count, colsData);
    state_vector.ToUnifiedFormat(count, sdata);
    auto states = (SumNoLiftState **)sdata.data;

    // Assuming no GROUP BY clause is passed, therefore single state
    auto &state = *states[sdata.sel->get_index(0)];

    // Create empty map for each combination
    auto att_count = duckdb::ListValue::GetChildren(cols.GetValue(0)).size();
    auto comb_count = std::pow(2, att_count) - 1;
    state.maps = std::vector<std::map<hash_t, int64_t>>(comb_count);

    for (idx_t i = 0; i < count; i++) {
        std::cout << i << "\n";

        auto tuple = duckdb::ListValue::GetChildren(cols.GetValue(i));
        std::vector<hash_t> combinations = getHashCombinations(tuple);

        for (idx_t j = 0; j < comb_count; j++) {
            state.maps[j][combinations[j]]++;
        }
    }
}

static void sumNoLiftCombine(duckdb::Vector &state_vector, duckdb::Vector &combined, duckdb::AggregateInputData &, idx_t count) {    
    duckdb::UnifiedVectorFormat sdata;
    state_vector.ToUnifiedFormat(count, sdata);
    auto state_ptr = (SumNoLiftState **)sdata.data;
    auto combined_ptr = duckdb::FlatVector::GetData<SumNoLiftState *>(combined);

    for (idx_t i = 0; i < count; i++) {
        auto &state = *state_ptr[sdata.sel->get_index(i)];

        if (state.maps.empty()) {
            // State maps not initialized, skip
            continue;
        }

        if (combined_ptr[i]->maps.empty()) {
            // Combined maps not initialized, copy state maps
            combined_ptr[i]->maps = state.maps;
        } else {
            // Both state and combined maps are initialized, combine
            // Assume that maps are in the same order
            for (idx_t j = 0; j < state.maps.size(); j++) {
                for (const auto& [k, v] : state.maps[j]) {
                    combined_ptr[i]->maps[j][k] += v;
                }
            }
        }
    }
}

static void sumNoLiftFinalize(duckdb::Vector &state_vector, duckdb::AggregateInputData &, duckdb::Vector &result, idx_t count, idx_t offset) {
    duckdb::UnifiedVectorFormat sdata;
    state_vector.ToUnifiedFormat(count, sdata);
    auto states = (SumNoLiftState **)sdata.data;
    auto &mask = duckdb::FlatVector::Validity(result);
    auto old_len = duckdb::ListVector::GetListSize(result);

    // Assuming no GROUP BY clause is passed, therefore single state 
    auto &state = *states[sdata.sel->get_index(0)];

    // Create result list 
    for (const auto& map : state.maps) {
        auto map_size = map.size();
        duckdb::vector<duckdb::Value> keys(map_size);
        duckdb::vector<duckdb::Value> values(map_size);

        idx_t index = 0;
        for (const auto& [k, v] : map) {
            keys[index] = duckdb::Value::UBIGINT(k);
            values[index] = duckdb::Value::INTEGER(v);
            index++;
        }

        duckdb::Value combination_map = duckdb::Value::MAP(
            duckdb::LogicalType::UBIGINT, // key type 
            duckdb::LogicalType::INTEGER, // value type 
            keys,
            values
        );
        duckdb::ListVector::PushBack(result, combination_map);
    }

    // Set result list size 
    auto result_data = duckdb::ListVector::GetData(result);
    result_data[offset].length = duckdb::ListVector::GetListSize(result) - old_len;
    result_data[offset].offset = old_len;
    old_len += result_data[offset].length;

    result.Verify(1);
}

duckdb::unique_ptr<duckdb::FunctionData> sumNoLiftBind(duckdb::ClientContext &context, duckdb::AggregateFunction &function, duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> &arguments) {
    auto res_type = duckdb::LogicalType::LIST(duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::INTEGER));
    function.return_type = res_type;
    return duckdb::make_uniq<duckdb::VariableReturnBindData>(function.return_type);
}

} // namespace sumNoLift