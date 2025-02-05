#include "duckdb.hpp"

#include <iostream>
#include <string>
#include <set>
#include <unordered_set>
#include <vector>
#include <map>
#include <chrono>
#include <functional>

using AttributeSet = std::set<int>;

class SchemaMiner {
private:
    static duckdb::DBConfig* initConfig() {
        auto config = new duckdb::DBConfig();
        config->options.allow_unsigned_extensions = true;
        return config;
    }

protected:
    // DB
    duckdb::DuckDB db;
    duckdb::Connection conn;

    // Relation info 
    std::string csvPath;
    int attributeCount;
    long tupleCount;

    // Entropies 
    std::map<AttributeSet, double> entropies;

public:
    SchemaMiner(std::string csvPath, int attributeCount) : 
        csvPath(csvPath),
        attributeCount(attributeCount),
        db(nullptr, initConfig()),
        conn(db) {

        // Load extension and CSV
        loadExtension();
        loadCSV();

        computeEntropiesWithPruning();
    }

    void loadExtension() {
        std::string loadQry = "LOAD './mining_extension/build/release/extension/quack/quack.duckdb_extension';";

        auto loadResult = conn.Query(loadQry);

        if (loadResult->HasError()) {
            std::string loadErrMsg = "\033[1;31mFailed to load mining extension: \033[0m";
            std::cerr << loadErrMsg << loadResult->ToString();
            exit(1);
        }
    }

    void loadCSV() {
        std::string loadQry = "CREATE TABLE tbl AS SELECT * FROM read_csv('" + csvPath + "', header=false, columns={";
        for (int i = 0; i < attributeCount; i++) {
            loadQry += "'col" + std::to_string(i) + "': 'VARCHAR'";
            if (i != attributeCount - 1) {
                loadQry += ",";
            }
        }
        loadQry += "});";
        conn.Query(loadQry);
    }

    std::map<AttributeSet, double> getEntropies() {
        return entropies;
    }

    /*
        This method prunes entire attribute sets where possible but doesn't 
        prune individual tuples.
    */
    void computeEntropiesPruneSets() {
        std::string computeQry = "SELECT prune(custom_sum(lift_exact([";
        for (int i = 0; i < attributeCount; i++) {
            computeQry += "col" + std::to_string(i);
            if (i != attributeCount - 1) {
                computeQry += ", ";
            }
        }
        computeQry += "]))) FROM tbl;";
        bool hasResults = true;

        while (hasResults) {
            auto entropiesResult = conn.Query(computeQry);
            auto map = entropiesResult->GetValue(0, 0);
            auto entryCount = duckdb::MapValue::GetChildren(map).size();

            hasResults = entryCount > 0;
        }
    }

    /*
        Generate all n-set combinations of attributes.
    */
    std::vector<std::vector<int>> getAttributeCombinations(int n) {
        std::vector<std::vector<int>> combinations;
        std::vector<int> stack;

        std::function<void(int, int)> generateCombinations = [&](int start, int k) {
            if (k == 0) {
                combinations.push_back(stack);
                return;
            }

            for (int i = start; i <= attributeCount - k; i++) {
                stack.push_back(i);
                generateCombinations(i + 1, k - 1);
                stack.pop_back();
            }
        };

        generateCombinations(0, n);
        return combinations;
    }

    std::vector<std::vector<int>> getSubsets(std::vector<int> attSet) {
        std::vector<std::vector<int>> subsets(attSet.size());
        for (int i = 0; i < attSet.size(); i++) {
            for (int j = 0; j < attSet.size(); j++) {
                if (i != j) {
                    subsets[i].push_back(attSet[j]);
                }
            }
        }
        return subsets;
    }

    /*
        Compute entropies for all n-sets in a single query.
        Assume that the previous layer query has been executed and stored in the table l[n-1]
        and original relation is stored in tbl.
    */
    void computeSingleLayer(int n) {
        auto attSets = getAttributeCombinations(n);
        auto prevAttSets = getAttributeCombinations(n - 1);

        // Map previous sets to their position in l[n-1].out.sets
        std::map<std::vector<int>, int> prevIndexMap;
        for (int i = 0; i < prevAttSets.size(); i++) {
            prevIndexMap[prevAttSets[i]] = i;
        }

        std::string qry = "CREATE TABLE l" + std::to_string(n) + " AS SELECT sum_dict([\n";
        for (auto& atts : attSets) {
            qry += "\tCASE\n\t\tWHEN ";

            std::vector<std::vector<int>> subsets = getSubsets(atts);

            // Iterate through atts. and remove 1 by 1 to create filtering conditions
            for (const auto& subset : subsets) {
                int offset = prevIndexMap[subset];
                qry += "filt(hash_list([";
                for (const auto& att : subset) {
                    qry += "col" + std::to_string(att);
                    if (att != subset.back()) {
                        qry += ", ";
                    }
                }
                qry += "]), l" + std::to_string(n - 1) + ".out.sets, " + std::to_string(offset) + ") AND\n\t\t\t";
            }

            qry.resize(qry.size() - 7); // Remove last AND\n\t\t\t

            qry += "\n\t\tTHEN hash_list([";
            for (const auto& att : atts) {
                qry += "col" + std::to_string(att) + ",";
            }
            qry.resize(qry.size() - 1); // Remove last comma
            qry += "])\n\t\tELSE NULL\n\tEND,\n";
        }
        qry.resize(qry.size() - 2); // Remove last comma and newline

        qry += "]) AS out\nFROM tbl, l" + std::to_string(n - 1) + ";";
        std::cout << qry << "\n\n";

        // START: Why are the att. sets being generated in reverse order.
    }

    /*
        This method computes entropies for all sets (unless no n-sets are found)
        but prunes individual tuples at each stage.
    */
    void computeEntropiesWithPruning() {
        for (int i = 2; i < attributeCount+1; i++) {
            computeSingleLayer(i);
        }
    }
};

int main() {
    auto start = std::chrono::high_resolution_clock::now();
    SchemaMiner sm("./dataviz/datasets/test.csv", 3);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    // std::cout << "Runtime: " << duration.count() << "s\n";

    return 0;
}