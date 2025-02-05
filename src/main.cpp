#include "duckdb.hpp"

#include <iostream>
#include <string>
#include <set>
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
        int n = attSet.size();
        std::vector<std::vector<int>> subsets(n);
        for (int i = 0; i < attSet.size(); i++) {
            std::vector<int> subset;
            for (int j = 0; j < attSet.size(); j++) {
                if (j != i) {
                    subset.push_back(attSet[j]);
                }
            }
            subsets[n - i - 1] = subset;
        }
        return subsets;
    }

    /*
        Compute entropies for all 1-sets in a single query and make resulting table
    */
    void computeFirstLayer() {
        std::string qry = "CREATE TABLE l1 AS SELECT sum_dict([\n";
        for (int i = 0; i < attributeCount; i++) {
            qry += "\thash_list([col" + std::to_string(i) + "]),\n";
        }
        qry.resize(qry.size() - 2); // Remove last comma and newline
        qry += "]) AS out\nFROM tbl;";

        conn.Query(qry);
        conn.Query("SELECT * FROM l1;")->Print();
    }

    /*
        Compute entropies for all n-sets in a single query.
        Assume that the previous layer query has been executed and stored in the table l[n-1]
        and original relation is stored in tbl.

        Returns 1 if at least one valid n-set is found, 0 otherwise. 
    */
    int computeSingleLayer(int n) {
        // TODO: Store combinations / previous combinations as class members
        // avoids regeneration and allows easy saving of entropies
        // TODO: Save entropies
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
        
        //std::cout << qry << "\n\n";
        conn.Query(qry);
        conn.Query("SELECT * FROM l" + std::to_string(n) + ";")->Print();

        // Check for non-zero entropies 
        auto entropyResult = conn.Query("SELECT out.entropies FROM l" + std::to_string(n) + ";");
        auto entropyList = entropyResult->GetValue(0, 0);
        for (const auto& entropy : duckdb::ListValue::GetChildren(entropyList)) {
            if (entropy.GetValue<int>() != 0) {
                return 1;
            }
        }

        return 0;
    }

    /*
        Compute entropies for all set (unless no n-sets are found) at which point we stop
        and prune individual tuples at each stage.
    */
    void computeEntropiesWithPruning() {
        computeFirstLayer();

        int layer = 2;
        while (layer <= attributeCount) {
            if (!computeSingleLayer(layer)) {
                // No valid n-sets found, stop
                break;
            }
            layer++;
        }
    }
};

int main() {
    auto start = std::chrono::high_resolution_clock::now();
    SchemaMiner sm("./dataviz/datasets/small_flights.csv", 19);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Runtime: " << duration.count() << "ms\n";

    return 0;
}