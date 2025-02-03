#include "duckdb.hpp"

#include <iostream>
#include <string>
#include <set>
#include <unordered_set>
#include <vector>
#include <map>
#include <chrono>

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

        computeEntropies();
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

    void computeEntropies() {
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
};

int main() {
    auto start = std::chrono::high_resolution_clock::now();
    SchemaMiner sm("flights.csv", 19);
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start);
    std::cout << "Runtime: " << duration.count() << "s\n";

    return 0;
}