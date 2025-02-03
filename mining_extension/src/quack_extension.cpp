#define DUCKDB_EXTENSION_MAIN

#include "quack_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "get_entropy.cpp"
#include "lift.cpp"
#include "lift_exact.cpp"
#include "sum.cpp"
#include "sum_no_lift.cpp"
#include "prune.cpp"
#include "hash_list.cpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

void registerLiftFunction(DuckDB &db) {
	auto liftFunc = ScalarFunction(
		"lift",
		{LogicalType::LIST(LogicalType::VARCHAR)},
		LogicalType::MAP(
			LogicalType::LIST(LogicalType::INTEGER), // key: attr set
			LogicalType::UBIGINT // value: hash
		),
		lift::liftFunction
	);
	ExtensionUtil::RegisterFunction(*db.instance, liftFunc);
}

void registerHashListFunction(DuckDB &db) {
	auto argTypes = {LogicalType::LIST(LogicalType::VARCHAR)};
	auto returnType = LogicalType::UBIGINT;
	auto hashListFunc = ScalarFunction(
		"hash_list",
		argTypes,
		returnType,
		hash_list::hashListFunction
	);
	ExtensionUtil::RegisterFunction(*db.instance, hashListFunc);
}

void registerLiftExactFunction(DuckDB &db) {
	auto liftExactFunc = ScalarFunction(
		"lift_exact",
		{LogicalType::LIST(LogicalType::VARCHAR)},
		LogicalType::MAP(
			LogicalType::LIST(LogicalType::INTEGER), // key: attr set
			LogicalType::UBIGINT // value: hash
		),
		lift_exact::liftExactFunction
	);
	ExtensionUtil::RegisterFunction(*db.instance, liftExactFunc);
}

void registerCustomSumFunction(DuckDB &db) {
	auto argTypes = duckdb::vector<LogicalType>{
		LogicalType::MAP(
			LogicalType::LIST(LogicalType::INTEGER), // key: attr set
			LogicalType::UBIGINT					 // value: hash
		)
	};

	auto returnType = LogicalType::MAP(
		LogicalType::LIST(LogicalType::INTEGER), // key: attr set
		LogicalType::LIST(LogicalType::INTEGER) // value: dist
	);

	auto customSumFunc = AggregateFunction(
		"custom_sum",
		argTypes,
		returnType,
		AggregateFunction::StateSize<customSum::CustomSumState>,
		AggregateFunction::StateInitialize<customSum::CustomSumState, customSum::CustomSumFunction>,
		customSum::customSumUpdate,
		customSum::customSumCombine,
		customSum::customSumFinalize,
		nullptr,
		customSum::customSumBind,
		AggregateFunction::StateDestroy<customSum::CustomSumState, customSum::CustomSumFunction>
	);

	ExtensionUtil::RegisterFunction(*db.instance, customSumFunc);
}

void registerSumNoLiftFunction(DuckDB &db) {
	auto argTypes = {duckdb::LogicalType::LIST(duckdb::LogicalType::ANY)};

	auto sumNoLiftFunc = duckdb::AggregateFunction(
		"sum_no_lift",
		argTypes,
		duckdb::LogicalType::LIST(
			duckdb::LogicalType::MAP(duckdb::LogicalType::UBIGINT, duckdb::LogicalType::INTEGER)
		),
		duckdb::AggregateFunction::StateSize<sumNoLift::SumNoLiftState>,
		duckdb::AggregateFunction::StateInitialize<sumNoLift::SumNoLiftState, sumNoLift::SumNoLiftFunction>,
		sumNoLift::sumNoLiftUpdate,
		sumNoLift::sumNoLiftCombine,
		sumNoLift::sumNoLiftFinalize,
		nullptr,
		sumNoLift::sumNoLiftBind,
		duckdb::AggregateFunction::StateDestroy<sumNoLift::SumNoLiftState, sumNoLift::SumNoLiftFunction>
	);

	ExtensionUtil::RegisterFunction(*db.instance, sumNoLiftFunc);
}

void registerGetEntropyFunction(DuckDB &db) {
	auto getEntropyFunc = ScalarFunction(
		"get_entropy",
		{duckdb::LogicalType::LIST(
			duckdb::LogicalType::MAP(duckdb::LogicalType::UBIGINT, duckdb::LogicalType::INTEGER))},
		duckdb::LogicalType::MAP(duckdb::LogicalType::VARCHAR, duckdb::LogicalType::DOUBLE),
		getEntropy::getEntropyFunction
	);

	ExtensionUtil::RegisterFunction(*db.instance, getEntropyFunc);
}

void registerPruneFunction(DuckDB &db) {
	auto argTypes = {LogicalType::MAP(
		LogicalType::LIST(LogicalType::INTEGER), // key: attr set
		LogicalType::LIST(LogicalType::INTEGER)  // value: dist
	)};
	auto resultType = LogicalType::MAP(
		LogicalType::LIST(LogicalType::INTEGER), // key: attr set
		LogicalType::DOUBLE // value: entropy
	);
	auto pruneFunc = ScalarFunction(
		"prune",
		argTypes,
		resultType,
		lift_exact::pruneFunction
	);

	ExtensionUtil::RegisterFunction(*db.instance, pruneFunc);
}

void QuackExtension::Load(DuckDB &db) {
	registerLiftFunction(db);
	registerLiftExactFunction(db);
	registerCustomSumFunction(db);
	registerSumNoLiftFunction(db);
	registerGetEntropyFunction(db);
	registerPruneFunction(db);
	registerHashListFunction(db);
}

std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void quack_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::QuackExtension>();
}

DUCKDB_EXTENSION_API const char *quack_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif