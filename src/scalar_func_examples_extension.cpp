#define DUCKDB_EXTENSION_MAIN

#include "scalar_func_examples_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void QuackScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Quack " + name.GetString() + " 🐥");
	});
}

inline void IsLeapYearScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &year_vector = args.data[0];
	UnaryExecutor::Execute<int32_t, bool>(year_vector, result, args.size(), [&](int32_t year) {
		return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
	});
}

inline void FibonacciScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_vector = args.data[0];

	auto Phi = (1 + sqrt(5)) / 2;
	auto phi = Phi - 1;

	UnaryExecutor::ExecuteWithNulls<int32_t, int64_t>(
	    input_vector, result, args.size(), [&](int32_t input, ValidityMask &mask, idx_t idx) {
		    if (input >= 0 && input < 93) {
			    return lround((pow(Phi, input) - pow(-phi, input)) / sqrt(5));
		    } else {
			    mask.SetInvalid(idx);
			    return 0l;
		    }
	    });
}

static void LoadInternal(DatabaseInstance &instance) {
	auto quack_scalar_function = ScalarFunction("quack", {LogicalType::VARCHAR}, LogicalType::VARCHAR, QuackScalarFun);
	ExtensionUtil::RegisterFunction(instance, quack_scalar_function);

	auto is_leap_year_scalar_function =
	    ScalarFunction("is_leap_year", {LogicalType::INTEGER}, LogicalType::BOOLEAN, IsLeapYearScalarFun);
	ExtensionUtil::RegisterFunction(instance, is_leap_year_scalar_function);

	auto fibonacci_scalar_function =
	    ScalarFunction("fibonacci", {LogicalType::INTEGER}, LogicalType::BIGINT, FibonacciScalarFun);
	ExtensionUtil::RegisterFunction(instance, fibonacci_scalar_function);
}

void ScalarFuncExamplesExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ScalarFuncExamplesExtension::Name() {
	return "scalar_func_examples";
}

std::string ScalarFuncExamplesExtension::Version() const {
#ifdef EXT_VERSION_SCALAR_FUNC_EXAMPLES
	return EXT_VERSION_SCALAR_FUNC_EXAMPLES;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void scalar_func_examples_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::ScalarFuncExamplesExtension>();
}

DUCKDB_EXTENSION_API const char *scalar_func_examples_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
