#define DUCKDB_EXTENSION_MAIN

#include "scalar_func_examples_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void ScalarFuncExamplesScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "ScalarFuncExamples "+name.GetString()+" 🐥");
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto scalar_func_examples_scalar_function = ScalarFunction("scalar_func_examples", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ScalarFuncExamplesScalarFun);
    ExtensionUtil::RegisterFunction(instance, scalar_func_examples_scalar_function);
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
