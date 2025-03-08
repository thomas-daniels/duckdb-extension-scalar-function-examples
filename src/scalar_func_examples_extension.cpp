#define DUCKDB_EXTENSION_MAIN

#include "scalar_func_examples_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
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

inline void DiscriminantScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &a_vector = args.data[0];
	auto &b_vector = args.data[1];
	auto &c_vector = args.data[2];

	TernaryExecutor::Execute<double_t, double_t, double_t, double_t>(a_vector, b_vector, c_vector, result, args.size(),
	                                                                 [&](double a, double b, double c) {
		                                                                 auto discriminant = b * b - 4 * a * c;
		                                                                 return discriminant;
	                                                                 });
}

inline void SolveQuadraticEquationScalarFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &a_vector = args.data[0];
	auto &b_vector = args.data[1];
	auto &c_vector = args.data[2];

	GenericExecutor::ExecuteTernary<PrimitiveType<double_t>, PrimitiveType<double_t>, PrimitiveType<double_t>,
	                                StructTypeBinary<double_t, double_t>>(
	    a_vector, b_vector, c_vector, result, args.size(),
	    [&](PrimitiveType<double_t> a, PrimitiveType<double_t> b, PrimitiveType<double_t> c) {
		    auto discriminant = b.val * b.val - 4 * a.val * c.val;
		    StructTypeBinary<double_t, double_t> solution;
		    solution.a_val = (-b.val + sqrt(discriminant)) / (2 * a.val);
		    solution.b_val = (-b.val - sqrt(discriminant)) / (2 * a.val);
		    return solution;
	    });
}

struct QuadraticEquationSolution {
	double x1;
	double x2;
	bool exists;

	static void AssignResult(Vector &result, idx_t i, QuadraticEquationSolution solution) {
		auto &entries = StructVector::GetEntries(result);
		if (!solution.exists) {
			FlatVector::SetNull(result, i, true);
		} else {
			FlatVector::GetData(*entries[0])[i] = solution.x1;
			FlatVector::GetData(*entries[1])[i] = solution.x2;
		}
	}
};

inline void SolveQuadraticEquation2ScalarFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &a_vector = args.data[0];
	auto &b_vector = args.data[1];
	auto &c_vector = args.data[2];

	GenericExecutor::ExecuteTernary<PrimitiveType<double_t>, PrimitiveType<double_t>, PrimitiveType<double_t>,
	                                QuadraticEquationSolution>(
	    a_vector, b_vector, c_vector, result, args.size(),
	    [&](PrimitiveType<double_t> a, PrimitiveType<double_t> b, PrimitiveType<double_t> c) {
		    auto discriminant = b.val * b.val - 4 * a.val * c.val;
		    QuadraticEquationSolution solution;
		    if (discriminant >= 0) {
			    solution.exists = true;
			    solution.x1 = (-b.val + sqrt(discriminant)) / (2 * a.val);
			    solution.x2 = (-b.val - sqrt(discriminant)) / (2 * a.val);
		    } else {
			    solution.exists = false;
		    }
		    return solution;
	    });
}

inline void QuadraticEquationFromSolutionScalarFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &solution_vector = args.data[0];

	GenericExecutor::ExecuteUnary<StructTypeBinary<double_t, double_t>, PrimitiveType<string_t>>(
	    solution_vector, result, args.size(), [&](StructTypeBinary<double_t, double_t> solution) {
		    double x1 = solution.a_val;
		    double x2 = solution.b_val;

		    double b = -x1 - x2;
		    double c = x1 * x2;
		    return StringVector::AddString(result, "x^2 + " + to_string(b) + "x + " + to_string(c));
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

	auto discriminant_scalar_function =
	    ScalarFunction("discriminant", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                   LogicalType::DOUBLE, DiscriminantScalarFun);
	ExtensionUtil::RegisterFunction(instance, discriminant_scalar_function);

	child_list_t<LogicalType> quadratic_equation_solution_child_types;
	quadratic_equation_solution_child_types.push_back(std::make_pair("x1", LogicalType::DOUBLE));
	quadratic_equation_solution_child_types.push_back(std::make_pair("x2", LogicalType::DOUBLE));
	auto solve_quadratic_equation_scalar_function =
	    ScalarFunction("solve_quadratic_equation", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                   LogicalType::STRUCT(quadratic_equation_solution_child_types), SolveQuadraticEquationScalarFunc);
	ExtensionUtil::RegisterFunction(instance, solve_quadratic_equation_scalar_function);

	auto solve_quadratic_equation_scalar_function2 =
	    ScalarFunction("solve_quadratic_equation2", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                   LogicalType::STRUCT(quadratic_equation_solution_child_types), SolveQuadraticEquation2ScalarFunc);
	ExtensionUtil::RegisterFunction(instance, solve_quadratic_equation_scalar_function2);

	auto quadratic_equation_from_solution_scalar_function = ScalarFunction(
	    "quadratic_equation_from_solution", {LogicalType::STRUCT(quadratic_equation_solution_child_types)},
	    LogicalType::VARCHAR, QuadraticEquationFromSolutionScalarFunc);
	ExtensionUtil::RegisterFunction(instance, quadratic_equation_from_solution_scalar_function);
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
