"""Constants that have the functions of their type, as a column of that type has: ``constant("Hello").length()``,
``constant(datetime.datetime(2016, 8, 10, 8, 55)).hour()``, ``constant(16).sqrt()``.

A constant is the input of its own functions and a function of it is labelled with its value, as a column's is with its
display name. A bool has no functions."""
from datafinder.date_functions import DateFunctions, DateTimeFunctions
from datafinder.scalar_functions import (
    DecimalFunctionOperation, DoubleFunctionOperation, IntegerFunctionOperation, NumericFunctions, StringFunctions,
)
from model.relational import (
    ConstantOperation, DateConstantOperation, DateTimeConstantOperation, DecimalConstantOperation,
    FloatConstantOperation, IntegerConstantOperation, StringConstantOperation, StringPredicates, concat,
)


class _FunctionInput:
    """A constant is what its functions are applied to, and its value names them."""

    def _function_input(self) -> ConstantOperation:
        return self  # type: ignore[return-value]

    def _function_label(self) -> str:
        return str(getattr(self, "value"))


class StringConstant(_FunctionInput, StringFunctions, StringPredicates, StringConstantOperation):
    """A text constant, which has the string functions and pattern tests of a text column, and joins with ``+``."""

    def __add__(self, other):
        return concat(self, other)

    def __radd__(self, other):
        return concat(other, self)


class IntegerConstant(_FunctionInput, NumericFunctions[IntegerFunctionOperation], IntegerConstantOperation):
    """A whole number constant, which has the numeric functions of an integer column."""

    _number_type = IntegerFunctionOperation


class FloatConstant(_FunctionInput, NumericFunctions[DoubleFunctionOperation], FloatConstantOperation):
    """A floating-point constant, which has the numeric functions of a double column."""

    _number_type = DoubleFunctionOperation


class DecimalConstant(_FunctionInput, NumericFunctions[DecimalFunctionOperation], DecimalConstantOperation):
    """A decimal constant, which has the numeric functions of a decimal column."""

    _number_type = DecimalFunctionOperation


class DateConstant(_FunctionInput, DateFunctions, DateConstantOperation):
    """A date constant, which has the date functions of a date column."""


class DateTimeConstant(_FunctionInput, DateTimeFunctions, DateTimeConstantOperation):
    """A datetime constant, which has the datetime functions of a datetime column."""
