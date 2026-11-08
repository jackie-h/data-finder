"""Scalar functions as methods, shared by attributes and by the results of other functions, so they chain:
``f.name().upper().length()``.

An attribute reads its column and is labelled by its display name; a function result is itself the input and is
labelled by its own name."""
import decimal
from typing import TYPE_CHECKING, Any, Callable, Generic, TypeVar

from model.relational import (
    Arithmetic, Equatable, Orderable, ScalarFunction, ScalarFunctionOperation, StringPredicates, concat,
)

N = TypeVar("N", bound=ScalarFunctionOperation)


def string_function(element, function: ScalarFunction, label: str | None = None, second_arg: int | float | None = None,
                    extra_args: list | None = None) -> 'StringFunctionOperation':
    """A function that gives text, applied to ``element``."""
    return StringFunctionOperation(element, function, label, second_arg=second_arg, extra_args=extra_args)


def integer_function(element, function: ScalarFunction, label: str | None = None, second_arg: int | float | None = None,
                     extra_args: list | None = None) -> 'IntegerFunctionOperation':
    """A function that gives a whole number, applied to ``element``."""
    return IntegerFunctionOperation(element, function, label, second_arg=second_arg, extra_args=extra_args)


def double_function(element, function: ScalarFunction, label: str | None = None, second_arg: int | float | None = None,
                    extra_args: list | None = None) -> 'DoubleFunctionOperation':
    """A function that gives a floating-point number, applied to ``element``."""
    return DoubleFunctionOperation(element, function, label, second_arg=second_arg, extra_args=extra_args)


class StringFunctions:
    """Functions of text. The user of this class provides ``_function_input()`` and ``_function_label()``."""

    if TYPE_CHECKING:  # provided by the class this is mixed into; declared here only, so it does not hide the real one
        def _function_input(self) -> Any: ...
        def _function_label(self) -> str: ...

    def upper(self):
        """Return the value converted to uppercase. Equivalent to ``str.upper()``."""
        return string_function(self._function_input(), ScalarFunction.UPPER, 'Upper ' + self._function_label())

    def lower(self):
        """Return the value converted to lowercase. Equivalent to ``str.lower()``."""
        return string_function(self._function_input(), ScalarFunction.LOWER, 'Lower ' + self._function_label())

    def strip(self):
        """Remove leading and trailing whitespace. Equivalent to ``str.strip()``."""
        return string_function(self._function_input(), ScalarFunction.TRIM, 'Strip ' + self._function_label())

    def lstrip(self):
        """Remove leading whitespace. Equivalent to ``str.lstrip()``."""
        return string_function(self._function_input(), ScalarFunction.LTRIM, 'Lstrip ' + self._function_label())

    def rstrip(self):
        """Remove trailing whitespace. Equivalent to ``str.rstrip()``."""
        return string_function(self._function_input(), ScalarFunction.RTRIM, 'Rstrip ' + self._function_label())

    def length(self):
        """Return the number of characters. Equivalent to ``len(s)``."""
        return integer_function(self._function_input(), ScalarFunction.LENGTH, 'Length ' + self._function_label())

    def reverse(self):
        """Return the value with characters in reverse order. Equivalent to ``s[::-1]``."""
        return string_function(self._function_input(), ScalarFunction.REVERSE, 'Reverse ' + self._function_label())

    def left(self, n: int):
        """Return the first ``n`` characters. Equivalent to ``s[:n]``."""
        return string_function(self._function_input(), ScalarFunction.LEFT, 'Left ' + self._function_label(), extra_args=[n])

    def right(self, n: int):
        """Return the last ``n`` characters. Equivalent to ``s[-n:]``."""
        return string_function(self._function_input(), ScalarFunction.RIGHT, 'Right ' + self._function_label(), extra_args=[n])

    def repeat(self, n: int):
        """Return the value repeated ``n`` times. Equivalent to ``s * n``."""
        return string_function(self._function_input(), ScalarFunction.REPEAT, 'Repeat ' + self._function_label(), extra_args=[n])

    def __mul__(self, n: int):
        """Repeat the value ``n`` times. Equivalent to ``repeat(n)``."""
        return self.repeat(n)

    def replace(self, from_str: str, to_str: str):
        """Replace all occurrences of ``from_str`` with ``to_str``. Equivalent to ``str.replace()``."""
        return string_function(self._function_input(), ScalarFunction.REPLACE, 'Replace ' + self._function_label(), extra_args=[from_str, to_str])

    def substring(self, start: int, length: int | None = None):
        """Return a substring using 0-based ``start`` index.

        ``substring(start)`` returns from ``start`` to end of string.
        ``substring(start, length)`` returns ``length`` characters from ``start``.
        Equivalent to ``s[start:]`` and ``s[start:start+length]`` respectively.
        """
        args = [start + 1] if length is None else [start + 1, length]
        return string_function(self._function_input(), ScalarFunction.SUBSTRING, 'Substring ' + self._function_label(), extra_args=args)

    def __getitem__(self, key):
        """Support Python slice syntax to produce string operations.

        Supported forms::

            s[:n]     → left(n)
            s[-n:]    → right(n)
            s[::-1]   → reverse()
            s[start:] → substring(start)
            s[a:b]    → substring(a, b - a)
        """
        if not isinstance(key, slice):
            raise TypeError("StringAttribute only supports slice indexing")
        start, stop, step = key.start, key.stop, key.step
        if step == -1 and start is None and stop is None:
            return self.reverse()
        if step is not None and step != 1:
            raise ValueError(f"Unsupported slice step: {step}")
        if start is not None and start < 0 and stop is None:
            return self.right(-start)
        if start is None and stop is not None and stop >= 0:
            return self.left(stop)
        if start is not None and start >= 0:
            if stop is None:
                return self.substring(start)
            if stop > start:
                return self.substring(start, stop - start)
        raise ValueError(f"Unsupported slice: [{start}:{stop}:{step}]")


class NumericFunctions(Generic[N]):
    """Functions of a number. The user of this class provides ``_function_input()``, ``_function_label()`` and
    ``_number_type``, the operation that holds a result of the same type as the input: ``abs``, ``ceil``, ``floor``,
    ``mod`` and ``round`` keep an integer an integer. ``sqrt``, ``power``, the logarithm, exponential and the
    trigonometric functions give a floating-point number, as SQL does."""

    if TYPE_CHECKING:  # provided by the class this is mixed into; declared here only, so it does not hide the real one
        def _function_input(self) -> Any: ...
        def _function_label(self) -> str: ...
        _number_type: Callable[..., N]

    def _same_type(self, function: ScalarFunction, label: str, second_arg: int | float | None = None) -> N:
        return self._number_type(self._function_input(), function, label, second_arg=second_arg)

    def abs(self):
        """Return the absolute value. Equivalent to ``abs(n)``."""
        return self._same_type(ScalarFunction.ABS, 'Abs ' + self._function_label())

    def ceil(self):
        """Return the smallest integer >= the value. Equivalent to ``math.ceil(n)``."""
        return self._same_type(ScalarFunction.CEILING, 'Ceil ' + self._function_label())

    def floor(self):
        """Return the largest integer <= the value. Equivalent to ``math.floor(n)``."""
        return self._same_type(ScalarFunction.FLOOR, 'Floor ' + self._function_label())

    def sqrt(self):
        """Return the square root. Equivalent to ``math.sqrt(n)``."""
        return double_function(self._function_input(), ScalarFunction.SQRT, 'Sqrt ' + self._function_label())

    def sin(self):
        """Return the sine of the value (in radians)."""
        return double_function(self._function_input(), ScalarFunction.SIN, 'Sin ' + self._function_label())

    def cos(self):
        """Return the cosine of the value (in radians)."""
        return double_function(self._function_input(), ScalarFunction.COS, 'Cos ' + self._function_label())

    def tan(self):
        """Return the tangent of the value (in radians)."""
        return double_function(self._function_input(), ScalarFunction.TAN, 'Tan ' + self._function_label())

    def asin(self):
        """Return the inverse sine, in radians."""
        return double_function(self._function_input(), ScalarFunction.ASIN, 'Asin ' + self._function_label())

    def acos(self):
        """Return the inverse cosine, in radians."""
        return double_function(self._function_input(), ScalarFunction.ACOS, 'Acos ' + self._function_label())

    def atan(self):
        """Return the inverse tangent, in radians."""
        return double_function(self._function_input(), ScalarFunction.ATAN, 'Atan ' + self._function_label())

    def atan2(self, x: int | float):
        """Return atan2(value, x), in radians."""
        return double_function(self._function_input(), ScalarFunction.ATAN2, 'Atan2 ' + self._function_label(), second_arg=x)

    def exp(self):
        """Return e raised to the value."""
        return double_function(self._function_input(), ScalarFunction.EXP, 'Exp ' + self._function_label())

    def log(self):
        """Return the natural logarithm of the value."""
        return double_function(self._function_input(), ScalarFunction.LOG, 'Log ' + self._function_label())

    def mod(self, n: int):
        """Return the remainder after division by ``n``. Equivalent to ``value % n``."""
        return self._same_type(ScalarFunction.MOD, 'Mod ' + self._function_label(), second_arg=n)

    def __mod__(self, n: int):
        """Return the remainder after division by ``n``. Equivalent to ``mod(n)``."""
        return self.mod(n)

    def power(self, n: int):
        """Return the value raised to the power of ``n``. Equivalent to ``value ** n``."""
        return double_function(self._function_input(), ScalarFunction.POWER, 'Power ' + self._function_label(), second_arg=n)

    def __pow__(self, n: int):
        """Return the value raised to the power of ``n``. Equivalent to ``power(n)``."""
        return self.power(n)

    def round(self, d: int | None = None):
        """Round to ``d`` decimal places. Equivalent to ``round(value, d)``.

        When ``d`` is omitted, rounds to the nearest integer.
        """
        return self._same_type(ScalarFunction.ROUND, 'Round ' + self._function_label(), second_arg=d)


def _label_of(operation: ScalarFunctionOperation) -> str:
    return operation.display_name or operation.function.name.title()


class _Result:
    """What a function result gives its own functions: it is their input, and their label is its own name."""

    def _function_input(self):
        return self

    def _function_label(self) -> str:
        return _label_of(self)  # type: ignore[arg-type]


class StringFunctionOperation(_Result, StringFunctions, StringPredicates, Equatable[str], ScalarFunctionOperation):
    """A function that gives text, which has the string functions too: ``f.name().upper().length()``. It is compared
    for equality with text, as a string column is, and has no order."""

    def __add__(self, other):
        return concat(self, other)

    def __radd__(self, other):
        return concat(other, self)


class IntegerFunctionOperation(_Result, NumericFunctions['IntegerFunctionOperation'], Arithmetic, Orderable[int],
                               ScalarFunctionOperation):
    """A function that gives a whole number (``length``, or ``abs`` of an integer): compared with integers."""


class DoubleFunctionOperation(_Result, NumericFunctions['DoubleFunctionOperation'], Arithmetic, Orderable[float],
                              ScalarFunctionOperation):
    """A function that gives a floating-point number (``sqrt``, ``power``, or ``abs`` of a double)."""


class DecimalFunctionOperation(_Result, NumericFunctions['DecimalFunctionOperation'], Arithmetic,
                               Orderable[decimal.Decimal], ScalarFunctionOperation):
    """A function that gives a decimal (``abs`` of a decimal column)."""


IntegerFunctionOperation._number_type = IntegerFunctionOperation  # type: ignore[attr-defined]
DoubleFunctionOperation._number_type = DoubleFunctionOperation  # type: ignore[attr-defined]
DecimalFunctionOperation._number_type = DecimalFunctionOperation  # type: ignore[attr-defined]
