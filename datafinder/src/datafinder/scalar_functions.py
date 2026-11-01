"""Scalar functions as methods, shared by attributes and by the results of other functions, so they chain:
``f.name().upper().length()``.

An attribute reads its column and is labelled by its display name; a function result is itself the input and is
labelled by its own name."""
from typing import TYPE_CHECKING, Any

from model.relational import (
    ScalarFunction, ScalarFunctionOperation,
)

def string_function(element, function: ScalarFunction, label: str | None = None, second_arg: int | float | None = None,
                    extra_args: list | None = None) -> 'StringFunctionOperation':
    """A function that gives text, applied to ``element``."""
    return StringFunctionOperation(element, function, label, second_arg=second_arg, extra_args=extra_args)


def number_function(element, function: ScalarFunction, label: str | None = None, second_arg: int | float | None = None,
                    extra_args: list | None = None) -> 'NumericFunctionOperation':
    """A function that gives a number, applied to ``element``."""
    return NumericFunctionOperation(element, function, label, second_arg=second_arg, extra_args=extra_args)


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
        return number_function(self._function_input(), ScalarFunction.LENGTH, 'Length ' + self._function_label())

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


class NumericFunctions:
    """Functions of a number. The user of this class provides ``_function_input()`` and ``_function_label()``."""

    if TYPE_CHECKING:  # provided by the class this is mixed into; declared here only, so it does not hide the real one
        def _function_input(self) -> Any: ...
        def _function_label(self) -> str: ...

    def abs(self):
        """Return the absolute value. Equivalent to ``abs(n)``."""
        return number_function(self._function_input(), ScalarFunction.ABS, 'Abs ' + self._function_label())

    def ceil(self):
        """Return the smallest integer >= the value. Equivalent to ``math.ceil(n)``."""
        return number_function(self._function_input(), ScalarFunction.CEILING, 'Ceil ' + self._function_label())

    def floor(self):
        """Return the largest integer <= the value. Equivalent to ``math.floor(n)``."""
        return number_function(self._function_input(), ScalarFunction.FLOOR, 'Floor ' + self._function_label())

    def sqrt(self):
        """Return the square root. Equivalent to ``math.sqrt(n)``."""
        return number_function(self._function_input(), ScalarFunction.SQRT, 'Sqrt ' + self._function_label())

    def sin(self):
        """Return the sine of the value (in radians)."""
        return number_function(self._function_input(), ScalarFunction.SIN, 'Sin ' + self._function_label())

    def cos(self):
        """Return the cosine of the value (in radians)."""
        return number_function(self._function_input(), ScalarFunction.COS, 'Cos ' + self._function_label())

    def tan(self):
        """Return the tangent of the value (in radians)."""
        return number_function(self._function_input(), ScalarFunction.TAN, 'Tan ' + self._function_label())

    def asin(self):
        """Return the inverse sine, in radians."""
        return number_function(self._function_input(), ScalarFunction.ASIN, 'Asin ' + self._function_label())

    def acos(self):
        """Return the inverse cosine, in radians."""
        return number_function(self._function_input(), ScalarFunction.ACOS, 'Acos ' + self._function_label())

    def atan(self):
        """Return the inverse tangent, in radians."""
        return number_function(self._function_input(), ScalarFunction.ATAN, 'Atan ' + self._function_label())

    def atan2(self, x: int | float):
        """Return atan2(value, x), in radians."""
        return number_function(self._function_input(), ScalarFunction.ATAN2, 'Atan2 ' + self._function_label(), second_arg=x)

    def exp(self):
        """Return e raised to the value."""
        return number_function(self._function_input(), ScalarFunction.EXP, 'Exp ' + self._function_label())

    def log(self):
        """Return the natural logarithm of the value."""
        return number_function(self._function_input(), ScalarFunction.LOG, 'Log ' + self._function_label())

    def mod(self, n: int):
        """Return the remainder after division by ``n``. Equivalent to ``value % n``."""
        return number_function(self._function_input(), ScalarFunction.MOD, 'Mod ' + self._function_label(), second_arg=n)

    def __mod__(self, n: int):
        """Return the remainder after division by ``n``. Equivalent to ``mod(n)``."""
        return self.mod(n)

    def power(self, n: int):
        """Return the value raised to the power of ``n``. Equivalent to ``value ** n``."""
        return number_function(self._function_input(), ScalarFunction.POWER, 'Power ' + self._function_label(), second_arg=n)

    def __pow__(self, n: int):
        """Return the value raised to the power of ``n``. Equivalent to ``power(n)``."""
        return self.power(n)

    def round(self, d: int | None = None):
        """Round to ``d`` decimal places. Equivalent to ``round(value, d)``.

        When ``d`` is omitted, rounds to the nearest integer.
        """
        return number_function(self._function_input(), ScalarFunction.ROUND, 'Round ' + self._function_label(), second_arg=d)


def _label_of(operation: ScalarFunctionOperation) -> str:
    return operation.display_name or operation.function.name.title()


class StringFunctionOperation(StringFunctions, ScalarFunctionOperation):
    """A function that gives text, which has the string functions too: ``f.name().upper().length()``."""

    def _function_input(self):
        return self

    def _function_label(self) -> str:
        return _label_of(self)


class NumericFunctionOperation(NumericFunctions, ScalarFunctionOperation):
    """A function that gives a number, which has the numeric functions and arithmetic too."""

    def _function_input(self):
        return self

    def _function_label(self) -> str:
        return _label_of(self)
