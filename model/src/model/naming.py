import builtins
import keyword
import re

from model.m3 import Property

_BUILTIN_NAMES = set(dir(builtins))


def to_snake_case(name: str) -> str:
    """snake_case of a camelCase identifier or a display name: camelCase humps and runs of
    non-alphanumeric characters each become a single underscore."""
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    s = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', s)
    return re.sub(r'[^0-9a-zA-Z]+', '_', s).strip('_').lower()


def to_python_name(prop: Property) -> str:
    """The Python identifier for a property: its name in snake_case, with a trailing underscore if it
    would shadow a keyword or builtin."""
    name = to_snake_case(prop.name)
    if keyword.iskeyword(name) or name in _BUILTIN_NAMES:
        name += '_'
    return name
