"""
Ported from legend-engine core_relational ``tests/advanced/testContractMoneyScenario.pure``.

Recorded as unsupported: the finder cannot express these yet.
Mapping: tests/simple_mapping.md.
"""
from datafinder_examples_tests.spec import FinderSpec

from datafinder_legend_tests.legend import ported, unsupported

_SRC = "tests/advanced/testContractMoneyScenario.pure"


CONTRACT_MONEY_SCENARIO_SPECS = FinderSpec(
    finder_name="PersonFinder",
    mapping_file="tests/simple_mapping.md",
    expectations=[
        unsupported("test1", f"{_SRC}::test1",
                    'select "root".id as "id", "root".price as "amount", "fx_0".rate as "rate", case when "currency_0".value = \'USD\' then "root".price else ("root".price * "fx_0".rate) end as "value" from Contract as "root" left outer join Currency as "currency_0" on ("root".id = "currency_0".contractId) left outer join FX as "fx_0" on ("root".currency = "fx_0".currency and ("fx_0".date = \'2003-10-10\' and "fx_0".tenor = 1))',
                    'a scenario with money and contract classes'),
        unsupported("test2", f"{_SRC}::test2",
                    'select "root".id as "id", "root".price as "amount", "fx_0".rate as "rate", case when "currency_0".value = \'USD\' then "root".price else ("root".price * "fx_0".rate) end as "value" from Contract as "root" left outer join Currency as "currency_0" on ("root".id = "currency_0".contractId) left outer join FX as "fx_0" on ("root".currency = "fx_0".currency and ("fx_0".date = \'2003-10-10\' and "fx_0".tenor = 1))',
                    'a scenario with money and contract classes'),
    ],
)
