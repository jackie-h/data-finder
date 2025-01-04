from m3 import Class, Property, String, Float, Package, Integer
from relational import Column, Table, RelationalClassMapping, RelationalPropertyMapping

def create_account_class() -> Class:
    p1 = Property('id', Integer)
    p2 = Property('name', String)

    account_c = Class('Account', [p1, p2], Package('finance'))
    return account_c


def create_trade_class(account:Class) -> Class:
    p1 = Property('symbol', String)
    p2 = Property('price', Float)
    p3 = Property('account', account)

    trade_c = Class('Trade', [p1, p2, p3], Package('finance'))
    return trade_c


def create_mappings() -> list[RelationalClassMapping]:
    account_c = create_account_class()
    trade_c = create_trade_class(account_c)

    c1 = Column('id', 'INT')
    c2 = Column('account_id', 'INT')
    c3 = Column('sym', 'VARCHAR')
    c4 = Column('price', 'DOUBLE')

    trade_t = Table('trade', [c1, c2, c3, c4])

    pm1 = RelationalPropertyMapping(trade_c.property('symbol'), c3)
    pm2 = RelationalPropertyMapping(trade_c.property('price'), c4)
    pm3 = RelationalPropertyMapping(trade_c.property('account'),c2)
    rm_t = RelationalClassMapping(trade_c, [pm1, pm2, pm3])

    ac1 = Column('id', 'INT')
    ac2 = Column('name', 'VARCHAR')
    account_t = Table('account', [ac1, ac2])

    a_pm1 = RelationalPropertyMapping(account_c.property('id'), ac1)
    a_pm2 = RelationalPropertyMapping(account_c.property('name'), ac2)
    rm_a = RelationalClassMapping(account_c, [a_pm1, a_pm2])

    return [rm_t,rm_a]
