import os

from datafinder_generator.generator import generate
from model.m3 import Class, Property, String, Float, Package, Integer, Date
from model.mapping import Mapping
from model.relational import Column, Table, RelationalClassMapping, RelationalPropertyMapping, Join


def create_account_class() -> Class:
    p1 = Property('id', Integer)
    p2 = Property('name', String)

    account_c = Class('Account', [p1, p2], Package('finance'))
    return account_c


def create_instrument_class() -> Class:
    p1 = Property('symbol', String)
    #TODO this doesn't belong here, but using for simple example
    p2 = Property('price', Float)

    instrument_c = Class('Instrument', [p1, p2], Package('finance'))
    return instrument_c


def create_trade_class(account:Class) -> Class:
    p1 = Property('symbol', String)
    p2 = Property('price', Float)
    p3 = Property('account', account)

    trade_c = Class('Trade', [p1, p2, p3], Package('finance'))
    return trade_c


def create_contractual_position_class(instrument:Class) -> Class:
    p1 = Property('date', Date)
    p2 = Property('quantity', Float)
    p3 = Property('counterparty', Integer)
    p4 = Property('instrument', instrument)
    p5 = Property('npv', Float)
    pos_c = Class('ContractualPosition', [p1, p2, p3, p4, p5], Package('finance'))
    return pos_c


def create_mappings_normalized() -> Mapping:
    account_c = create_account_class()

    ac1 = Column('id', 'INT')
    ac2 = Column('name', 'VARCHAR')
    account_t = Table('account', [ac1, ac2])

    instrument_c = create_instrument_class()
    ic1 = Column('SYM', 'VARCHAR')
    ic2 = Column('PRICE', 'DOUBLE')
    instrument_t = Table('price', [ic1,ic2])

    c_position_c = create_contractual_position_class(instrument_c)
    p1 = Column('DATE', 'DATE')
    p2 = Column('INSTRUMENT', 'VARCHAR')
    p3 = Column('CPTY_ID', 'INT')
    p4 = Column('QUANTITY', 'DOUBLE')
    p5 = Column('NPV', 'DOUBLE')
    pos_t = Table('contractualposition', [p1, p2, p3, p4, p5])

    trade_c = create_trade_class(account_c)

    c1 = Column('id', 'INT')
    c2 = Column('account_id', 'INT')
    c3 = Column('sym', 'VARCHAR')
    c4 = Column('price', 'DOUBLE')

    trade_t = Table('trade', [c1, c2, c3, c4])

    pm1 = RelationalPropertyMapping(trade_c.property('symbol'), c3)
    pm2 = RelationalPropertyMapping(trade_c.property('price'), c4)
    pm3 = RelationalPropertyMapping(trade_c.property('account'),Join(c2,ac1))
    rm_t = RelationalClassMapping(trade_c, [pm1, pm2, pm3])

    a_pm1 = RelationalPropertyMapping(account_c.property('id'), ac1)
    a_pm2 = RelationalPropertyMapping(account_c.property('name'), ac2)
    rm_a = RelationalClassMapping(account_c, [a_pm1, a_pm2])

    i_pm1 = RelationalPropertyMapping(instrument_c.property('symbol'), ic1)
    i_pm2 = RelationalPropertyMapping(instrument_c.property('price'), ic2)
    rm_i = RelationalClassMapping(instrument_c, [i_pm1, i_pm2])


    cpm1 = RelationalPropertyMapping(c_position_c.property('quantity'), p4)
    cpm2 = RelationalPropertyMapping(c_position_c.property('counterparty'), p3)
    cpm3 = RelationalPropertyMapping(c_position_c.property('instrument'), Join(p2,ic1))
    cpm4 = RelationalPropertyMapping(c_position_c.property('npv'), p5)
    rm_cp = RelationalClassMapping(c_position_c, [cpm1, cpm2, cpm3, cpm4])

    return Mapping('Test Mapping 1', [rm_t,rm_a,rm_i,rm_cp])


def generate_mappings():
    rcms = create_mappings_normalized()
    import sys
    mn = sys.modules[__name__]
    directory = os.path.dirname(mn.__file__)
    generate(rcms, directory)


if __name__ == '__main__':
    generate_mappings()
