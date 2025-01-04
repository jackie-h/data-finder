from m3 import Class, Property, String, Float, Package
from relational import Column, Table, RelationalClassMapping, RelationalPropertyMapping


def create():

    p1 = Property('symbol', String)
    p2 = Property('price', Float)

    trade_c = Class('Trade',[p1,p2], Package('finance'))

    c1 = Column('id', 'INT')
    c2 = Column('account_id', 'INT')
    c3 = Column('sym', 'VARCHAR')
    c4 = Column('price', 'DOUBLE')

    trade_t = Table('trade', [c1,c2,c3,c4])

    pm1 = RelationalPropertyMapping(p1, c3)
    pm2 = RelationalPropertyMapping(p2, c4)
    rm = RelationalClassMapping(trade_c, [pm1,pm2])

    #CREATE TABLE trade(id INT, account_id INT, sym VARCHAR, price DOUBLE);