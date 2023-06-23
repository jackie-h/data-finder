import sqlalchemy as db




def sql_alchemy_demo():

    engine = db.create_engine("sqlite:///test.sqlite")

    conn = engine.connect()
    #conn = kx.QConnection('localhost', 5000)
    #qvec = conn('2+til 2')
    #kx.q.sql()
    print(kx.q('1+1'))


if __name__ == '__main__':
    sql_alchemy_demo()