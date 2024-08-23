import sqlalchemy as db


def sql_alchemy_demo():

    engine = db.create_engine("sqlite:///test.sqlite")

    conn = engine.connect()


if __name__ == '__main__':
    sql_alchemy_demo()