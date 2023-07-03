import pykx as kx


def pykx_demo():
    #Embedded mode
    print(kx.q('1+1'))

    #IPC mode
    conn = kx.QConnection('localhost', 5001)
    qvec = conn('2+til 2')
    print(qvec)
    count = conn('count trade')
    print(count)
    #requires kdb insights
    #res = conn.sql('select top 5 from trade')
    #print(res)
    res = conn.qsql.select('trade')
    print(res)
    res = conn.qsql.select('trade', ['sym','price'])
    print(res)
    res = conn.qsql.select('trade', ['sym', 'price'], 'sym=`AMZN')
    print(res)

if __name__ == '__main__':
    pykx_demo()