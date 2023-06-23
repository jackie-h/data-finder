import pykx as kx


def pykx_demo():
    #conn = kx.QConnection('localhost', 5000)
    #qvec = conn('2+til 2')
    #kx.q.sql()
    print(kx.q('1+1'))


if __name__ == '__main__':
    pykx_demo()