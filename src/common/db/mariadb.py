from sqlalchemy import create_engine


def acq_engine():
    # return create_engine(f'mariadb+pymysql://acq:acq00!q@103.218.158.204:3306/acq', pool_recycle=3600)
    return create_engine(f'mariadb+pymysql://acq:acq12345@10.103.220.109:3306/acq', pool_recycle=3600)

