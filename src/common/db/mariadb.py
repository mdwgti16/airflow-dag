import pandas as pd
from sqlalchemy import create_engine
from sys import platform


def acq_engine():
    if platform == "linux" or platform == "linux2":
        return create_engine(f'mariadb+pymysql://acq:acq00!q@10.103.220.109:3306/acq', pool_recycle=3600)
    else:
        return create_engine(f'mariadb+pymysql://acq:acq00!q@103.218.158.204:3306/acq', pool_recycle=3600)


def acq_service():
    return pd.read_sql('SELECT * FROM SERVICE', acq_engine())


def acq_interval():
    return acq_service()['SCHEDULE_INTERVAL'].unique()
