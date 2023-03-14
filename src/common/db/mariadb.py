import pandas as pd
from sqlalchemy import create_engine
from sys import platform


def acq_engine():
    if platform == "linux" or platform == "linux2":
        return create_engine(f'mariadb+pymysql://acq:acq00!q@10.103.220.109:3306/acq', pool_recycle=3600)
    else:
        return create_engine(f'mariadb+pymysql://acq:acq00!q@103.218.158.204:3306/acq', pool_recycle=3600)


def acq_service(env=None, interval=None):
    return pd.read_sql(f'''
        SELECT * 
        FROM SERVICE
        WHERE 1=1
            {f"AND SCHEDULE_INTERVAL = '{interval}'" if interval else ''}
            {f"AND ENV = '{env}'" if env else ''}
    ''', acq_engine())


def acq_detail_interval(env):
    return acq_service(env=env)['SCHEDULE_INTERVAL'].unique()


def acq_detail_task(env, interval):
    acq_svc = acq_service(env, interval).drop_duplicates(['COLLECT_SITE', 'SUB_SITE', 'TYPE'])

    site_interval = acq_svc.groupby(['COLLECT_SITE', 'SUB_SITE']).agg({
        'COLLECT_SITE': min,
        'SUB_SITE': min,
        'SCHEDULE_INTERVAL': min,
        'TYPE': ','.join
    })

    return site_interval.reset_index(drop=True)

