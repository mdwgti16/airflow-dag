from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'Airflow',
}

from time import sleep

def process(p1):
    print(p1)
    print(pd.read_csv('/opt/airflow/files/test.txt'))
    sleep(60*60*5)
    return 'done'


with DAG(dag_id='test3', schedule_interval='0 19 * * *', default_args=default_args, catchup=False) as dag:
    task_4 = PythonOperator(task_id='task_4', python_callable=process, op_args=['my super parameter'])

    task_4
