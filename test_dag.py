from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'Airflow',
}

def process(p1):
    print(p1)
    return 'done'

with DAG(dag_id='parallel_dag_test', schedule_interval='/5 * * * *', default_args=default_args, catchup=False) as dag:

    # Tasks dynamically generated
    tasks = [BashOperator(task_id='task_{0}'.format(t), bash_command='sleep 10'.format(t)) for t in range(1, 5)]

    # task_4 = PythonOperator(task_id='task_4', python_callable=process, op_args=['my super parameter'])

    task_bash = BashOperator(task_id='task_bash', bash_command='echo "pipeline done"')

    # task_6 = BashOperator(task_id='task_6', bash_command='sleep 60')

    # tasks >> task_4 >> task_5 >> task_6
    tasks >> task_bash
