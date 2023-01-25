from datetime import datetime

import telegram
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


def hello(**kwargs):
    bot = telegram.Bot(token='5374963190:AAF5tIr3I3UGjzqwainq9B-UHsv-szscVvY')
    bot.sendMessage(chat_id='-712874164', text="start")
    print('Hello from {kw}'.format(kw=kwargs['my_keyword']))


# with DAG('hello_world', description='Hello World DAG',
#         schedule_interval='0 12 * * *',
#         start_date=datetime(2017, 3, 20), catchup=False) as dag:
#
#     hello_operator = PythonOperator(task_id='hello_task',
#                                     python_callable=hello,
#                                     dag=dag,
#                                     op_kwargs={'my_keyword': 'Airflow'}
#                                     )
#
#     hello_operator


default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'Airflow',
}


# def process(p1):
#     print(p1)
#     return 'done'


with DAG(dag_id='parallel_dag_test', schedule_interval='0 12 * * *', default_args=default_args, catchup=False) as dag:
    # Tasks dynamically generated
    tasks = [BashOperator(task_id='task_{0}'.format(t), bash_command='sleep 5'.format(t)) for t in range(1, 4)]

    task_4 = PythonOperator(task_id='task_4',
                                    python_callable=hello,
                                    dag=dag,
                                    op_kwargs={'my_keyword': 'Airflow'}
                                    )

    task_5 = BashOperator(task_id='task_5', bash_command='echo "pipeline done"')

    task_6 = BashOperator(task_id='task_6', bash_command='sleep 5')

    tasks >> task_4 >> task_5 >> task_6
