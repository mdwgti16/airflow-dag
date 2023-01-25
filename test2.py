from datetime import datetime

import telegram
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

dag = DAG('hello_world', description='Hello World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)


def hello(**kwargs):
    bot = telegram.Bot(token='5374963190:AAF5tIr3I3UGjzqwainq9B-UHsv-szscVvY')
    bot.sendMessage(chat_id='-712874164', text="start")
    print('Hello from {kw}'.format(kw=kwargs['my_keyword']))


with DAG('hello_world', description='Hello World DAG',
        schedule_interval='0 12 * * *',
        start_date=datetime(2017, 3, 20), catchup=False) as dag:

    hello_operator = PythonOperator(task_id='hello_task',
                                    python_callable=hello,
                                    dag=dag,
                                    op_kwargs={'my_keyword': 'Airflow'}
                                    )

    hello_operator
