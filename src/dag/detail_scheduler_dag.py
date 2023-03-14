import subprocess
from cron_converter import Cron
from datetime import datetime

from time import sleep

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

from src.common.db.mariadb import *


def process(p1):
    # cmd = 'java -DSpring.batch.job.names=DetailJob -Dacq.collectSite=lg.dns-shop.ru -Dmongodb.url=mongodb://acq:acq12345@10.98.192.100:27017/acq.acqlog?authSource=acq -Dmariadb.admin.url=jdbc:mariadb://10.103.220.109:3306/acq -jar /opt/airflow/files/application-0.0.1-SNAPSHOT.jar'
    # cmd = 'java -DSpring.batch.job.names=DetailJob -Dacq.collectSite=top.naverstore.com -Dmongodb.url=mongodb://acq:acq12345@10.98.30.157:27017/acq.acqlog?authSource=acq -Dmariadb.admin.url=jdbc:mariadb://10.103.220.109:3306/acq -jar /opt/nfs/files/application-0.0.1-SNAPSHOT.jar'
    cmd = 'java -DSpring.batch.job.names=DetailJob -Dacq.collectSite=lg.datart.cz -Dmongodb.url=mongodb://acq:acq12345@10.98.30.157:27017/acq.acqlog?authSource=acq -Dmariadb.admin.url=jdbc:mariadb://10.103.220.109:3306/acq -jar /opt/nfs/files/application-0.0.1-SNAPSHOT.jar'
    print(cmd)
    subprocess.run(cmd.split(' '))

    return 'done'


# with DAG(dag_id='detail_scheduler', schedule_interval='0 1 * * *', default_args=default_args, catchup=False) as dag:
#
#     detail = PythonOperator(task_id='detail', python_callable=process, op_args=['my super parameter'])
#
#     detail

def get_cron_interval(interval):
    cron = Cron(interval)
    schedule = cron.schedule(start_date=datetime.now())
    n1 = schedule.next()
    n2 = schedule.next()

    int((n2 - n1).total_seconds() / 60)


def airflow_failed_callback(context):
    # message 작성
    message = """
            :red_circle: Task Failed.
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {exec_date}
            *Exception*: {exception}
            *Log Url*: {log_url}
            """.format(
        dag=context.get('task_instance').dag_id,
        task=context.get('task_instance').task_id,
        exec_date=context.get('execution_time'),
        exception=context.get('exception'),
        log_url=context.get('task_instance').log_url
    )
    print('Airflow', message)


def hello_world_py(dag_id):
    print("This is DAG: {}".format(str(dag_id)))


def create_dag(dag_id, interval, default_args):
    dag = DAG(dag_id=dag_id,
              schedule_interval=interval,
              default_args=default_args,
              catchup=False,
              # max_active_runs=1,
              on_failure_callback=airflow_failed_callback)

    with dag:
        t1 = PythonOperator(task_id="hello_world",
                            python_callable=hello_world_py,
                            op_kwargs={'dag_id': dag_id})
    return dag


for interval in acq_interval():
    dag_name = 'ACQ_DETAIL_SCHEDULER'
    dag_id = f'{dag_name}_{str(get_cron_interval(interval))}'
    default_args = {
        # 'retries': 3,
        # 'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2019, 1, 1),
        'owner': 'Airflow',
    }

    globals()[dag_id] = create_dag(dag_id, interval, default_args)
