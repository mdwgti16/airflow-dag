import subprocess
from time import sleep

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2019, 1, 1),
    'owner': 'Airflow',
}


def process(p1):
    cmd = 'java -DSpring.batch.job.names=DetailJob -Dacq.collectSite=top.naverstore.com -Dmongodb.url=mongodb://acq:acq12345@10.98.192.100:27017/acq.acqlog?authSource=acq -Dmariadb.admin.url=jdbc:mariadb://10.103.220.109:3306/acq -jar /opt/airflow/files/application-0.0.1-SNAPSHOT.jar'
    print(cmd)
    subprocess.run(cmd.split(' '))
    sleep(60*60*5)

    return 'done'

with DAG(dag_id='detail_scheduler', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    detail = PythonOperator(task_id='detail', python_callable=process, op_args=['my super parameter'])
    detail
