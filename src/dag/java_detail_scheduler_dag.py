import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from src.common.db.mariadb import *
from src.dag.scheduler_util import *

ENV = 'JAVA'


def create_dag(dag_id, interval, default_args):
    dag = DAG(dag_id=dag_id,
              schedule_interval=interval,
              default_args=default_args,
              catchup=False,
              # max_active_runs=1,
              on_failure_callback=airflow_failed_callback)

    acq_tasks = acq_detail_task(ENV, interval)

    if acq_tasks is not None and len(acq_tasks) > 0:
        start_time, end_time = get_cron_time_period_with_format(interval)
        with dag:
            start = EmptyOperator(task_id="java_detail_start")
            tasks = acq_tasks.apply(create_task, job_names="JavaDetailJob", start_time=start_time, end_time=end_time,
                                    log_file_prefix=dag_id + '_',
                                    api_path='-Dacq.node.detailApiUrl=http://10.109.170.130:8013/acq/java/detail',
                                    axis=1).tolist()
            end = EmptyOperator(task_id="detail_end")

            start >> tasks >> end

        return dag


for interval in acq_detail_interval(ENV):
    dag_name = 'ACQ_JAVA_DETAIL_SCHEDULER'
    dag_id = f'{dag_name}_{str(get_cron_interval(interval))}'
    default_args = {
        # 'retries': 3,
        # 'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2019, 1, 1),
        'owner': 'Airflow',
    }

    dags = create_dag(dag_id, interval, default_args)
    if dags:
        globals()[dag_id] = dags
