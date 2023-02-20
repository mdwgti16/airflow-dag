# import subprocess
# from time import sleep
#
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
#
#
# from datetime import datetime, timedelta
#
# default_args = {
#     # 'retries': 3,
#     # 'retry_delay': timedelta(minutes=5),
#     'start_date': datetime(2019, 1, 1),
#     'owner': 'Airflow',
# }
#
# def process(p1):
#     # cmd = 'java -DSpring.batch.job.names=DetailJob -Dacq.collectSite=lg.dns-shop.ru -Dmongodb.url=mongodb://acq:acq12345@10.98.192.100:27017/acq.acqlog?authSource=acq -Dmariadb.admin.url=jdbc:mariadb://10.103.220.109:3306/acq -jar /opt/airflow/files/application-0.0.1-SNAPSHOT.jar'
#     # cmd = 'java -DSpring.batch.job.names=DetailJob -Dacq.collectSite=top.naverstore.com -Dmongodb.url=mongodb://acq:acq12345@10.98.30.157:27017/acq.acqlog?authSource=acq -Dmariadb.admin.url=jdbc:mariadb://10.103.220.109:3306/acq -jar /opt/nfs/files/application-0.0.1-SNAPSHOT.jar'
#     cmd = 'java -DSpring.batch.job.names=DetailJob -Dacq.collectSite=lg.datart.cz -Dmongodb.url=mongodb://acq:acq12345@10.98.30.157:27017/acq.acqlog?authSource=acq -Dmariadb.admin.url=jdbc:mariadb://10.103.220.109:3306/acq -jar /opt/nfs/files/application-0.0.1-SNAPSHOT.jar'
#     print(cmd)
#     subprocess.run(cmd.split(' '))
#
#     return 'done'
#
#
# with DAG(dag_id='acq_detail_scheduler',
#          schedule_interval='0 1 * * *',
#          default_args=default_args,
#          catchup=False,
#          max_active_runs=1,
#          on_failure_callback=None) as dag:
#     start = EmptyOperator(task_id='start')
#
#     run = KubernetesPodOperator(
#         task_id="acq-scheduler-java",
#         namespace='default',
#         image='test/image',
#         secrets=[
#             env
#         ],
#         image_pull_secrets=[k8s.V1LocalObjectReference('image_credential')],
#         name="job",
#         is_delete_operator_pod=True,
#         get_logs=True,
#         resources=pod_resources,
#         env_from=configmaps,
#         dag=dag,
#     )
#
#     k = KubernetesPodOperator(
#         name="hello-dry-run",
#         image="debian",
#         cmds=["bash", "-cx"],
#         arguments=["echo", "10"],
#         labels={"foo": "bar"},
#         task_id="dry_run_demo",
#         do_xcom_push=True,
#     )
#
# k.dry_run()
#     detail = PythonOperator(task_id='detail', python_callable=process, op_args=['my super parameter'])
#
#     start >> detail
