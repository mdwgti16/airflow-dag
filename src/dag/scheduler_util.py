from cron_converter import Cron
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


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


def get_cron_interval(schedule_interval):
    cron = Cron(schedule_interval)
    schedule = cron.schedule(start_date=datetime.now())
    n1 = schedule.next()
    n2 = schedule.next()

    return int((n2 - n1).total_seconds() / 60)


def get_cron_time_period(schedule_interval):
    cron = Cron(schedule_interval)
    schedule = cron.schedule(start_date=datetime.now())
    n1 = schedule.prev()
    n2 = schedule.next()

    return n1, n2


def get_cron_time_period_with_format(schedule_interval):
    start_time, end_time = get_cron_time_period(schedule_interval)
    fmt = '%Y-%m-%d/%H:%M:%S'

    return start_time.strftime(fmt), end_time.strftime(fmt)


def create_task(r, job_names, start_time, end_time, log_file_prefix, api_path):
    collect_site = r.COLLECT_SITE
    sub_site = r.SUB_SITE

    return BashOperator(
        task_id=f"{collect_site}__{sub_site}__{get_cron_interval(r['SCHEDULE_INTERVAL'])}",
        bash_command=f"java -DSpring.batch.job.names={job_names} \
        -Dacq.collectSite={collect_site} -Dacq.subSite={sub_site} -Dacq.type={r.TYPE} \
        {api_path} \
        -Dmongodb.url=mongodb://acq:acq12345@10.98.30.157:27017/acq.acqlog?authSource=acq \
        -Dmariadb.admin.url=jdbc:mariadb://10.103.220.109:3306/acq -DlogFilePrefix={log_file_prefix} \
        -Dlogging.config=/opt/nfs/files/log4j2.xml -jar /opt/nfs/files/application-0.0.1-SNAPSHOT.jar {start_time} {end_time}"
    )
