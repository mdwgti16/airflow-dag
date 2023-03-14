FROM apache/airflow:2.5.1-python3.8

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir python-telegram-bot pymysql
