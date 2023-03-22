FROM apache/airflow:2.5.1-python3.8

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir python-telegram-bot pymysql cron-converter

USER root
RUN apt-get update
RUN apt-get install -y wget apt-transport-https gnupg
RUN wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | apt-key add -
RUN echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list
RUN apt-get update
RUN apt-get install -y temurin-11-jdk

USER airflow