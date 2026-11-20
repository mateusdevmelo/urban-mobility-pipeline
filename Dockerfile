FROM apache/airflow:2.10.4-python3.12

ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.12

USER root

# Java 17 necessário para o PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Cria ambiente virtual isolado para o dbt
RUN python -m venv /opt/dbt-venv \
    && chown -R airflow:root /opt/dbt-venv

COPY requirements-airflow.txt /tmp/requirements-airflow.txt
COPY requirements-dbt.txt /tmp/requirements-dbt.txt

USER airflow

# Dependências do pipeline no ambiente do Airflow
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/requirements-airflow.txt

# dbt em ambiente Python separado
RUN /opt/dbt-venv/bin/pip install --no-cache-dir \
    -r /tmp/requirements-dbt.txt

ENV DBT_EXECUTABLE=/opt/dbt-venv/bin/dbt
ENV SPARK_LOCAL_DIR=/tmp/spark-local
ENV PYTHONUNBUFFERED=1
