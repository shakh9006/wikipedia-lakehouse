
FROM apache/airflow:3.0.4

COPY requirements.txt /requirements.txt

USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ARG AIRFLOW_VERSION=3.0.4
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"

# Copy any additional files if needed (uncomment as needed)
# COPY ./dags /opt/airflow/dags
# COPY ./plugins /opt/airflow/plugins
# COPY ./config /opt/airflow/config
