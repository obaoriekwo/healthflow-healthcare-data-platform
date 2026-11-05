# Pinned versions: change them together, never independently (see docs/decisions.md, ADR-005).
ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12

COPY requirements-ingestion.txt requirements-airflow.txt requirements-dbt.txt /tmp/

# dbt lives in its own virtual environment: its dependencies conflict with Airflow's.
# Cosmos calls it through DBT_EXECUTABLE.
USER root
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
 && chown -R airflow: /opt/dbt-venv
USER airflow

# Ingestion code and Cosmos go into Airflow's environment, installed against Airflow's
# official constraints file so nothing Airflow depends on can be upgraded.
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements-ingestion.txt \
      -r /tmp/requirements-airflow.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
