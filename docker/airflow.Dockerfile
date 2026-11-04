# Pinned versions: change them together, never independently (see docs/decisions.md, ADR-005).
ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION=3.2.2
ARG PYTHON_VERSION=3.12

COPY requirements-ingestion.txt /tmp/requirements-ingestion.txt

# Install against Airflow's official constraints file so our packages cannot
# upgrade anything Airflow depends on.
RUN pip install --no-cache-dir \
      -r /tmp/requirements-ingestion.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
