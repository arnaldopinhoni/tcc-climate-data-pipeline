#!/usr/bin/env bash
set -euo pipefail

python -m pip install --no-cache-dir --upgrade "uv>=0.10.0"

cd /opt/airflow
uv sync --frozen --no-dev --no-install-project

airflow db migrate

# Idempotente: se o usuário já existir, segue o startup.
airflow users create \
  --username "${AIRFLOW_ADMIN_USERNAME}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
  --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
  --role Admin \
  --email "${AIRFLOW_ADMIN_EMAIL}" || true

airflow webserver &
airflow scheduler