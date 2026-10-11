"""
Manual dbt DAGs — driven by dbt/dbt_jobs.json.

Use this for explicit dbt commands that don't fit the group-based approach
(e.g. full builds, ad-hoc selects, cross-project commands).

Each entry in dbt_jobs.json creates one <name>_dbt DAG.

Fields:
  name               required  — DAG id prefix and Asset name
  dbt_command        required  — full dbt CLI command (e.g. "dbt build --select +my_model")
  schedule_interval  optional  — cron string, or null
  schedules          optional  — Asset trigger name(s) as a list, or null
  tags               optional  — list of Airflow DAG tags
"""

import json
import os
from pathlib import Path

from airflow.sdk import Asset
from include.custom_decorators.dag_template import dag_template
from include.custom_operators.dbt_operator import CustomDbtOperator

CONN_ID = 'TRINO'
DBT_PROJECT_DIR = Path(os.environ['AIRFLOW_HOME']) / 'dbt' / 'TrinoDbt'

jobs_file = Path(os.environ['AIRFLOW_HOME']) / 'dbt' / 'dbt_jobs.json'
dbt_jobs = json.loads(jobs_file.read_text()) if jobs_file.exists() else []


def build_dbt_job(job: dict) -> None:
    name = job['name']
    raw_schedules = job.get('schedules')
    schedule = (
        [Asset(s) for s in raw_schedules]
        if isinstance(raw_schedules, list)
        else (Asset(raw_schedules) if raw_schedules else None)
    )

    @dag_template(
        dag_id=f'{name}_dbt',
        schedule=schedule,
        schedule_interval=job.get('schedule_interval'),
        tags=job.get('tags', ['dbt']),
    )
    def dag_func():
        CustomDbtOperator(
            task_id='dbt_build',
            dbt_commands=job['dbt_command'],
            dbt_project_path=str(DBT_PROJECT_DIR),
            conn_id=CONN_ID,
        )

    dag_func()


for _job in dbt_jobs:
    build_dbt_job(_job)
