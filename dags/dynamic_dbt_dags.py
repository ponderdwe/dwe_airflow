"""
Dynamic dbt DAGs — driven entirely by dbt target/manifest.json.

One DAG is created per dbt group. Schedule and inter-group dependencies are
derived from the manifest so no Airflow-side config files are needed:
  - Run `dbt parse` (or `dbt compile`) to regenerate the manifest after any
    project change, then restart the dag-processor to pick up new DAGs.

Group YAML owner fields used here:
  schedule_interval  cron string or null (manual / asset-only)
  condition          'or' | 'and' — how to combine upstream group Asset triggers
  tags               list of Airflow DAG tags
"""

import json
import operator as op
import os
from functools import reduce
from pathlib import Path

from airflow.sdk import Asset
from include.custom_decorators.dag_template import dag_template
from include.custom_operators.dbt_operator import CustomDbtOperator

# Connection ID for Trino/data warehouse — override with DBT_CONN_ID env var
CONN_ID = os.environ.get('DBT_CONN_ID', 'EDW')

# Path to the dbt project (must contain dbt_project.yml and profiles.yml)
DBT_PROJECT_DIR = Path(os.environ['AIRFLOW_HOME']) / 'dbt' / os.environ.get('DBT_PROJECT_NAME', '')
manifest_path = DBT_PROJECT_DIR / 'target' / 'manifest.json'

# ── Load manifest ──────────────────────────────────────────────────────────
if not manifest_path.exists():
    groups = {}
    group_deps = {}
else:
    manifest = json.loads(manifest_path.read_text())
    groups = manifest.get('groups', {})

    # Build node-id → group name (nodes + sources via loader field)
    node_group: dict[str, str] = {}
    for nid, node in manifest.get('nodes', {}).items():
        g = node.get('config', {}).get('group')
        if g:
            node_group[nid] = g
    for sid, src in manifest.get('sources', {}).items():
        g = src.get('loader')
        if g:
            node_group[sid] = g

    # group → set of upstream groups it depends on
    group_deps: dict[str, set] = {}
    for nid, node in manifest.get('nodes', {}).items():
        my_group = node.get('config', {}).get('group')
        if not my_group:
            continue
        for dep_id in node.get('depends_on', {}).get('nodes', []):
            dep_group = node_group.get(dep_id)
            if dep_group and dep_group != my_group:
                group_deps.setdefault(my_group, set()).add(dep_group)


# ── DAG factory ───────────────────────────────────────────────────────────
def build_dbt_dag(group: dict) -> None:
    name = group['name']
    owner = group.get('owner', {})
    schedule_interval = owner.get('schedule_interval')
    condition = owner.get('condition', 'or')
    tags = owner.get('tags', ['dbt'])

    # Build Asset-based schedule from inter-group deps (mirrors dbt_dags.sql logic)
    upstream_assets = [
        Asset(dep) for dep in sorted(group_deps.get(name, []))
    ]
    combine = {'or': op.or_, 'and': op.and_}.get(condition, op.or_)
    schedule = reduce(combine, upstream_assets) if upstream_assets else None

    @dag_template(
        dag_id=f'{name}_dbt',
        schedule=schedule,
        schedule_interval=schedule_interval,
        tags=tags,
    )
    def dag_func():
        CustomDbtOperator(
            task_id='dbt_build',
            dbt_commands=f'dbt build --select group:{name}',
            dbt_project_path=str(DBT_PROJECT_DIR),
            conn_id=CONN_ID,
        )

    dag_func()


for _group in groups.values():
    build_dbt_dag(_group)
