from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
import json


class CustomDbtOperator(BashOperator):

    def __init__(
            self,
            *,
            dbt_commands: str | list[str],
            dbt_project_path: str,
            conn_id: str = 'EDW',
            run_dbt_deps: bool = False,
            **kwargs
    ):
        self._conn_id = conn_id
        dbt_cmds = dbt_commands if isinstance(dbt_commands, list) else [dbt_commands]
        if run_dbt_deps:
            dbt_cmds.insert(0, 'dbt deps')

        # Connection loaded at execute() time to avoid import errors during DAG parsing
        super().__init__(
            bash_command=' ; '.join([
                f'{cmd} --project-dir {dbt_project_path} --profiles-dir {dbt_project_path}'
                for cmd in dbt_cmds
            ]),
            append_env=True,
            **kwargs
        )

    def execute(self, context):
        conn = BaseHook.get_connection(self._conn_id)
        extra = json.loads(conn.extra or '{}')
        trino_env = extra.get('env', 'dev')

        self.env = {
            'TRINO_HOST':     conn.host or '',
            'TRINO_USER':     conn.login or '',
            'TRINO_PASSWORD': conn.password or '',
            'TRINO_DBNAME':   conn.schema or 'iceberg',
            'TRINO_SCHEMA':   extra.get('schema', 'silver'),
            'TRINO_ENV':      trino_env,
            'PYTHONWARNINGS': 'ignore:Unverified HTTPS request',
        }
        return super().execute(context)
