from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
import os


class CustomDbtOperator(BashOperator):

    def __init__(
            self,
            *,
            dbt_commands: str,
            dbt_project_path: str,
            run_dbt_deps: bool = False,
            **kwargs
    ):
        self.dbt_project_path = dbt_project_path
        self.dbt_commands = dbt_commands if isinstance(dbt_commands, list) else [dbt_commands]

        if run_dbt_deps:
            self.dbt_commands.insert(0, 'dbt deps')

        target_flag = '-t prod' if os.environ.get('env', '') == 'prod' else ''

        bash_commands = [
            f'{cmd} --project-dir {self.dbt_project_path} --profiles-dir {self.dbt_project_path} {target_flag}'.strip()
            for cmd in self.dbt_commands
        ]
        bash_command = ' ; '.join(bash_commands)

        conn = BaseHook.get_connection('EDW')
        super().__init__(
            bash_command=bash_command,
            env={
                **os.environ,
                'TRINO_DBNAME': conn.schema,
                'TRINO_HOST': conn.host,
                'TRINO_SCHEMA': 'dbt',
                'TRINO_USER': conn.login,
                'PYTHONWARNINGS': 'ignore:Unverified HTTPS request',
                'TRINO_PASSWORD': conn.password if conn.password else '',
            },
            **kwargs
        )
