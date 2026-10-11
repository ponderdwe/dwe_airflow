import os
from airflow.datasets import Dataset
from include.custom_decorators.dag_template import dag_template
from include.custom_operators.dbt_operator import CustomDbtOperator


# Path to the dbt project directory (must contain dbt_project.yml and profiles.yml).
# Update this to match your project structure, e.g.:
#   f"{os.environ['AIRFLOW_HOME']}/dbt/MyProject"
dbt_project_path = os.environ.get(
    'DBT_PROJECT_PATH',
    f"{os.environ['AIRFLOW_HOME']}/dbt",
)

# Define your dbt build groups here.
# Each key becomes the DAG id (suffixed with _dbt).
#   dbt_command     - dbt CLI command to run (e.g. 'dbt build', 'dbt run', 'dbt test')
#   schedules       - Dataset/Asset trigger(s), or None for time-only scheduling
#   schedule_interval - cron expression, or None for dataset-only / manual scheduling
dbt_projects = {
    'build_all': {
        'dbt_command': 'dbt build',
        'schedules': None,
        'schedule_interval': '5 4 * * *',
    },
}


def build_dbt_project(project_name, dbt_project):
    @dag_template(
        dag_id=f'{project_name}_dbt',
        schedule=dbt_project.get('schedules'),
        schedule_interval=dbt_project.get('schedule_interval'),
        tags=['transform', 'dbt'],
    )
    def dag_func():
        CustomDbtOperator(
            task_id='run_dbt',
            dbt_commands=dbt_project['dbt_command'],
            dbt_project_path=dbt_project_path,
            outlets=[Dataset(f'{project_name}_dbt')],
        )

    dag_func()


for _project_name, _dbt_project in dbt_projects.items():
    build_dbt_project(_project_name, _dbt_project)
