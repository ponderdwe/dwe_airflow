from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.sdk.definitions._internal.contextmanager import DagContext
from pendulum import datetime
from typing import Callable
import functools


common_dag_configurations = {
    'max_active_runs': 1,
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
    'owner_links': {'Ponder': 'https://ponder.co/team'},
    'schedule': None,
}

common_dag_default_args = {
    'owner': 'Ponder',
    'email': 'egor@ponder.co',
}


def dag_template(**kwargs):
    kwargs.pop('add_tasks', None)
    kwargs['default_args'] = dict(
        common_dag_default_args, **kwargs.pop('default_args', {})
    )

    # Airflow 3 removed schedule_interval — convert to schedule
    schedule_interval = kwargs.pop('schedule_interval', None)
    schedule = kwargs.get('schedule')

    if schedule and schedule_interval:
        from airflow.timetables.assets import AssetOrTimeSchedule
        from airflow.timetables.trigger import CronTriggerTimetable
        kwargs['schedule'] = AssetOrTimeSchedule(
            timetable=CronTriggerTimetable(schedule_interval, timezone='UTC'),
            assets=(
                schedule if isinstance(schedule, (list, tuple)) else [schedule]
            ),
        )
    elif schedule_interval:
        kwargs['schedule'] = schedule_interval

    dag_configurations = dict(common_dag_configurations, **kwargs)
    dag_id = dag_configurations.get('dag_id', 'unknown')

    def wrapper(f: Callable):
        @functools.wraps(f)
        def f_with_asset_emit():
            f()

            dag_obj = DagContext.get_current_dag()
            current_leaves = list(dag_obj.leaves) if dag_obj else []

            @task(task_id='update_asset', outlets=[Asset(dag_id)])
            def update_asset():
                pass

            update_t = update_asset()
            for t in current_leaves:
                t >> update_t

        def factory():
            return dag(**dag_configurations)(f_with_asset_emit)()
        return factory
    return wrapper
