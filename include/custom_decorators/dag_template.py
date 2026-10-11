from airflow.decorators import dag
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
    kwargs['default_args'] = dict(common_dag_default_args, **kwargs.pop('default_args', {}))

    # Airflow 3 removed schedule_interval — convert to schedule
    schedule_interval = kwargs.pop('schedule_interval', None)
    schedule = kwargs.get('schedule')

    if schedule and schedule_interval:
        from airflow.timetables.datasets import DatasetOrTimeSchedule
        from airflow.timetables.trigger import CronTriggerTimetable
        kwargs['schedule'] = DatasetOrTimeSchedule(
            timetable=CronTriggerTimetable(schedule_interval, timezone='UTC'),
            datasets=schedule if isinstance(schedule, (list, tuple)) else [schedule],
        )
    elif schedule_interval:
        kwargs['schedule'] = schedule_interval

    dag_configurations = dict(common_dag_configurations, **kwargs)

    def wrapper(f: Callable):
        @functools.wraps(f)
        def factory(*args, **kwargs):
            return dag(**dag_configurations)(f)()
        return factory
    return wrapper
