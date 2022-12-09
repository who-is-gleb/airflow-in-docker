import csv
import os
import logging
import datetime
import pendulum
import pathlib
from datetime import timedelta
from typing import List, Dict

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context, Context
from airflow import settings
from airflow.models import Log, TaskInstance

from core.dag_default_args import get_default_arguments
from core.clickhouse import ClickhouseLocalhost
from models.tags import Tag
from models.LogRow import LogRow
from models.TaskInstanceRow import TaskInstanceRow

DAG_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))


default_args = get_default_arguments(
    retry_delay=timedelta(minutes=1),
    retries=0
)


@dag(
    dag_id='export-views-into-csv',
    description='Exports views data into csv on local machine',
    schedule_interval=None,
    start_date=datetime.datetime(2022, 12, 8),
    default_args=default_args,
    catchup=False,
    tags=[Tag.views]
)
def create_dag():
    @task(task_id='export_active_users_to_csv')
    def export_active_users():
        client = ClickhouseLocalhost()
        result = client.execute('SELECT * FROM orchestration.most_active_user_per_day FORMAT CSV')
        # This can surely be changed to any other folder if mounted. For example, saving to root folder of
        # airflow and local machine (something like .:/opt/airflow)
        final_path = (DAG_DIR / pathlib.Path("most_active_users_per_day.csv"))
        logging.info(f'Exporting data to {final_path}')
        with open(final_path, 'w+') as fd:
            writer = csv.writer(fd)
            writer.writerows(result)

    exported_users = export_active_users()

    @task(task_id='export_task_metrics_to_csv')
    def export_task_metrics():
        client = ClickhouseLocalhost()
        result = client.execute('SELECT * FROM orchestration.tasks_metrics FORMAT CSV')
        final_path = (DAG_DIR / pathlib.Path("tasks_metrics.csv"))
        logging.info(f'Exporting data to {final_path}')
        with open(final_path, 'w+') as fd:
            writer = csv.writer(fd)
            writer.writerows(result)


created_dag = create_dag()


def main():
    """Main function for local script testing. Set LOCAL_DEBUG=1 option in run config for that to work."""
    pass


if __name__ == '__main__':
    if os.environ.get('LOCAL_DEBUG'):
        exit(main())
