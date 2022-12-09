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

doc_md = """
### This DAG imports data from the Airflow's metadata db into CH. In particular, tables 'log' and 'task_instance'.

It has catchup option as 'false' and schedule as 'None', so every run would need to be launched by hands

### This logic can be used with rather small or medium-sized data only.
To work with larger sets of data, there are few steps to take:
1) Use generators and yield data instead of saving everything into RAM (the whole list of data)
2) Avoid using XCom and passing data between tasks. Instead, save data to the FS (filesystem) and
read it in other tasks using a given path to file.
3) And, of course, scale Airflow (add more workers) and/or use clustered Airflow in Kubernetes
"""

session = settings.Session()
DAG_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))

tables = {
    'log_data': {
        'type': Log,
        'element_type': LogRow,
        'table_name': 'orchestration_logs',
        'db_name': 'orchestration',
        'partition_by': 'toYYYYMM(log_execution_date)',
        'order_by': 'log_id'
    },
    'task_instance_data': {
        'type': TaskInstance,
        'element_type': TaskInstanceRow,
        'table_name': 'orchestration_task_instances',
        'db_name': 'orchestration',
        'partition_by': 'toYYYYMM(task_instance_start_date)',
        'order_by': 'task_instance_task_id'
    }
}

metrics = {
    'most_active_user_per_day': {
        'query': (DAG_DIR / pathlib.Path('queries/most_active_users.sql')).open().read(),
        'view_name': 'most_active_user_per_day'
    },
    'tasks_metrics': {
        'query': (DAG_DIR / pathlib.Path('queries/tasks_metrics.sql')).open().read(),
        'view_name': 'tasks_metrics'
    },
}


def export_data(prepared_data: List[Dict], date_type):
    table_config = tables[date_type]

    client = ClickhouseLocalhost()
    schema = table_config['element_type'].get_ch_table_schema()
    logging.info(f'table_config type: {str(table_config["type"])}')
    logging.info(f'table_config table_name: {table_config["table_name"]}')
    logging.info(f'table_config db_name: {table_config["db_name"]}')
    logging.info(f'table_config partition_by: {table_config["partition_by"]}')
    logging.info(f'table_config order_by: {table_config["order_by"]}')

    client.create_table(db_name=table_config['db_name'],
                        table_name=table_config['table_name'],
                        schema=schema,
                        partition=table_config['partition_by'],
                        order_by=table_config['order_by'],
                        recreate=True)
    client.insert_data(db_name=table_config['db_name'],
                       table_name=table_config['table_name'],
                       data=prepared_data,
                       schema=schema)
    logging.info(f'Successfully completed task export_data_task')


default_args = get_default_arguments(
    retry_delay=timedelta(minutes=1),
    retries=0
)


@dag(
    dag_id='export-tables-into-ch',
    doc_md=doc_md,
    description='Exports tables log and task_instance into CH ad-hoc',
    schedule_interval=None,
    start_date=datetime.datetime(2022, 12, 8),
    default_args=default_args,
    catchup=False,
    tags=[Tag.adhoc]
)
def create_dag():
    data_tasks = []
    for table_name, table_info in tables.items():
        @task(task_id=f'get_{table_name}_table')
        def get_table_task(data_type, row_type):
            logging.info(f'Extracting all data from {data_type} table.')
            query = session.query(data_type)
            logging.info(f'Query: {str(query)}')
            results = query.all()

            results_parsed = [row_type(row).to_dict() for row in results]

            logging.info(f'Finished extracting from {data_type} table. Total length: {str(len(results_parsed))}')
            return results_parsed

        table_results = get_table_task(table_info['type'], table_info['element_type'])

        @task(task_id=f'insert_{table_name}_into_ch')
        def insert_task(prepared_data, _table_name):
            export_data(prepared_data, _table_name)

        inserted_results = insert_task(table_results, table_name)
        data_tasks.append(inserted_results)

    for metric_name, metrics_config in metrics.items():
        @task(task_id=f'recreate_view_for_most_active_users')
        def create_view(_metric_name, _metrics_config):
            view_name = _metrics_config['view_name']
            query = _metrics_config['query']
            logging.info(f'This task is about to create view {view_name}')
            logging.info(f'The query is:\n{query}')

            client = ClickhouseLocalhost()
            client.create_view(db_name='orchestration', view_name=view_name, query=query)

        created_view = create_view(metric_name, metrics_config)
        data_tasks >> created_view


created_dag = create_dag()


def main():
    """Main function for local script testing. Set LOCAL_DEBUG=1 option in run config for that to work."""
    pass


if __name__ == '__main__':
    if os.environ.get('LOCAL_DEBUG'):
        exit(main())
