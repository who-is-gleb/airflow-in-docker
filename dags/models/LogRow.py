import datetime
import pendulum
import logging
from typing import Optional, Dict


class LogRow:
    log_id: int
    log_dttm: datetime.datetime
    log_dag_id: Optional[str]
    log_task_id: Optional[str]
    log_event: str
    log_execution_date: Optional[datetime.datetime]
    log_owner: str

    def __init__(self, log_row):
        self.log_id = log_row.id
        self.log_dttm = log_row.dttm
        self.log_dag_id = log_row.dag_id
        self.log_task_id = log_row.task_id
        self.log_event = log_row.event
        self.log_execution_date = log_row.execution_date
        self.log_owner = log_row.owner
        logging.info(f'Created LogRow object. '
                     f' log_id: {str(self.log_id)}, type: {type(self.log_id)};'
                     f' log_dttm: {str(self.log_dttm)}, type: {type(self.log_dttm)};'
                     f' log_dag_id: {str(self.log_dag_id)}, type: {type(self.log_dag_id)};'
                     f' log_task_id: {str(self.log_task_id)}, type: {type(self.log_task_id)};'
                     f' log_event: {str(self.log_event)}, type: {type(self.log_event)};'
                     f' log_execution_date: {str(self.log_execution_date)}, type: {type(self.log_execution_date)};'
                     f' log_owner: {str(self.log_owner)}, type: {type(self.log_owner)};'
                     )

    def __str__(self):
        return 'LogRow class str'

    def __repr__(self):
        return 'LogRow class representation'

    @staticmethod
    def get_ch_table_schema() -> Dict:
        return {
            "log_id": "UInt32",
            "log_dttm": "DateTime",
            "log_dag_id": "String",
            "log_task_id": "String",
            "log_event": "String",
            "log_execution_date": "DateTime",
            "log_owner": "String",
        }

    def to_dict(self) -> Dict:
        log_dttm = self.log_dttm.strftime('%Y-%m-%d %H:%M:%S') if \
            self.log_dttm is not None else '1970-01-01 00:00:00'
        log_dttm = pendulum.parse(log_dttm).int_timestamp

        log_execution_date = self.log_execution_date.strftime('%Y-%m-%d %H:%M:%S') if \
            self.log_execution_date is not None else '1970-01-01 00:00:00'
        log_execution_date = pendulum.parse(log_execution_date).int_timestamp

        return {
            "log_id": self.log_id,
            "log_dttm": log_dttm,
            "log_dag_id": self.log_dag_id if self.log_dag_id is not None else '',
            "log_task_id": self.log_task_id if self.log_task_id is not None else '',
            "log_event": self.log_event,
            "log_execution_date": log_execution_date,
            "log_owner": self.log_owner,
        }
