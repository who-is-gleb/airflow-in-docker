import datetime
import pendulum
import logging
from typing import Optional, Dict


class TaskInstanceRow:
    task_instance_task_id: str
    task_instance_start_date: Optional[datetime.datetime]
    task_instance_duration: Optional[float]
    task_instance_run_id: str

    def __init__(self, task_instance_row):
        self.task_instance_task_id = task_instance_row.task_id
        self.task_instance_start_date = task_instance_row.start_date
        self.task_instance_duration = task_instance_row.duration
        self.task_instance_run_id = task_instance_row.run_id
        logging.info(f'Created TaskInstanceRow object. '
                     f' task_instance_task_id: {str(self.task_instance_task_id)}, type: {type(self.task_instance_task_id)};'
                     f' task_instance_start_date: {str(self.task_instance_start_date)}, type: {type(self.task_instance_start_date)};'
                     f' task_instance_duration: {str(self.task_instance_duration)}, type: {type(self.task_instance_duration)};'
                     f' task_instance_run_id: {str(self.task_instance_run_id)}, type: {type(self.task_instance_run_id)};'
                     )

    def __str__(self):
        return 'TaskInstanceRow class str'

    def __repr__(self):
        return 'TaskInstanceRow class representation'

    @staticmethod
    def get_ch_table_schema() -> Dict:
        return {
            "task_instance_task_id": "String",
            "task_instance_start_date": "DateTime",
            "task_instance_duration": "Float64",
            "task_instance_run_id": "String",
        }

    def to_dict(self) -> Dict:
        task_instance_start_date = self.task_instance_start_date.strftime('%Y-%m-%d %H:%M:%S') if \
            self.task_instance_start_date is not None else '1970-01-01 00:00:00'
        task_instance_start_date = pendulum.parse(task_instance_start_date).int_timestamp

        return {
            "task_instance_task_id": self.task_instance_task_id,
            "task_instance_start_date": task_instance_start_date,
            "task_instance_duration": self.task_instance_duration if self.task_instance_duration is not None else 0.0,
            "task_instance_run_id": self.task_instance_run_id,
        }
