select task_instance_task_id,
     count(log_id),
     round(avg(task_instance_duration), 2) as avg_duration
from orchestration.orchestration_task_instances as ti
left join orchestration.orchestration_logs as logs on ti.task_instance_task_id = logs.log_task_id
group by task_instance_task_id
order by task_instance_task_id