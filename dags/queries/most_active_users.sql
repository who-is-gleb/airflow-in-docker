select day,
       argMax(log_owner, owners_activity) as owner_with_max_activity
from (
     select toDate(log_dttm) as day,
            log_owner,
            count(log_owner) as owners_activity
     from orchestration.orchestration_logs
     group by toDate(log_dttm), log_owner
     order by day, owners_activity
)
group by day
