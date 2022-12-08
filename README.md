# airflow-in-docker
A project to show what would it take to launch Airflow and Clickhouse in Docker, how to query the internal DB and insert into Clickhouse.

Steps:
- Run Airflow and CH in docker
- Create DAG to export metadata db data (tables LOG and TASK_INSTANCE) from Airflow into CH
- Create specific views in CH
- Create DAG to export views data into the FS as CSV 
