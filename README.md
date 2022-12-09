# airflow-in-docker
A project to show what would it take to launch Airflow and Clickhouse in Docker, how to query the Airflow's internal metadata DB, insert into Clickhouse, create views, and extract data as CSV to the local machine.


How to start:
- Launch docker-compose up
- Navigate to 0.0.0.0:8080 and use 'airflow-stage' (or 'airflow-common') as username and password
- Launch 'export-tables-into-ch' DAG first, wait for the result
- Navigate to http://localhost:8123/, use 'ch_user' and 'ch_pass' as authorization
- Query tables if necessary 
- Launch 'export-views-into-csv' DAG, wait for the result
- You can find the extracted CSV files on your local machine under the root/dags folder (in my case its /Users/[myname]/airflow-project/airflow-in-docker/dags)
