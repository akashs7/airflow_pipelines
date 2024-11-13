# airflow_pipelines
Repository containing two airflow pipelines with the dag files

## Airflow Setup

- Install airflow using the `pip install apache-airflow`
- Initialize Airflow db `airflow db init`
- Create an airflow user `airflow users create. 
  --username admin \ 
  --firstname FIRST_NAME 
  --lastname LAST_NAME \
  --role Admin \
  --email admin@example.com`
- Start airflow services `airflow webserver --port 8080   
  airflow scheduler`
