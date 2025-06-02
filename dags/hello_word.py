from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the function
def say_hello():
    print("Hello, World from Airflow!")

# Define the DAG
with DAG(
    dag_id='hello_world_dag',
    start_date=datetime(2024, 1, 1),  # any past date
    schedule_interval='@daily',       # runs once daily
    catchup=False,                    # do not run backfills
    tags=['example'],
) as dag:

    # Define the task
    hello_task = PythonOperator(
        task_id='say_hello_task',
        python_callable=say_hello
    )
