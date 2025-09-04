from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def start_process():
    print("Task1: Starting the DAG process.")

def multiply_by_2():
    number = 5
    result = number * 2
    print(f"Task2: {number} * 2 = {result}")
    return result

def add_10():
    number = 11
    result = number + 10
    print(f"Task3: {number} + 10 = {result}")
    return result

def sum_results(ti):
    val1 = ti.xcom_pull(task_ids='multiply_by_2')
    val2 = ti.xcom_pull(task_ids='add_10')
    total = val1 + val2
    print(f"Task4: Sum of {val1} and {val2} = {total}")
    return total

def divide_by_3(ti):
    val = ti.xcom_pull(task_ids='sum_results')
    result = val / 3
    print(f"Task5: {val} / 3 = {result}")
    return result

def end_process():
    print("Task6: DAG process completed.")

with DAG(
    'math_pipeline',
    start_date=datetime(2025, 9, 4),
    schedule_interval=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='start_process',
        python_callable=start_process
    )

    t2 = PythonOperator(
        task_id='multiply_by_2',
        python_callable=multiply_by_2
    )

    t3 = PythonOperator(
        task_id='add_10',
        python_callable=add_10
    )

    t4 = PythonOperator(
        task_id='sum_results',
        python_callable=sum_results
    )

    t5 = PythonOperator(
        task_id='divide_by_3',
        python_callable=divide_by_3
    )

    t6 = PythonOperator(
        task_id='end_process',
        python_callable=end_process
    )