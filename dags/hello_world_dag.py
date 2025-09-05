from airflow.decorators import dag, task
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dag(
    dag_id="hello_world_dag",
    description="Simple DAG that prints 'Hello World'",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### Hello World DAG
    A simple DAG with a single task that prints 'Hello World' using proper logging.
    """
)
def hello_world_dag():
    @task
    def print_hello_world() -> None:
        logger.info("Hello World")

    print_hello_world()

dag = hello_world_dag()