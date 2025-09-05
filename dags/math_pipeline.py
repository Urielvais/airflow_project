from airflow.decorators import dag, task
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Constants
NUMBER1: int = 5
NUMBER2: int = 11
ADD_VALUE: int = 10
DIVISOR: int = 3

@dag(
    dag_id="math_pipeline",
    description="DAG that performs a simple math workflow with dependencies",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    doc_md="""
    ### Math Pipeline DAG
    6 tasks performing simple arithmetic:
    1. Start process
    2. Multiply NUMBER1 by 2
    3. Add ADD_VALUE to NUMBER2
    4. Sum results from task 2 and 3
    5. Divide result by DIVISOR
    6. Log completion
    """
)
def math_pipeline():
    @task
    def start_process() -> str:
        logger.info("Task1: Starting the DAG process.")
        return "ok"

    @task
    def multiply_number() -> int:
        result = NUMBER1 * 2
        logger.info(f"Task2: {NUMBER1} * 2 = {result}")
        return result

    @task
    def add_constant() -> int:
        result = NUMBER2 + ADD_VALUE
        logger.info(f"Task3: {NUMBER2} + {ADD_VALUE} = {result}")
        return result

    @task
    def sum_results(val1: int, val2: int) -> int:
        total = val1 + val2
        logger.info(f"Task4: {val1} + {val2} = {total}")
        return total

    @task
    def divide_result(val: float) -> float:
        if DIVISOR == 0:
            logger.error("DIVISOR is 0! Cannot divide by zero.")
            raise ValueError("DIVISOR cannot be zero.")
        result = val / DIVISOR
        logger.info(f"Task5: {val} / {DIVISOR} = {result}")
        return result

    @task
    def end_process(final_value: float) -> None:
        logger.info(f"Task6: DAG process completed successfully. Final result = {final_value}")

    # wiring
    _ = start_process()
    v1 = multiply_number()
    v2 = add_constant()
    summed = sum_results(v1, v2)
    final = divide_result(summed)
    end_process(final)

dag = math_pipeline()