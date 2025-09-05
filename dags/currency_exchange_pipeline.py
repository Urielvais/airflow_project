from airflow.decorators import dag, task
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Constants
USD_AMOUNT: float = 1000
RATES: dict = {"EUR": 0.91, "ILS": 3.65}

@dag(
    dag_id="currency_exchange_pipeline",
    description="DAG that fetches currency exchange rates and converts a fixed USD amount",
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 9 * * *',
    catchup=False,
    doc_md="""
    ### Currency Exchange Pipeline
    1. Fetch simulated exchange rates (EUR and ILS)
    2. Convert a fixed USD amount ($1000) using the fetched rates
    """
)
def currency_exchange_pipeline():
    @task
    def fetch_rates() -> dict:
        if not RATES:
            logger.error("Rates dictionary is empty!")
            raise ValueError("Rates dictionary cannot be empty")
        logger.info(f"Fetched rates: {RATES}")
        return RATES

    @task
    def convert_amount(rates: dict) -> None:
        if not rates:
            logger.error("Received empty rates dictionary!")
            raise ValueError("Rates dictionary cannot be empty")
        conversions = {currency: USD_AMOUNT * rate for currency, rate in rates.items()}
        logger.info(f"Conversions for ${USD_AMOUNT}: {conversions}")

    rates = fetch_rates()
    convert_amount(rates)

dag = currency_exchange_pipeline()