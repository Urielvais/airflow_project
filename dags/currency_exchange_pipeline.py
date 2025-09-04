from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def fetch_rates():
    rates = {"EUR": 0.91, "ILS": 3.65}
    print(f"Fetched rates: {rates}")
    return rates

def convert_amount(ti):
    rates = ti.xcom_pull(task_ids='fetch_rates')
    usd_amount = 1000
    conversions = {currency: usd_amount * rate for currency, rate in rates.items()}
    print(f"Conversions for ${usd_amount}: {conversions}")

with DAG('currency_exchange_pipeline', start_date=datetime(2025,1,1), schedule_interval='0 9 * * *', catchup=False) as dag:
    t1 = PythonOperator(task_id='fetch_rates', python_callable=fetch_rates)
    t2 = PythonOperator(task_id='convert_amount', python_callable=convert_amount)