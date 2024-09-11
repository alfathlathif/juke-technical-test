from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

CUSTOMER_TXT_PATH = '/opt/airflow/dags/Customers.txt'
ORDER_TXT_PATH = '/opt/airflow/dags/Orders.txt'
OUTPUT_PATH = '/opt/data/output.csv'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2021, 1, 1, tz="UTC"),
    'retries': 1
}

# Keep only one schedule option
dag = DAG(
    'etl_join_orders_customers_txt',
    catchup=False,
    description='ETL DAG to join Orders and Customers data from TXT files',
    schedule_interval='@daily'  # Use this or 'schedule="@daily"', not both
)

def etl_join():
    print(f"Reading customers from {CUSTOMER_TXT_PATH}")
    customers = pd.read_csv(CUSTOMER_TXT_PATH, delimiter=';')
    
    print(f"Reading orders from {ORDER_TXT_PATH}")
    orders = pd.read_csv(ORDER_TXT_PATH, delimiter='|')
    
    print("Merging orders and customers")
    joined_data = pd.merge(orders, customers, on='CustomerID', how='left')
    
    print(f"Saving output to {OUTPUT_PATH}")
    joined_data.to_csv(OUTPUT_PATH, index=False)
    
    print(f"Data successfully joined and exported to {OUTPUT_PATH}")

etl_task = PythonOperator(
    task_id='etl_join_orders_customers_txt',
    python_callable=etl_join,
    dag=dag,
)

etl_task
