from __future__ import annotations
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd

# File paths
CUSTOMER_TXT_PATH = '/opt/airflow/dags/Customers.txt'
ORDER_TXT_PATH = '/opt/airflow/dags/Orders.txt'
OUTPUT_PATH = '/opt/data/output.csv'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

# Define the DAG
with DAG(
    'etl_join_orders_customers_txt',
    default_args=default_args,
    description='ETL DAG to join Orders and Customers data from TXT files',
    catchup=False
) as dag:

    # Task 1: Read the customers data
    @task(task_id="read_customers")
    def read_customers():
        print(f"Reading customers from {CUSTOMER_TXT_PATH}")
        customers = pd.read_csv(CUSTOMER_TXT_PATH, delimiter=';')
        return customers

    # Task 2: Read the orders data
    @task(task_id="read_orders")
    def read_orders():
        print(f"Reading orders from {ORDER_TXT_PATH}")
        orders = pd.read_csv(ORDER_TXT_PATH, delimiter='|')
        return orders

    # Task 3: Merge customers and orders data
    @task(task_id="merge_customers_orders")
    def merge_customers_orders(customers, orders):
        print("Merging orders and customers")
        joined_data = pd.merge(orders, customers, on='CustomerID', how='left')
        return joined_data

    # Task 4: Save the merged data to output
    @task(task_id="save_output")
    def save_output(joined_data):
        print(f"Saving output to {OUTPUT_PATH}")
        joined_data.to_csv(OUTPUT_PATH, index=False)
        print(f"Data successfully joined and exported to {OUTPUT_PATH}")

    # Define the task dependencies
    customer_data = read_customers()
    order_data = read_orders()

    # Merge the data and then save it
    merged_data = merge_customers_orders(customer_data, order_data)
    save_output(merged_data)