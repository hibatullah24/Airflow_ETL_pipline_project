from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import numpy as np
import mysql.connector

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 30),
}

# Initialize the DAG
dag = DAG(
    dag_id='sales_etl_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Extract task
def extract():
    df = pd.read_csv('/opt/airflow/data/sales_data_sample.csv', encoding='latin1')
    df.to_csv('/opt/airflow/data/clean_sales.csv', index=False)

# Transform task
def transform():
    df = pd.read_csv('/opt/airflow/data/clean_sales.csv')

    # Fill numerical columns with mean
    num_cols = ['QUANTITYORDERED', 'PRICEEACH', 'SALES']
    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        df[col].fillna(df[col].mean(), inplace=True)

    # Fill categorical columns with mode
    cat_cols = [
        'PRODUCTCODE', 'PRODUCTLINE', 'CUSTOMERNAME', 'CONTACTLASTNAME',
        'CONTACTFIRSTNAME', 'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2',
        'CITY', 'STATE', 'POSTALCODE', 'COUNTRY', 'TERRITORY',
        'STATUS', 'DEALSIZE'
    ]
    for col in cat_cols:
        if col in df.columns:
            if df[col].isnull().any():
                if col == 'ADDRESSLINE2':
                    df[col].fillna("N/A", inplace=True)  # Special handling
                else:
                    df[col].fillna(df[col].mode()[0], inplace=True)

    # Ensure QUANTITYORDERED is numeric and no zeros due to bad conversion
    # Convert to numeric forcing errors to NaN, then fill with mean again if any
    df['QUANTITYORDERED'] = pd.to_numeric(df['QUANTITYORDERED'], errors='coerce')
    if df['QUANTITYORDERED'].isnull().any():
        df['QUANTITYORDERED'].fillna(df['QUANTITYORDERED'].mean(), inplace=True)
    # Finally convert to int after filling
    df['QUANTITYORDERED'] = df['QUANTITYORDERED'].round(0).astype(int)

    # Calculate Total Price and convert to USD
    df['Total_Price'] = df['QUANTITYORDERED'] * df['PRICEEACH']
    df['Price_USD'] = df['Total_Price'] * 0.28

    # Save cleaned data
    df.to_csv('/opt/airflow/data/transformed_sales.csv', index=False)

# Load task
def load():
    df = pd.read_csv('/opt/airflow/data/transformed_sales.csv')

    # Ensure types are native Python types and QUANTITYORDERED is int
    df = df.astype(object).where(pd.notnull(df), None)
    df['QUANTITYORDERED'] = df['QUANTITYORDERED'].astype(int)
    df['ORDERNUMBER'] = df['ORDERNUMBER'].astype(int)
    df['ORDERLINENUMBER'] = df['ORDERLINENUMBER'].astype(int)
    df['MONTH_ID'] = df['MONTH_ID'].astype(int)
    df['YEAR_ID'] = df['YEAR_ID'].astype(int)
    df['QTR_ID'] = df['QTR_ID'].astype(int)

    conn = mysql.connector.connect(
        host='host.docker.internal',
        user='root',
        password='root',
        database='saleDB'
    )
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT IGNORE INTO customers (
                CUSTOMERNAME, CONTACTLASTNAME, CONTACTFIRSTNAME, PHONE,
                ADDRESSLINE1, CITY, POSTALCODE,
                COUNTRY, TERRITORY
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['CUSTOMERNAME'], row['CONTACTLASTNAME'], row['CONTACTFIRSTNAME'], row['PHONE'],
            row['ADDRESSLINE1'], row['CITY'],
            row['POSTALCODE'], row['COUNTRY'], row['TERRITORY']
        ))


        cursor.execute("""
            INSERT IGNORE INTO dates (ORDERDATE, QTR_ID, MONTH_ID, YEAR_ID)
            VALUES (%s, %s, %s, %s)
        """, (
            row['ORDERDATE'], row['QTR_ID'], row['MONTH_ID'], row['YEAR_ID']
        ))

        cursor.execute("""
            INSERT IGNORE INTO orders (
                ORDERNUMBER, ORDERLINENUMBER, ORDERDATE, STATUS,
                QTR_ID, MONTH_ID, YEAR_ID, PRODUCTCODE, CUSTOMERNAME,
                SALES, TOTAL_PRICE, PRICE_USD
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['ORDERNUMBER'], row['ORDERLINENUMBER'], row['ORDERDATE'], row['STATUS'],
            row['QTR_ID'], row['MONTH_ID'], row['YEAR_ID'], row['PRODUCTCODE'], row['CUSTOMERNAME'],
            row['SALES'], row['Total_Price'], row['Price_USD']
        ))

        cursor.execute("""
            INSERT IGNORE INTO order_details (
                ORDERNUMBER, ORDERLINENUMBER, QUANTITYORDERED,
                PRICEEACH, MSRP, DEALSIZE
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row['ORDERNUMBER'], row['ORDERLINENUMBER'], row['QUANTITYORDERED'],
            row['PRICEEACH'], row['MSRP'], row['DEALSIZE']
        ))

    conn.commit()
    cursor.close()
    conn.close()

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

# Task dependencies
extract_task >> transform_task >> load_task
