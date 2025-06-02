from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
import os
import mysql.connector as mysql

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 30),
    'retries': 1
}

# Paths
CSV_PATH = '/opt/airflow/data/sales_data_sample.csv'
EXTRACTED_PATH = '/opt/airflow/dags/files/extracted_data.csv'
TRANSFORMED_PATH = '/opt/airflow/dags/files/transformed_data.csv'

# DAG definition
with DAG(
    dag_id='sales_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # ----------------------
    # EXTRACT Task
    # ----------------------
    def extract():
        df = pd.read_csv(CSV_PATH, encoding='ISO-8859-1')
        os.makedirs(os.path.dirname(EXTRACTED_PATH), exist_ok=True)
        df.to_csv(EXTRACTED_PATH, index=False)

    # ----------------------
    # TRANSFORM Task
    # ----------------------
    def transform():
        df = pd.read_csv(EXTRACTED_PATH)
        df.drop_duplicates(inplace=True)

        # Handle missing values
        cat_col = ['PRODUCTCODE', 'PRODUCTLINE', 'TERRITORY']
        num_col = ['QUANTITYORDERED', 'PRICEEACH', 'SALES']

        for col in cat_col:
            if col in df.columns:
                df[col].fillna(df[col].mode()[0], inplace=True)

        for col in num_col:
            if col in df.columns:
                df[col].fillna(df[col].mean(), inplace=True)

        # Date and calculated fields
        df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'], errors='coerce')
        df['ORDERDATE'].fillna(pd.Timestamp('2000-01-01'), inplace=True)

        df['TOTAL_PRICE'] = df['QUANTITYORDERED'] * df['PRICEEACH']
        df['PRICE_USD'] = df['SALES'] * 0.385

        # Encoding and normalization
        le = LabelEncoder()
        for col in cat_col:
            df[col] = le.fit_transform(df[col].astype(str))

        scaler = MinMaxScaler()
        df[num_col] = scaler.fit_transform(df[num_col])

        df.to_csv(TRANSFORMED_PATH, index=False)

    # ----------------------
    # LOAD Task
    # ----------------------
    def load():
        df = pd.read_csv(TRANSFORMED_PATH)

        conn = mysql.connect(
            host='host.docker.internal',
            user='root',
            password='root',
            database='saleDB'
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT IGNORE INTO orders (
                    ORDERNUMBER, ORDERLINENUMBER, ORDERDATE, STATUS,
                    QTR_ID, MONTH_ID, YEAR_ID, PRODUCTCODE,
                    CUSTOMERNAME, SALES, TOTAL_PRICE, PRICE_USD
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['ORDERNUMBER'], row['ORDERLINENUMBER'],
                pd.to_datetime(row['ORDERDATE']).strftime('%Y-%m-%d'),
                row['STATUS'], row['QTR_ID'], row['MONTH_ID'], row['YEAR_ID'],
                row['PRODUCTCODE'], row['CUSTOMERNAME'], row['SALES'],
                row['TOTAL_PRICE'], row['PRICE_USD']
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Load complete into 'orders' table only")

    # ----------------------
    # PythonOperator Tasks
    # ----------------------
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
