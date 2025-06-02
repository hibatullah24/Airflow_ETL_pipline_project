# 🚀 Sales Data ETL Pipeline with Airflow, Docker, and MySQL

This project demonstrates a complete ETL pipeline that extracts sales data from a CSV file, performs data cleaning and transformation using Python, and loads it into a normalized MySQL database. The entire process is orchestrated using Apache Airflow running in a Dockerized environment.

---

## 📊 Project Overview

- **Extract:** Read raw data from `sales_data_sample.csv`
- **Transform:** Clean and format data (handle missing values, convert data types, calculate total prices, etc.)
- **Load:** Normalize and insert data into four MySQL tables: `orders`, `customers`, `order_items`, and `date`
- **Orchestrate:** Manage tasks using Apache Airflow with CeleryExecutor

---

## 🛠️ Tech Stack

- Python 3.11
- Apache Airflow
- MySQL
- Docker & Docker Compose
- MySQL Workbench
- Visual Studio Code

---

## 🧱 Database Schema

The data is loaded into a normalized MySQL schema with the following tables:

- `orders(ORDERNUMBER, ORDERLINENUMBER, ORDERDATE, STATUS, QTR_ID, MONTH_ID,YEAR_ID, PRODUCTCODE, CUSTOMERNAME,  SALES, TOTAL_PRICE,   PRICE_USD)`
- `customers(CUSTOMERNAME, CONTACTLASTNAME, CONTACTFIRSTNAME, PHONE, ADDRESSLINE1, CITY, POSTALCODE, COUNTRY, TERRITORY  )`
- `order_items(ORDERNUMBER, ORDERLINENUMBER, QUANTITYORDERED, PRICEEACH, MSRP, DEALSIZE)`
- `date(ORDERDATE, QTR_ID, MONTH_ID, YEAR_ID)`


