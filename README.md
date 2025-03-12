# Pyspark_ETL-on-DataBricks
A Simple ETL Project on DataBricks
Data Source : Kaggle

# PySpark ETL Project: Customer and Order Data Processing

## **Project Overview**
This project demonstrates an end-to-end **ETL (Extract, Transform, Load) pipeline** using **Apache Spark (PySpark)** in **Databricks**. The project processes customer, order, and order item data stored in CSV format, performs data transformations, and outputs structured JSON reports. Additionally, it includes revenue analysis based on order trends.

## **Objectives**
- **Extract** customer, order, and order item data from CSV files.
- **Transform** data by joining tables, aggregating customer orders, and structuring nested data.
- **Load** transformed data into JSON format for easy consumption.
- **Analyze** order trends and revenue generation over time.

## **Technologies Used**
- **Apache Spark (PySpark)** – for distributed data processing
- **Databricks** – for cloud-based Spark execution
- **DBFS (Databricks File System)** – for file storage and data persistence
- **JSON and CSV** – for structured data storage and retrieval

---

## **Data Sources**
The input data consists of three CSV files stored in Databricks' FileStore:
- `customers.txt` – Contains customer details (ID, name, email, address, etc.).
- `orders.txt` – Contains order details (order ID, date, customer ID, and status).
- `order_items.txt` – Contains order item details (item ID, product ID, price, and subtotal).

---

## **ETL Workflow**
### **1. Extract: Reading CSV Files into Spark DataFrames**
- Define schemas for structured data processing.
- Load CSV files into PySpark DataFrames using Databricks FileStore.
- Display and validate the data structure.

### **2. Transform: Data Processing and Structuring**
#### **A. Joining Customers and Orders**
- Perform an **inner join** between customers and orders based on `customer_id`.
- Select relevant fields such as `customer_id`, `order_id`, `order_date`, and `order_status`.
- **Group orders by customer** to create a nested order structure.
- **Aggregate** multiple orders for each customer.

#### **B. Denormalizing Data (Customers + Orders + Order Items)**
- Join `customers`, `orders`, and `order_items` into a **denormalized dataset**.
- Create a **hierarchical JSON-like structure**, where each customer has:
  - Order details (order ID, date, status)
  - A list of associated order items
- Aggregate multiple items under each order.

#### **C. Writing Transformed Data to JSON**
- Save the structured customer order data as JSON to **Databricks FileStore**.

#### **D. Reading and Flattening JSON Data**
- Reload the **nested JSON file** into PySpark.
- Flatten hierarchical structures using **explode()** functions.
- Convert the structured data into a tabular format for analysis.

### **3. Load: Revenue Analysis and Trend Insights**
- **Filter** orders to include only completed transactions.
- **Group revenue by month** to analyze sales trends.
- **Calculate total monthly revenue**.
- Order results chronologically for reporting.

---

## **Output & Results**
- **Processed JSON Files**:
  - **`final.json`** – Structured customer order data.
  - **`denormalized.json`** – Fully joined and structured data for analytics.
- **Revenue Analysis Table**:
  - Displays monthly revenue trends from completed orders.
  
---

## **Use Cases**
- **E-commerce Order Analytics**: Helps businesses understand customer purchase patterns.
- **Customer Segmentation**: Identifies frequent buyers and order trends.
- **Revenue Forecasting**: Tracks monthly revenue growth and seasonal trends.
- **Data Warehousing & Reporting**: Provides structured, optimized data for dashboards.

---

## **Conclusion**
This project showcases how **PySpark** can be leveraged for efficient data transformation and analytics. By structuring, aggregating, and analyzing customer order data, businesses can gain valuable insights into sales trends, customer behavior, and revenue performance.
