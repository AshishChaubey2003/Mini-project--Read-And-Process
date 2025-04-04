# Mini-project--Read-And-Process
# 🧾 Customer and Order Data Processing with PySpark

This project demonstrates how to process and analyze customer and order datasets using PySpark. The pipeline includes data cleansing, enrichment, transformation, and various analytical insights such as customer ranking, order statistics, and more.

## 🚀 Technologies Used
- Apache Spark (PySpark)
- Databricks Notebooks
- CSV & Parquet file formats
- SQL-like DataFrame operations and Window functions

## 📁 Dataset
- `customers.csv`: Contains customer details including city, state, country, registration date, and active status.
- `orders.csv`: Contains order details with customer IDs, order status, date, and amount.

## 🔧 Key Features & Steps

### 📊 Customer Data Processing
- Load data using SparkSession and read CSV format.
- Handle null values in `city`, `state`, and `country` fields.
- Convert `registration_date` to date format and cast `is_active` to boolean.
- Extract `registration_year` and `registration_month`.
- Count distinct cities, states, and countries.
- Group data by `state` and `country` and generate pivot tables on `is_active` users.
- Use Window functions to assign `rank`, `dense_rank`, and `row_number` based on registration date (per state).
- Filter recent customers (`registration_date >= 2023-07-01`).
- Find the oldest and newest registration dates per city.
- Save processed data in Parquet format.

### 🔄 Order Data Analysis
- Load `orders.csv` and join with customer data on `customer_id`.
- Calculate:
  - Total orders per customer
  - Total and average amount spent per customer
  - Order count per status
  - Monthly order trends
- Use Window functions to:
  - Rank customers based on total spending
  - Combine order count with total spend for customer behavior insights

## 📦 Output
- Cleaned customer data saved as Parquet: `/FileStore/tables/processed_customers`
- Multiple DataFrames for insights (e.g., `customer_total_spend`, `orders_status_count`, etc.)

## 📈 Insights Examples
- Top spenders and most frequent customers
- Monthly order trend
- Active vs Inactive users per state
- Spending vs order frequency mapping

## 📫 Author
**Ashish Kumar Chaubey**  
Final Year B.Tech CSE Student  
[LinkedIn](#) | [GitHub](#)

