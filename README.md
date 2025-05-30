# ChatDB: AI-Assisted SQL & NoSQL Query Generator

Wil Sheffield  
May 2025

## Overview
ChatDB is an interactive database assistant that dynamically generates SQL and NoSQL queries for users.  
It allows users to:
- Generate **SQL queries** (`GROUP BY`, `JOIN`, `WHERE`)
- Generate **MongoDB queries** (`$match`, `$sort`, `$group`)
- Load **CSV & JSON datasets** for query execution  

## Technologies Used
- **Python** (Data processing & query generation)
- **MySQL** (SQL execution)
- **MongoDB** (NoSQL execution)

## Files in this Repository
- `01_final_code.py` → Python script for query generation
- `02_final_report.pdf` → Summary report of the project
- `SampleData/` → Example datasets (CSV, JSON)

## How to Use
1. Clone the repository:
   ```bash
   git clone https://github.com/wilrsheff/ChatDB.git
   cd ChatDB
2. Install dependencies:
   pip install pandas pymongo mysql-connector-python
3. Run the script:
   python final_code.py
4. Enter your natural language query, and ChatDB will generate an SQL or NoSQL query.

## Sample Query Output
- User Query: "Get the total sales per category from the sales dataset”
- Generated SQL Query:
   SELECT category, SUM(sales) 
   FROM sales_data 
   GROUP BY category;
- Generated MongoDB Query:
   {
      "$group": { "_id": "$category", "totalSales": { "$sum": "$sales" } }
   }
