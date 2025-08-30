ETL Workshop 

📋 Project Description

This project implements a complete ETL process for selection process data, including extraction, transformation, loading into a Data Warehouse, and generating KPIs with visualizations.

🏗️ Project Architecture
-Extract: Data loading from CSV file

-Transform: Data cleaning, business rule application, and dimensional model creation

-Load: Loading into SQLite Data Warehouse

-Analysis: SQL queries and visualizations from the DW

📊 Dimensional Model
Star schema with:

Fact_Hiring: Fact table with hiring metrics

Dim_Date: Time dimension

Dim_Technology: Technology dimension

Dim_Country: Geographic dimension

Dim_Seniority: Experience level dimension

##🚀 Installation and Usage
-Clone the repository

-Install dependencies: pip install -r requirements.txt

-Execute the ETL pipeline

-Visualizations will be generated automatically

📈 Implemented KPIs
Hires by technology

Hires by year

Hires by seniority

Hires by country (USA, Brazil, Colombia, Ecuador)

Hire rate by technology

Average scores by seniority

🛠️ Technologies Used
Python 3.x

Pandas

SQLite

Matplotlib

Seaborn



📁 Project Structure
Workshop_JPL/

├── data/                      # Data directory

│   └── candidates.csv         # Input data file

├── dw/                        # (Optional) Generated dimension and fact CSVs

├── visualizations/            # Generated visualizations (optional)

├── ETL.py                     # ETL pipeline script

├── KPIs.py                    # KPI analysis and visualization script

├── conection.py               # Database connection manager (SQLAlchemy engine)

├── requirements.txt           # Dependencies

└── README.md                  # Project documentation

🔧 Setup Instructions
Place your candidates.csv file in the project root

Install required packages: pip install -r requirements.txt

Run the main application: python main.py

📊 Expected Output
SQLite database with star schema

6 professional KPI visualizations

Data analysis reports

Dashboard with hiring metrics

🎯 Business Insights
This solution provides valuable insights for:

Recruitment strategy optimization

Technology talent market analysis

Geographical hiring patterns

Candidate evaluation process effectiveness

📝 License
This project is created for educational purposes as part of the Data Engineering and Artificial Intelligence academic program.
