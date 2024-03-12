import psycopg2
import pandas as pd
import time

# Define your database connection parameters
db_params = {
    'database': 'linkedin',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost'
}

# Define table creation queries
create_companies_table_query = """
CREATE TABLE IF NOT EXISTS companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255),
    company_location VARCHAR(255)
)
"""

create_jobs_table_query = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id SERIAL PRIMARY KEY,
    company_id INT,
    job_title VARCHAR(255),
    job_link VARCHAR(255),
    job_date DATE,
    job_location VARCHAR(255),
    job_level VARCHAR(255),
    job_type VARCHAR(255),
    job_skills VARCHAR(10000),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
)
"""

# Path to your Excel files
excel_files = {
    'C:/Users/nalan_7isj07o/python/practiceex/ex1/extractload/company_list.xlsx': ('companies', ['company_id', 'company_name', 'company_location']),
    'C:/Users/nalan_7isj07o/python/practiceex/ex1/extractload/jobs_list.xlsx': ('jobs', ['job_id', 'company_id', 'job_title', 'job_link', 'job_date', 'job_location', 'job_level', 'job_type', 'job_skills']),
    
}

# Connect to your PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Create tables if they don't exist
for table_name, query in [('companies', create_companies_table_query), ('jobs', create_jobs_table_query)]:
    cur.execute(query)
    print(f"'{table_name}' table created.")
# Insert data into tables
for excel_file_path, (table_name, columns) in excel_files.items():
    start_time = time.time()  # Start timing
    
    df = pd.read_excel(excel_file_path)
    placeholders = ','.join(['%s'] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    rows_inserted = 0  # Initialize counter for rows inserted
    for _, row in df.iterrows():
        data = tuple(row[col] if col in row else None for col in columns)
        cur.execute(insert_query, data)
        rows_inserted += 1  # Increment counter
    
    execution_time = time.time() - start_time  # Calculate execution time
    
    # Print message including number of rows inserted and execution time
    print(f"Data inserted into '{table_name}' table: {rows_inserted} rows in {execution_time:.2f} seconds.")
    # Move the print statement here, outside of the inner loop
    print(f"Data inserted into '{table_name}' table.")
# Commit changes and close cursor/connection
conn.commit()
cur.close()
conn.close()
