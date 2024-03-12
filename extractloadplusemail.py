import psycopg2
import pandas as pd
import time
import http.client
import json
import base64

# Define the function to send an email via Mailjet
def send_mailjet_email(api_key, api_secret, from_email, to_email, subject, text_content, html_content):
    conn = http.client.HTTPSConnection("api.mailjet.com")
    payload = json.dumps({
        "Messages": [{
            "From": {
                "Email": from_email,
            },
            "To": [{
                "Email": to_email,
            }],
            "Subject": subject,
            "TextPart": text_content,
            "HTMLPart": html_content,
        }]
    })

    credentials = f"{api_key}:{api_secret}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Basic {encoded_credentials}'
    }

    conn.request("POST", "/v3.1/send", payload, headers)
    res = conn.getresponse()
    data = res.read().decode("utf-8")
    print(data)

# Database connection parameters
db_params = {
    'database': 'linkedin1',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost'
}

# Create tables queries
create_companies_table_query = """
CREATE TABLE IF NOT EXISTS companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    company_location VARCHAR(255) NOT NULL
);
"""

create_jobs_table_query = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id SERIAL PRIMARY KEY,
    company_id INT NOT NULL,
    job_title VARCHAR(255) NOT NULL,
    job_link VARCHAR(255) NOT NULL,
    job_date DATE NOT NULL,
    job_location VARCHAR(255) NOT NULL,
    job_level VARCHAR(255),
    job_type VARCHAR(255),
    job_skills VARCHAR(10000),
    FOREIGN KEY (company_id) REFERENCES companies(company_id)
);
"""

# Connect to your PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

total_rows_inserted = 0  # Total rows inserted across all tables

try:
    cur.execute(create_companies_table_query)
    cur.execute(create_jobs_table_query)
    print("Tables created successfully.")

    start_time = time.time()  # Start timing

    # Define the path to your Excel files and the corresponding table names and columns
    excel_files = {
         'C:/Users/nalan_7isj07o/python/practiceex/ex1/extractload/company_list.xlsx': ('companies', ['company_id', 'company_name', 'company_location']),
         'C:/Users/nalan_7isj07o/python/practiceex/ex1/extractload/jobs_list.xlsx': ('jobs', ['job_id', 'company_id', 'job_title', 'job_link', 'job_date', 'job_location', 'job_level', 'job_type', 'job_skills']),
   
    }

    for file_path, (table_name, columns) in excel_files.items():
        df = pd.read_excel(file_path)
        for _, row in df.iterrows():
            data = tuple(row[col] for col in columns)
            cur.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})", data)
            total_rows_inserted += 1

    conn.commit()  # Commit the transaction
    execution_time = time.time() - start_time  # Calculate execution time

    success_message = f"All data has been successfully inserted. Total rows inserted: {total_rows_inserted} to {table_name}. Execution time: {execution_time:.2f} seconds."
    print(success_message)
    send_mailjet_email('4658484b3d687a4f5c7772d18a9522dc', 'e30d0068eab722621913c61396205b32', 'keshya.gk@gmail.com', 'g.krish93@gmail.com', "Data Insertion Successful", success_message, f"<p>{success_message}</p>")

except Exception as e:
    conn.rollback()  # Roll back in case of error
    error_message = f"Failed to insert data. Error: {e}"
    print(error_message)
    send_mailjet_email('4658484b3d687a4f5c7772d18a9522dc', 'e30d0068eab722621913c61396205b32', 'keshya.gk@gmail.com', 'g.krish93@gmail.com', "Data Insertion Failed", error_message, f"<p>{error_message}</p>")
    raise e  # Optionally re-raise the exception

finally:
    cur.close()
    conn.close()
