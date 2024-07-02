from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import csv
from datetime import datetime
from src.log.log import log_to_csv


def extract_database(table_name: str): 
    
    try:
        # create connection to database
        conn = create_engine("postgresql://postgres:aku@localhost/mini_order")

        log = pd.read_csv("log.csv")

        # Get date from previous process
        condition = (
            (log['step'] == 'extraction') &
            (log['status'] == 'success') &
            (log['source'] == 'database') &
            (log['table_name'] == table_name)
        )

        # Apply the filter
        etl_date = log[condition]['etl_date']

        # If no previous extraction has been recorded (etl_date is empty), set etl_date to '1111-01-01' indicating the initial load.
        # Otherwise, retrieve data added since the last successful extraction (etl_date).
        if(etl_date.empty):
            etl_date = '1111-01-01'
        else:
            etl_date = max(etl_date)

        # Constructs a SQL query to select all columns from the specified table_name where created_at is greater than etl_date.
        query = f"SELECT * FROM {table_name} WHERE created_at > %s::timestamp"

        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=query, con=conn, params=(etl_date,))
        log_msg = {
                "step" : "extraction",
                "status": "success",
                "source": "database",
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return df
    except Exception as e:
        log_msg = {
            "step" : "extraction",
            "status": "failed",
            "source": "database",
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
    finally:
        log_to_csv(log_msg, 'log.csv')

    
