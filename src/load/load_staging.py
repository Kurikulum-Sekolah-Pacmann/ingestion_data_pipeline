from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

from pangres import upsert

# create modul log_to_csv from previouse section
from src.log.log import log_to_csv
from src.load.load_error import handle_error

def load_staging(data, schema:str, table_name: str, idx_name:str, source):
    try:
        # create connection to database
        conn = create_engine("postgresql://postgres:aku@localhost/staging")
        
        # set data index or primary key
        data = data.set_index(idx_name)
        
        # Do upsert (Update for existing data and Insert for new data)
        upsert(con = conn,
                df = data,
                table_name = table_name,
                schema = schema,
                if_row_exists = "update")
        
        #create success log message
        log_msg = {
                "step" : "load staging",
                "status": "success",
                "source": source,
                "table_name": table_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return data
    except Exception as e:

        #create fail log message
        log_msg = {
            "step" : "load staging",
            "status": "failed",
            "source": source,
            "table_name": table_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }

        # Handling error: save data to Object Storage
        try:
            handle_error(data = data, bucket_name='error', table_name= table_name)
        except Exception as e:
            print(e)
    finally:
        log_to_csv(log_msg, 'log.csv')

    