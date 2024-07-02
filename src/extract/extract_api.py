import pandas as pd
from dotenv import load_dotenv
import requests
from datetime import datetime
from src.log.log import log_to_csv

def extract_api(link_api:str, list_parameter:dict, data_name):
    try:
        # Establish connection to API
        resp = requests.get(link_api, params=list_parameter)

        # Parse the response JSON
        raw_response = resp.json()

        # Convert the JSON data to a pandas DataFrame
        df_api = pd.DataFrame(raw_response)

        # create success log message
        log_msg = {
                "step" : "extraction",
                "status": "success",
                "source": "api",
                "table_name": data_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return df_api

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the API request: {e}")

        # create fail log message
        log_msg = {
                "step" : "extraction",
                "status": "failed",
                "source": "api",
                "table_name": data_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return pd.DataFrame()
    

    except ValueError as e:
        print(f"An error occurred while parsing the response JSON: {e}")

        # create fail log message
        log_msg = {
                "step" : "extraction",
                "status": "failed",
                "source": "api",
                "table_name": data_name,
                "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
            }
        return pd.DataFrame()
    
    finally:
        log_to_csv(log_msg, 'log.csv')