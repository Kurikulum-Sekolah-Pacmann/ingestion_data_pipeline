from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from dotenv import load_dotenv
import os
import csv
from datetime import datetime
from src.log.log import log_to_csv

load_dotenv(".env")

CRED_PATH = os.getenv("CRED_PATH")

def auth_gspread():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    #Define your credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CRED_PATH, scope) # Your json file here

    gc = gspread.authorize(credentials)

    return gc

def init_key_file(key_file:str):
    #define credentials to open the file
    gc = auth_gspread()
    
    #open spreadsheet file by key
    sheet_result = gc.open_by_key(key_file)
    
    return sheet_result

def extract_sheet(key_file:str, worksheet_name: str) -> pd.DataFrame:
    # init sheet
    sheet_result = init_key_file(key_file)
    
    worksheet_result = sheet_result.worksheet(worksheet_name)
    
    df_result = pd.DataFrame(worksheet_result.get_all_values())
    
    # set first rows as columns
    df_result.columns = df_result.iloc[0]
    
    # get all the rest of the values
    df_result = df_result[1:].copy()
    
    return df_result

def extract_spreadsheet(worksheet_name: str, key_file: str):

    try:
        # extract data
        df_data = extract_sheet(worksheet_name = worksheet_name,
                                    key_file = key_file)
        
        # success log message
        log_msg = {
            "step" : "extraction",
            "status": "success",
            "source": "spreadsheet",
            "table_name": worksheet_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
    except Exception as e:
        # fail log message
        log_msg = {
            "step" : "extraction",
            "status": "failed",
            "source": "spreadsheet",
            "table_name": worksheet_name,
            "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp
        }
    finally:
        # load log to csv file
        log_to_csv(log_msg, 'log.csv')
        
    return df_data

