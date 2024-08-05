# Week 3 and Week 4

## Data Ingestion (Week 3)
Materi 5 and 6 Course Data Pipeline
- Data Extraction
- Data Loading

## Data Transformation and Validation (Week 4)
Materi 7 Course Data Pipeline
- Data Transformation
- Data Validation

## Tools and Technologies:
- Python: For build Data Pipeline
- PostgreSQL: For log, staging and final data storage.
- MinIO: For load failed data.
- Docker: For running MinIO

## Preparation
1. Dataset
   1. Restore Data Database [Link](https://drive.google.com/drive/folders/1ED0sg2AZNH_Kl5Pb1cBUufnPCphpM21R)
   2. Dupplicate Spreadsheet [link](https://docs.google.com/spreadsheets/d/1354yIiiX5peKRL4fbTC1aVA40bqgsxg3fA1zPP0e3uQ/edit?usp=drive_link)
   3. Check Data API [link](https://api-order-teal.vercel.app/api/dummydata?page=2&start_date=2020-01-01&end_date=2023-01-31)
2. Database
   1. Create Staging Database (notebook mater6.ipynb)
   3. Create Warehouse Database (notebook mater7.ipynb)
3. Porject
   1. Save Your Credential Google Service Account
   2. Prepare Your MiniO (Access Key, Secreet Key, Bucket Name: "error-dellstore")
   4. create your .env

      ```
      DB_HOST="localhost"
      DB_USER="YOUR POSTGRES USER"
      DB_PASS="YOUR POSGRES PASS"

      DB_NAME_SOURCE="dellstore"
      DB_NAME_STG="staging"
      DB_SHCHEMA_STG="staging"
      DB_NAME_log="etl_log"

      CRED_PATH='YOUR_PATH/creds/file.json'
      MODEL_PATH='YOUR_PATH/models/'
      KEY_SPREADSHEET="YOUR SPREADSHEET KEY"

      ACCESS_KEY_MINIO = 'YOUR MINIO ACCESS KEY'
      SECRET_KEY_MINIO = 'YOUR MINIO SECRET KEY'

      ```

