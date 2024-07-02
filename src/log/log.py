import csv
import os
def log_to_csv(log_msg: dict, filename: str):
    # Check if the file exists
    file_exists = os.path.isfile(filename)

    # Define the column headers
    headers = ["step", "status", "source", "table_name","etl_date"]

    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=headers)

        # Write the header only if the file doesn't exist
        if not file_exists:
            writer.writeheader()

        # Append the log message
        writer.writerow(log_msg)

        