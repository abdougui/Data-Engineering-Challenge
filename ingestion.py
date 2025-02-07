import os
import pandas as pd
import logging
from io import StringIO
import requests
import boto3
def load_energy_data(excel_file_path: str) -> dict:
    try:
        xls = pd.ExcelFile(excel_file_path)
        data = {}
        possible_datetime_cols = ["Timestamp", "Date/Time", "Datetime", "DateTime"]
        
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet)
            df.columns = df.columns.str.strip()
            
            datetime_col = None
            for col in df.columns:
                if col.lower() in [p.lower() for p in possible_datetime_cols]:
                    datetime_col = col
                    break
                    
            if datetime_col is None:
                logging.warning("Sheet %s does not have a recognized datetime column; skipping.", sheet)
                continue
            
            if datetime_col != "Timestamp":
                df.rename(columns={datetime_col: "Timestamp"}, inplace=True)
                logging.info("Sheet %s: Renamed datetime column '%s' to 'Timestamp'.", sheet, datetime_col)
            else:
                logging.info("Sheet %s: Using datetime column '%s'.", sheet, datetime_col)
            
            if df.shape[1] < 2:
                logging.warning("Sheet %s does not have at least 2 columns (datetime + measurement); skipping.", sheet)
                continue
            
            data[sheet] = df
            logging.info("Loaded sheet: %s with %d records.", sheet, len(df))
            
        return data
    except Exception as e:
        logging.error("Error loading energy data: %s", str(e))
        raise



def load_carbon_footprint_data(aws_config: dict, file_path: str = None,aws_fetch: bool=False) -> pd.DataFrame:
    try:
        file_path='data/entsoe.csv'
        
        if os.path.exists(file_path):
            logging.info(f"File found locally: {file_path}")
            return pd.read_csv(file_path)
        
        all_data=[]
        for month in range(1, 13):
            for day in range(1, 32):
                if aws_fetch:
                    s3 = boto3.client(
                        's3',
                        aws_access_key_id=aws_config['access_key'],
                        aws_secret_access_key=aws_config['secret_key'],
                        region_name=aws_config.get('region_name', 'us-east-1')
                    )
                    bucket = aws_config['s3_bucket']
                    if not file_path:
                        file_path = f"carbon-footprint/2022/{month}/{day}/entsoe.csv"
                    response = s3.get_object(Bucket=bucket, Key=file_path)
                    content = response['Body'].read().decode('utf-8')
                    df = pd.read_csv(StringIO(content))
                    all_data.append(df)
                else:
                    try:
                        url = f"https://buitrix-challenge-01.s3.amazonaws.com/cfp-data/month={month}/day={day}/entsoe.csv"
                        response = requests.get(url)
                        response.raise_for_status()  # Raise HTTP errors (404, 500, etc.)
                        df = pd.read_csv(StringIO(response.text))
                        # Append to the list
                    except Exception as e:
                        print(e)
                        continue
        logging.info("Loaded carbon footprint data from %s", file_path)
        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            save_dataframe_to_csv(final_df, "data/entsoe.csv")
            return final_df
    except Exception as e:
        logging.error("Error loading carbon footprint data: %s", str(e))
        raise
def save_dataframe_to_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Data saved to {path}")
