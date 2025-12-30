import os
from dotenv import load_dotenv 
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

engine = create_engine('postgresql://admin:password123@localhost:5432/amazon_sales')

def download_data():
    os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME')
    os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY')

    print("Authenticating with Kaggle API...")
    print("Connecting to Kaggle...")
    api = KaggleApi()
    api.authenticate()
    
    dataset_id = 'karkavelrajaj/amazon-sales-dataset'
    print(f"Downloading {dataset_id}...")
    
    api.dataset_download_files(dataset_id, path='.', unzip=True)
    print("Download and Unzip complete!")

def load_to_postgres():
    df = pd.read_csv('amazon.csv')
    print(f"Loading {len(df)} rows to PostgreSQL...")
    
    df.to_sql('stg_amazon_sales', engine, if_exists='replace', index=False)
    print("Data loaded successfully to staging table!")

if __name__ == "__main__":
    try:
        download_data()
        load_to_postgres()
    except Exception as e:
        print(f"An error occurred: {e}")