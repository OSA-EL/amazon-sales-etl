import os
import pandas as pd
from sqlalchemy import create_engine
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv

# --- Configuration & Initialization ---
load_dotenv()
# Database connection string: postgresql://username:password@host:port/database
engine = create_engine('postgresql://admin:password123@localhost:5432/amazon_sales')

def extract():
    """
    Step 1: Extraction Phase
    Connects to Kaggle API and downloads the latest version of the dataset.
    """
    print("Step 1: Extracting data from Kaggle...")
    api = KaggleApi()
    api.authenticate()
    
    dataset_id = 'karkavelrajaj/amazon-sales-dataset'
    api.dataset_download_files(dataset_id, path='.', unzip=True)
    
    print("Extraction Complete: Files downloaded and unzipped.")
    return "amazon.csv"

def transform(file_path):
    """
    Step 2: Transformation Phase (Data Cleaning)
    Cleans prices, ratings, and handles missing values based on Kaggle schema.
    """
    print("Step 2: Transforming data in memory...")
    df = pd.read_csv(file_path)
    
    # 1. Cleaning Prices: Removing currency symbols and converting to float
    for col in ['actual_price', 'discounted_price']:
        df[col] = df[col].str.replace('₹', '').str.replace(',', '').astype(float)
    
    # 2. Cleaning Discounts: Converting '50%' to 0.50
    df['discount_percentage'] = df['discount_percentage'].str.replace('%', '').astype(float) / 100
    
    # 3. Cleaning Ratings: Handling non-numeric ratings and commas in count
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce').fillna(0)
    # Some rows might have commas in rating_count (e.g., '1,200')
    df['rating_count'] = df['rating_count'].astype(str).str.replace(',', '').astype(float).fillna(0)
    
    # 4. Handling Missing Descriptions: Using the correct column name 'about_product'
    if 'about_product' in df.columns:
        df['about_product'] = df['about_product'].fillna('No description available')
    
    # 5. Deduplication: Ensuring unique products only
    df = df.drop_duplicates(subset=['product_id'])
    
    print(f"Transformation Complete: {len(df)} unique records processed.")
    return df

def load(df):
    """
    Step 3: Loading Phase
    Establishes connection with PostgreSQL and inserts the cleaned DataFrame into the analytics table.
    """
    print("Step 3: Loading cleaned data to PostgreSQL...")
    # 'replace' ensures the table is updated with the latest cleaned data on each run
    df.to_sql('analytics_amazon_sales', engine, if_exists='replace', index=False)
    print("Load Complete: Data is now available in 'analytics_amazon_sales' table.")

# --- Main Execution Pipeline ---
if __name__ == "__main__":
    try:
        # Run the ETL pipeline sequentially
        raw_file = extract()
        clean_df = transform(raw_file)
        load(clean_df)
        
        print("\n" + "="*30)
        print("ETL PIPELINE EXECUTED SUCCESSFULLY")
        print("="*30)
        
    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")