from google.cloud import bigquery
import sys
import pandas as pd
from datetime import datetime

# Get list of CSV files from command line arguments (MUST be in UTF-8)
csv_files = sys.argv[1:]

# Save your service account keys as this
key_path = 'service_account.json'

# Create column names
columnNames = {
    '#': 'number',
    'Keyword': 'keyword',
    'Country': 'country',
    'Difficulty': 'kd',
    'Volume': 'volume',
    'CPC': 'cpc',
    'CPS': 'cps',
    'Parent Keyword': 'parent_kw',
    'Last Update': 'last_update',
    'SERP Features': 'serp_features',
    'Global volume': 'global_volume',
    'Traffic potential': 'traffic_potential',
}

# Create an empty list to store dataframes
dfs = []

# Loop through CSV files
for i, file in enumerate(csv_files):
    df = pd.read_csv(file)
    # If this is the first file, keep the header row
    if i == 0:
        header_row = df.columns
    # Otherwise, drop the header row
    else:
        df = df.drop(0)

    dfs.append(df)

# Concatenate dataframes into a single dataframe
df = pd.concat(dfs, ignore_index=True)

# Rename columns for BQ import
df = df.rename(columns=columnNames)

# Convert last_update column to datetime64 and make BigQuery friendly date
df['last_update'] = df['last_update'].astype('datetime64[ns]')
df['last_update'] = df['last_update'].dt.strftime('%Y-%m-%d')
df['last_update'] = df['last_update'].astype('datetime64[ns]')

# Add a timestamp to keep track of upload date
df['timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

# Initialize the BigQuery client with your service account
client = bigquery.Client.from_service_account_json(key_path)

# BigQuery table info
project_id = 'high-end-hideaways-377513'
dataset_id = 'kw_analysis'
table_id = 'cabin_kw_data'

schema = []
for col in df.columns:
    if df[col].dtype == "object":
        schema.append(bigquery.SchemaField(col, "STRING"))
    elif df[col].dtype == "int64":
        schema.append(bigquery.SchemaField(col, "INTEGER"))
    elif df[col].dtype == "float64":
        schema.append(bigquery.SchemaField(col, "FLOAT"))
    elif df[col].dtype == "datetime64[ns]":
        schema.append(bigquery.SchemaField(col, "DATETIME"))

# Check if the table exists
table_ref = client.dataset(dataset_id).table(table_id)
try:
    client.get_table(table_ref)
    print(f'Table {table_id} already exists.')
    # If the table exists, append the DF
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config)
    job.result()
    print(f'DF appended to table {table_id}.')
except:
    # If the table does not exist, create it and append the CSV to it skipping the header row
    print(f'Table {table_id} does not exist. Creating it now.')
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f'Table {table_id} created.')
    job_config = bigquery.LoadJobConfig()
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()
    print(f'DF appended to table {table_id}.')
