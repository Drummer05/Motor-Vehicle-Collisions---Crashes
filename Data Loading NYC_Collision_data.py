#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install pyarrow


# In[3]:


"""Import modules """
import pyarrow     
import pandas as pd
import os
from google.cloud import bigquery
from google.oauth2 import service_account


# In[ ]:


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'Censored'


# In[4]:


"""Create a BigQuery Dataset To load the transformed data"""

from google.cloud import bigquery
def create_bigquery_dataset(project_id, dataset_name):
    """Creates a BigQuery dataset."""
    bigquery_client = bigquery.Client(project=project_id)
    dataset_id = f"{project_id}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    bigquery_client.create_dataset(dataset)
    print(f"Dataset {dataset_id} created.")

project_id = 'western-figure-406502'
dataset_name = 'NYC_motor_vehicle_crashes'  
create_bigquery_dataset(project_id, dataset_name)


# In[5]:


"""Create facts and dimension tables in bigquery"""

# Get the path to the service account key file from the environment variable
service_account_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

# Set your Google Cloud credentials using the environment variable
credentials = service_account.Credentials.from_service_account_file(service_account_path)
# Initialize a BigQuery client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Define your dataset and table names
dataset_name = 'NYC_motor_vehicle_crashes'
fact_table_name = 'NYC_Collisions_Fact'
date_dim_table_name = 'Date_Dim'
location_dim_table_name = 'Location_Dim'

# Create the dataset
dataset_ref = client.dataset(dataset_name)
client.get_dataset(dataset_ref)

# Define the schema for the fact table
fact_table_schema = [
    bigquery.SchemaField('COLLISION_ID', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('DATE_ID', 'STRING'),
    bigquery.SchemaField('LOCATION_ID', 'INTEGER'),
    bigquery.SchemaField('NUMBER_OF_PERSONS_INJURED', 'INTEGER'),
    bigquery.SchemaField('NUMBER_OF_PERSONS_KILLED', 'INTEGER'),
    bigquery.SchemaField('NUMBER_OF_PEDESTRIANS_INJURED', 'INTEGER'),
    bigquery.SchemaField('NUMBER_OF_PEDESTRIANS_KILLED', 'INTEGER'),
    bigquery.SchemaField('NUMBER_OF_CYCLIST_INJURED', 'INTEGER'),
    bigquery.SchemaField('NUMBER_OF_CYCLIST_KILLED', 'INTEGER'),
    bigquery.SchemaField('NUMBER_OF_MOTORIST_INJURED', 'INTEGER'),
    bigquery.SchemaField('NUMBER_OF_MOTORIST_KILLED', 'INTEGER'),
    bigquery.SchemaField('CONTRIBUTING_FACTOR_VEHICLE_1', 'STRING'),
    bigquery.SchemaField('CONTRIBUTING_FACTOR_VEHICLE_2', 'STRING'),
    bigquery.SchemaField('CONTRIBUTING_FACTOR_VEHICLE_3', 'STRING'),
    bigquery.SchemaField('CONTRIBUTING_FACTOR_VEHICLE_4', 'STRING'),
    bigquery.SchemaField('CONTRIBUTING_FACTOR_VEHICLE_5', 'STRING'),
    bigquery.SchemaField('VEHICLE_TYPE_CODE_1', 'STRING'),
    bigquery.SchemaField('VEHICLE_TYPE_CODE_2', 'STRING'),
    bigquery.SchemaField('VEHICLE_TYPE_CODE_3', 'STRING'),
    bigquery.SchemaField('VEHICLE_TYPE_CODE_4', 'STRING'),
    bigquery.SchemaField('VEHICLE_TYPE_CODE_5', 'STRING'),
    bigquery.SchemaField('INJURIES', 'BOOL')
]

# Define the schema for the date dimension table
date_dim_table_schema = [
    bigquery.SchemaField('DATE_ID', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('YEAR', 'INTEGER'),
    bigquery.SchemaField('MONTH', 'INTEGER'),
    bigquery.SchemaField('DAY', 'INTEGER'),
    bigquery.SchemaField('DATE', 'DATE'),
    bigquery.SchemaField('CRASH_TIME', 'STRING')
]


# Define the schema for the location dimension table
location_dim_table_schema = [
    bigquery.SchemaField('LOCATION_ID', 'INTEGER', mode='REQUIRED'),
    bigquery.SchemaField('ZIP_CODE', 'INTEGER'),
    bigquery.SchemaField('BOROUGH', 'STRING'),
    bigquery.SchemaField("LATITUDE", "FLOAT"),
    bigquery.SchemaField("LONGITUDE", "FLOAT"),
    bigquery.SchemaField("ON_STREET_NAME", "STRING"),
    bigquery.SchemaField("CROSS_STREET_NAME", "STRING"),
    bigquery.SchemaField("OFF_STREET_NAME", "STRING")
]

# Create the tables
fact_table_ref = dataset_ref.table(fact_table_name)
try:
    client.get_table(fact_table_ref)
    print(f"Table {fact_table_name} already exists in the dataset {dataset_name}.")
except:
    fact_table = bigquery.Table(fact_table_ref, schema=fact_table_schema)
    client.create_table(fact_table)
    print(f"{fact_table_name} Created")

date_dim_table_ref = dataset_ref.table(date_dim_table_name)
try:
    client.get_table(date_dim_table_ref)
    print(f"Table {date_dim_table_name} already exists in the dataset {dataset_name}.")
except:
    date_dim_table = bigquery.Table(date_dim_table_ref, schema=date_dim_table_schema)
    client.create_table(date_dim_table)
    print(f"{date_dim_table_name} Created")
    
location_dim_table_ref = dataset_ref.table(location_dim_table_name)
try:
    client.get_table(location_dim_table_ref)
    print(f"Table {location_dim_table_name} already exists in the dataset {dataset_name}.")
except: 
    location_dim_table = bigquery.Table(location_dim_table_ref, schema=location_dim_table_schema)
    client.create_table(location_dim_table)
    print(f"{location_dim_table_name} Created")


# In[6]:


# Define the file path
file_path = 'C:\\Users\\Carlos\\Desktop\\NYC_Crash_Project\\NYC_motor_vehicle_crashes.csv'

# Read the clean CSV file into a DataFrame
df = pd.read_csv(file_path)


# In[8]:


# Function to upload data to BigQuery from a DataFrame
def upload_data_from_dataframe(df, table_ref):
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

def split_df(df):
    fact_cols = [
        "collision_id", "date_id", "location_id", 
        "number_of_persons_injured", "number_of_persons_killed", 
        "number_of_pedestrians_injured", "number_of_pedestrians_killed", 
        "number_of_cyclist_injured", "number_of_cyclist_killed", 
        "number_of_motorist_injured", "number_of_motorist_killed", 
        "contributing_factor_vehicle_1", "contributing_factor_vehicle_2", 
        "contributing_factor_vehicle_3", "contributing_factor_vehicle_4", 
        "contributing_factor_vehicle_5", "vehicle_type_code1", 
        "vehicle_type_code2", "vehicle_type_code_3", 
        "vehicle_type_code_4", "vehicle_type_code_5", "injuries"]
    
    date_cols = [
        "date_id", "year", "month", "day", "crash_date", "crash_time"]

    location_cols = [
        "location_id", "zip_code", "borough", "latitude", 
        "longitude", "on_street_name", "cross_street_name", "off_street_name"]

    # Assuming the DataFrame 'df' has columns in uppercase, convert them to lowercase
    df.columns = df.columns.str.lower()

    # Now, split the DataFrame based on the updated column lists
    fact_df = df[fact_cols]
    date_dim_df = df[date_cols]
    location_dim_df = df[location_cols]

    # Return the split DataFrames
    return fact_df, date_dim_df, location_dim_df

fact_df, date_dim_df, location_dim_df = split_df(df)

# Upload the data to BigQuery
upload_data_from_dataframe(fact_df, fact_table_ref)
upload_data_from_dataframe(date_dim_df, date_dim_table_ref)
upload_data_from_dataframe(location_dim_df, location_dim_table_ref)

