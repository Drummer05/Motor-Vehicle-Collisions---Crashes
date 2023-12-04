#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Packages 
import pyarrow
from google.cloud import storage
import pandas as pd
import io
import hashlib
import os


# In[ ]:


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "CENSORED"


# In[ ]:


# Replace with your GCS bucket and blob name
bucket_name = 'motor_vehicle_collisions'
source_blob_name = 'motor_vehicle_collisions_1.csv'

# Initialize a storage client
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

# Create a function to download the CSV file from GCS into memory
def download_blob_to_memory(bucket_name, source_blob_name):
    blob = bucket.blob(source_blob_name)
    data = blob.download_as_bytes()
    return io.BytesIO(data)

# Download the CSV file from GCS into memory
csv_memory = download_blob_to_memory(bucket_name, source_blob_name)

# Load the CSV data into a Pandas DataFrame
df = pd.read_csv(csv_memory)
# Replace spaces with underscores in the column names
df.columns = df.columns.str.replace(' ', '_')

# 1. Unified date format YYYY-MM-DD
df['crash_date'] = pd.to_datetime(df['crash_date']).dt.strftime('%Y-%m-%d')

# 2. Splitting the date into multiple units (Year, Month, Day)
df['year'] = pd.to_datetime(df['crash_date']).dt.year
df['month'] = pd.to_datetime(df['crash_date']).dt.month
df['day'] = pd.to_datetime(df['crash_date']).dt.day

# 3. Removing NULL values
# For demonstration, we will replace NaN values in 'ZIP_CODE' with a placeholder value (8888)
df['zip_code'].fillna(88888, inplace=True)

# 4. Removing Duplicate rows
df.drop_duplicates(inplace=True)

# 5. Verify Data against data reference (ZIP codes should be integers)
df['zip_code'] = df['zip_code'].astype(int)

# 6. Correct data types for new facts generated
# As an example, we convert 'COLLISION_ID' to a string, as it is a unique identifier and not a numerical value
df['collision_id'] = df['collision_id'].astype(str)

# 7. Adding one or many columns
# Add a column indicating whether an accident resulted in injuries or not
df['injuries'] = df['number_of_persons_injured'] > 0

def create_location_id(row):
    # Create a unique hash for each location based on ZIP_CODE and BOROUGH
    # You can include more fields if needed
    hasher = hashlib.sha1()
    hasher.update(str(row['zip_code']).encode('utf-8'))
    hasher.update(str(row['borough']).encode('utf-8'))
    # Return the first 10 characters of the hash as the location ID
    return hasher.hexdigest()[:10]

# Create a DATE_ID using the YYYYMMDD format
df['date_id'] = df['year'].astype(str) + \
                                df['month'].astype(str).str.zfill(2) + \
                                df['day'].astype(str).str.zfill(2)

# Create a TIME_ID using the HHMM format (assuming you have a 'CRASH_TIME' column in HH:MM format)
df['time_id'] = df['crash_time'].str.replace(':', '')

# Create a LOCATION_ID using a combination of ZIP_CODE and BOROUGH
df['location_id'] = df.apply(create_location_id, axis=1)


# In[ ]:


print(df.columns)


# In[ ]:


# take a look at the data
df.head()


# In[ ]:


def convert_columns_to_proper_case(df, columns):
    """
    Convert specified columns in the DataFrame to proper case.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the columns to be converted.
    columns (list): List of column names to convert to proper case.

    Returns:
    pandas.DataFrame: DataFrame with the specified columns in proper case.
    """
    for column in columns:
        if column in df.columns:
            df[column] = df[column].astype(str).str.title()
        else:
            print(f"Column '{column}' not found in DataFrame.")
    return df

columns_to_convert = [
    'borough', 'on_street_name', 'off_street_name', 'cross_street_name', 
    'contributing_factor_vehicle_1', 'contributing_factor_vehicle_2', 
    'contributing_factor_vehicle_3', 'contributing_factor_vehicle_4', 
    'contributing_factor_vehicle_5'
]

df = convert_columns_to_proper_case(df, columns_to_convert)
print(df)


# In[ ]:


"""Check if the DataFrame has any duplicate rows"""

def check_for_duplicates(df):
    if df.duplicated().any():
        print("There are duplicate rows in the DataFrame.")
        return True
    else:
        print("There are no duplicate rows in the DataFrame.")
        return False
check_for_duplicates(df)


# In[ ]:


"""Check fo Null Values"""
   
# A lot of null Values found
null_counts = df.isnull().sum()
print(null_counts)


# In[ ]:


"""Create a new column that contains the name of the months"""
# Mapping from numeric months to month names
month_mapping = {
    1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun',
    7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'
}

# Apply the mapping to create a new column
df['month_name'] = df['month'].map(month_mapping)


# In[ ]:


"""create a new column that contains the name of the seasons"""
# Function to map month to season
def map_month_to_season(month):
    if month in [12, 1, 2]:
        return 'Winter'
    elif month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    elif month in [9, 10, 11]:
        return 'Fall'

# Apply the function to create a new 'season' column
df['season'] = df['month'].apply(map_month_to_season)


# In[ ]:


""" Create a new column with the time of the day"""
# Function to extract hour from time string and map to part of the day
def map_time_to_part_of_day(time_str):
    # Extract the hour part and convert to integer
    hour = int(time_str.split(':')[0])
    if 5 <= hour <= 11:
        return 'Morning'
    elif 12 <= hour <= 16:
        return 'Afternoon'
    elif 17 <= hour <= 20:
        return 'Evening'
    else:
        return 'Night'

# Apply the function to create a new 'time_of_day' column
df['time_of_day'] = df['crash_time'].apply(map_time_to_part_of_day)


# In[ ]:


# Export the clean data 

df.to_csv('C:\\Users\\Carlos\\Desktop\\NYC_Crash_Project\\NYC_motor_vehicle_crashes.csv', index=False)

