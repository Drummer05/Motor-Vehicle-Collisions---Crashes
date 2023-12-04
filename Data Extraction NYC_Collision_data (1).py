#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Extract the data

# URL of the CSV file
url = 'https://data.cityofnewyork.us/resource/h9gi-nx95.csv'

# Make a GET request to the URL
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Read the content of the response into a pandas DataFrame
    data = StringIO(response.text)
    df = pd.read_csv(data)

    # Truncate the DataFrame to the first 10,000 rows
    df = df.head(10000)

    # Save the truncated DataFrame to a CSV file
    df.to_csv('NYC_Collisions.csv', index=False)
    print("First 10,000 rows saved to NYC_Collisions.csv")
else:
    print("Failed to retrieve data: Status code", response.status_code)


# In[ ]:


#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

import pandas as pd
from sodapy import Socrata

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.cityofnewyork.us", None)

# Example authenticated client (needed for non-public datasets):
# client = Socrata(data.cityofnewyork.us,
#                  MyAppToken,
#                  username="user@example.com",
#                  password="AFakePassword")

# First 2000 results, returned as JSON from API / converted to Python list of
# dictionaries by sodapy.
results = client.get("h9gi-nx95",limit=200000)

# Convert to pandas DataFrame
results_df = pd.DataFrame.from_records(results)

# Specify the file path for the CSV
csv_file_path = 'C:\\Users\\Carlos\\Desktop\\New folder\\NYC_Collision.csv'

# Save the DataFrame as a CSV file
results_df.to_csv(csv_file_path, index=False)

print(f"Data saved as CSV at {csv_file_path}")


# In[ ]:


## create a bucket in google cloud
def create_bucket(bucket_name, project_id):
    """Creates a new bucket in a specific project."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    new_bucket = storage_client.create_bucket(bucket, project=project_id)
    print(f"Bucket {new_bucket.name} created")
    
# Create a new bucket
bucket_name = 'motor_vehicle_collisions'
create_bucket(bucket_name, project_id)


# In[ ]:


# import the data to a buckect
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")
    

bucket_name = 'motor_vehicle_collisions'
source_file_name = 'C:\\Users\\Carlos\\Desktop\\NYC Collition\\NYC_Collision.csv'
destination_blob_name = 'motor_vehicle_collisions_1.csv'

# Upload the CSV file
upload_blob(bucket_name, source_file_name, destination_blob_name)


# In[ ]:


# get the data to the bucket in google cloud
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")
    

bucket_name = 'motor_vehicle_collisions'
source_file_name = 'C:\\Users\\Carlos\\Desktop\\NYC Collition\\NYC_Collision.csv'

# Upload the CSV file
upload_blob(bucket_name, source_file_name, destination_blob_name)


# In[ ]:




