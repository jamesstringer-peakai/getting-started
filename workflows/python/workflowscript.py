# First import the packages that will be needed  
import boto3
import pickle
import os
import tempfile
import snowflake.connector

import pandas as pd

from snowflake.connector.pandas_tools import write_pandas
from sklearn.linear_model import LinearRegression

# Set some variables up to use them later
bucket = os.environ['DATA_LAKE'] # For the data lake connection
filepath = os.environ['DATA_LAKE_ROOT_PATH'] + 'datascience/' # For the datalake connection
filename = 'predictions.csv' # Filename for the prediction data 
key_model_save = os.environ['DATA_LAKE_ROOT_PATH'] + "datascience/model.pkl" # Model name


## This function connects you to Snowflake
def connect_to_db():
    conn = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USERNAME'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
            database=os.environ['SNOWFLAKE_DATABASE'], 
            schema=os.environ['SNOWFLAKE_SCHEMA'])
    return conn


def read_from_db(connection_str, query):
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    return df

# Printing statements so we have some logs
print("Querying Snowflake data")

# Query to get all of our raw data from STAGE
query = """
select * from STAGE.HOUSEPRICES
"""
conn = connect_to_db()
df = read_from_db(conn, query)

###
Pre process our data. Here we are:
- removing the prices that are below zero
- removing categorical variables
You can change this script if you want to remove some more values
###

print("Pre-processing data")
df = df[df['price'] > 0]

data = df.drop(['date',
                'street', 
                'city', 
                'statezip',
                'country', 
                'PEAKAUDITCREATEDAT', 
                'REQUEST_ID'], axis=1)

# Building and fitting a Linear Regression model. You can change this
print("Build Model and add predictions to dataset")
model = LinearRegression().fit(data.drop('price', axis=1), data['price'])

# Prediciting on the data we have and appending the column to the orginal df 
predictions = model.predict(data.drop('price', axis=1))
df['predictions'] = predictions

# Altering the datatypes of df, ready to be saved into Snowflake
print("Changing datatypes as table in Snowflake has integers")
df['price'] = df['price'].astype(int)
df['predictions'] = df['predictions'].astype(int)
df['bedrooms'] =  df['bedrooms'].astype(int)
df['bathrooms'] =  df['bathrooms'].astype(int)
df['floors'] =  df['floors'].astype(int)

# Removing some fields we dont need 
df = df.drop(['PEAKAUDITCREATEDAT', 'REQUEST_ID'], axis=1)

# Changing the columns to upper case for Snowflake
df.columns = map(lambda x: str(x).upper(), df.columns)

# Creating some querys to create, delete and copy the prediction from our datalake
print("Querys to create, delete and copy predictions from S3")
create_table_query = """
CREATE OR REPLACE TABLE PUBLISH.HOUSEPRICE_PREDICTIONS (
date varchar(256),
  price integer,
  bedrooms integer,
  bathrooms integer,
  sqftliving integer,
  sqftlot integer,
  floors integer,
  waterfront integer,
  view integer,
  condition integer,
  sqftabove integer,
  sqftbasement integer,
  yrbuilt integer,
  yrrenovated integer,
  street varchar(256),
  city varchar(256),
  statezip varchar(256),
  country varchar(256),
  predictions integer    
)
"""

delete_query = "delete from PUBLISH.HOUSEPRICE_PREDICTIONS"

# Using our connection string to execture these queries
conn.cursor().execute(create_table_query)

conn.cursor().execute(delete_query)

print("Writing predictions data to Snowflake")

success, _, nrows, output = write_pandas(
            conn,
            df,
            table_name="HOUSEPRICE_PREDICTIONS",
            schema="PUBLISH",
            quote_identifiers=False
        )

print("Number of successful rows {}".format(nrows))

# Saving our model to our datalake to use later
print("Save model to s3")
s3 = boto3.client("s3")
with tempfile.TemporaryFile() as fp:
    pickle.dump(model, fp)
    fp.seek(0)
    s3.upload_fileobj(fp, 
                      Bucket=os.environ['DATA_LAKE'],
                      Key=key_model_save)
                      