# Import your packages 
import pickle
import boto3
import os
import snowflake.connector

import pandas as pd

from snowflake.connector.pandas_tools import write_pandas
from flask import Flask, Response, jsonify, request

# Declare your applications
app = Flask(__name__)

# Variables for pulling the model from S3
bucket_name = os.environ['DATA_LAKE']
key = os.environ['DATA_LAKE_ROOT_PATH'] + "datascience/model.pkl"

# Pull the model from S3
s3 = boto3.client("s3")
model = pickle.loads(
        s3.get_object(Bucket=bucket_name, Key=key)["Body"].read()
    )

# Create a route called get_house_prices for the estate agency to call
@app.route("/get_house_prices", methods=["POST"])
def custom():
    """
    Create a route that will use the model you have saved to S3 within your Workflow
    """
    payload = request.get_json()
    
    output = model.predict(pd.DataFrame(payload, index = [0]))[0]
    response = "{'predicted_price': £" + f"{round(output):,}" + "}"
    
    return Response(response, status=200)

# Health route to check the API is running
@app.route("/health")
def health():
    return Response("OK", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)