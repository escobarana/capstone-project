import os

import boto3
import json

from dotenv import load_dotenv
from pyspark.sql import DataFrame

load_dotenv()

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"


def get_snowflake_credentials() -> dict:
    """Stored on AWS Secretsmanager under the following secret snowflake/capstone/login."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=os.environ["SNOWFLAKE_SECRET_ID"])

    return json.loads(response["SecretString"])


def load_data_snowflake(df: DataFrame, table_name: str) -> None:
    """Load data to Snowflake."""
    credentials = get_snowflake_credentials()
    snowflake_options = {
        "sfUrl": credentials["URL"],
        "sfUser": credentials["USER_NAME"],
        "sfPassword": credentials["PASSWORD"],
        "sfDatabase": credentials["DATABASE"],
        "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
        "sfWarehouse": credentials["WAREHOUSE"],
        "sfRole": credentials["ROLE"],
    }
    df.write.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_options).option(
        "dbtable", table_name
    ).options(header=True).mode("overwrite").save()
