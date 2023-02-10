import argparse

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import split, col, when, to_utc_timestamp
from conveyorana.common.spark import ClosableSparkSession, transform, SparkLogger
import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

DataFrame.transform = transform


def main():
    parser = argparse.ArgumentParser(description="conveyorana")
    parser.add_argument(
        "-d", "--date", dest="date", help="date in format YYYY-mm-dd", required=True
    )
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=True
    )
    args = parser.parse_args()

    with ClosableSparkSession("conveyorana") as session:
        run(session, args.env, args.date)


def run(spark: SparkSession, environment: str, date: str):
    """Main ETL script definition.

    :return: None
    """
    # execute ETL pipeline
    logger = SparkLogger(spark)
    logger.info(f"Executing job for {environment} on {date}")
    data = extract_data(spark, date)
    transformed = transform_data(data, date)
    load_data(spark, transformed)


def extract_data(spark: SparkSession, date: str) -> DataFrame:
    """Load data from S3 Bucket.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    return spark.read.json("s3a://dataminded-academy-capstone-resources2/raw/open_aq/")


def transform_data(data: DataFrame, date: str) -> DataFrame:
    """Transform original dataset.

    :param data: Input DataFrame.
    :param date: The context date
    :return: Transformed DataFrame.
    """
    data = (
        data.withColumn("sensorType", split(col("sensorType"), " ").getItem(0))
        .withColumn("value", data.value.cast("float"))
        .withColumn("locationId", data.locationId.cast("int"))
        .withColumn("isAnalysis", data.isAnalysis.cast("boolean"))
        .withColumn("isMobile", data.isMobile.cast("boolean"))
        .withColumn("latitude", data.coordinates.latitude)
        .withColumn("longitude", data.coordinates.longitude)
        .withColumn("local_date", data.date.local.cast("timestamp"))
        .withColumn("utc_date", to_utc_timestamp(data.date.utc, "PST"))
        .drop("coordinates", "date")
    )

    # Replace empty strings with None in all columns of the dataframe
    data = data.select(
        [when(col(c) == "", None).otherwise(col(c)).alias(c) for c in data.columns]
    )

    return data


def get_snowflake_credentials() -> dict:
    """Stored on AWS Secretsmanager under the following secret snowflake/capstone/login."""
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=os.environ["SNOWFLAKE_SECRET_ID"])

    return json.loads(response["SecretString"])


def load_data(spark: SparkSession, data: DataFrame):
    """Writes the output dataset to some destination

    :param data: DataFrame to write.
    :return: None
    """
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
    data.write.format(SNOWFLAKE_SOURCE_NAME).options(**snowflake_options).option(
        "dbtable", "openaq"
    ).options(header=True).mode("overwrite").save()
    data.show()


if __name__ == "__main__":
    main()
