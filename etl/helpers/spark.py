from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
import os
from pyspark import SparkConf, SparkContext

load_dotenv()

conf = (
    SparkConf()
    .setAppName("DMCAPSTONE")
    .set(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.1.2,"
        "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,"
        "net.snowflake:snowflake-jdbc:3.13.3",
    )
    .set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
sc = spark.sparkContext


def get_spark_session() -> SparkSession:
    """Get Spark session."""
    return spark


def get_spark_context() -> SparkContext:
    """Get Spark context."""
    return sc


def get_s3_data() -> DataFrame:
    """Get data from S3."""
    # sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    # sc._jsc.hadoopConfiguration().set(
    #     "fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]
    # )
    # sc._jsc.hadoopConfiguration().set(
    #     "fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]
    # )
    return spark.read.json("s3a://dataminded-academy-capstone-resources/raw/open_aq/")
