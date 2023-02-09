from etl.scripts.transform import clean_dataframe
from helpers.spark import get_spark_session, get_s3_data
from helpers.snowflake import load_data_snowflake, get_snowflake_credentials

spark = get_spark_session()


def main():
    """Main ETL script definition."""
    # Get data from S3
    data = get_s3_data()
    # Transform data
    cleaned_df = clean_dataframe(data)
    # cleaned_df.show()
    # cleaned_df.printSchema()
    # Load data to Snowflake
    load_data_snowflake(cleaned_df, "open_aq")


if __name__ == "__main__":
    main()
