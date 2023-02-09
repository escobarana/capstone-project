from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col, when, to_utc_timestamp


def clean_dataframe(df: DataFrame) -> DataFrame:
    """Clean the dataframe and return a new dataframe."""

    df = (
        df.withColumn("sensorType", split(col("sensorType"), " ").getItem(0))
        .withColumn("value", df.value.cast("float"))
        .withColumn("locationId", df.locationId.cast("int"))
        .withColumn("isAnalysis", df.isAnalysis.cast("boolean"))
        .withColumn("isMobile", df.isMobile.cast("boolean"))
        .withColumn("latitude", df.coordinates.latitude)
        .withColumn("longitude", df.coordinates.longitude)
        .withColumn("local_date", df.date.local.cast("timestamp"))
        .withColumn("utc_date", to_utc_timestamp(df.date.utc, "PST"))
        .drop("coordinates", "date")
    )

    # Replace empty strings with None in all columns of the dataframe
    df = df.select(
        [when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df.columns]
    )

    return df
