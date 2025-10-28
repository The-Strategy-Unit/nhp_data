"""Helper method to get the spark session."""

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get spark session

    :return: get the spark session to use
    :rtype: SparkSession
    """
    spark = DatabricksSession.builder.getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark
