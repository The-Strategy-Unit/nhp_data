"""Helper method to get the spark session."""

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get spark session

    :return: get the spark session to use
    :rtype: SparkSession
    """
    # if you load databricks.connect at module level, if you aren't running on
    # databricks you can get errors
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark
