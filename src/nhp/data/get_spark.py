"""Helper method to get the spark session."""

import enum

from pyspark.sql import SparkSession


class PartitionOverwriteMode(enum.Enum):
    DYNAMIC = "dynamic"
    STATIC = "static"


def get_spark(
    partition_overwrite_mode: PartitionOverwriteMode = PartitionOverwriteMode.DYNAMIC,
) -> SparkSession:
    """Get spark session

    :return: get the spark session to use
    :rtype: SparkSession
    """
    # if you load databricks.connect at module level, if you aren't running on
    # databricks you can get errors
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode", partition_overwrite_mode.value
    )
    return spark
