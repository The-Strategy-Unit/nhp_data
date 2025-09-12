"""Helper method to get the spark session."""

import os

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


def get_spark(schema: str, catalog: str | None = None) -> SparkSession:
    """Get spark session

    :param schema: which schema to use
    :type schema: str
    :param catalog: catalog to use, defaults to the environment variable "nhp_catalog" (or, "nhp" if
      that environment variable is not set)
    :type catalog: str | None, optional

    :return: get the spark session to use
    :rtype: SparkSession
    """

    spark: SparkSession = DatabricksSession.builder.getOrCreate()
    spark.catalog.setCurrentCatalog(catalog or os.environ.get("nhp_catalog", "nhp"))
    spark.catalog.setCurrentDatabase(schema)
    return spark
