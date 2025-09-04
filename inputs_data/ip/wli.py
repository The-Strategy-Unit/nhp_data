"""Inpatients Waiting List Imbalances"""

# when running on databricks, we might need to change directory in order to
# import things correctly
import os

if not os.path.exists("readme.md"):
    os.chdir("..")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from inputs_data.ip import get_ip_df


def get_ip_wli(spark: SparkSession) -> DataFrame:
    """Get the inpatients waiting list imbalances data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients WLI data
    :rtype: DataFrame
    """
    return (
        get_ip_df(spark)
        .filter(F.col("admimeth") == "11")
        .filter(F.col("fyear") >= 201819)
        .groupBy("fyear", "provider", "tretspef")
        .agg(F.count("tretspef").alias("ip"))
    )
