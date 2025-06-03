"""Inpatients Waiting List Imbalances"""

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
