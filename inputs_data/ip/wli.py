"""Inpatients Waiting List Imbalances"""

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from inputs_data.ip import get_ip_df


def get_ip_wli(spark: SparkContext) -> DataFrame:
    """Get the inpatients waiting list imbalances data

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients WLI data
    :rtype: DataFrame
    """
    return (
        get_ip_df(spark)
        .filter(F.col("admimeth") == "11")
        .groupBy("fyear", "provider", "tretspef")
        .agg(F.count("tretspef").alias("ip"))
    )
