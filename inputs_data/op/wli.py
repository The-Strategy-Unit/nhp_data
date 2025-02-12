"""Outpatients Waiting List Imbalances"""

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from inputs_data.op import get_op_df


def get_op_wli(spark: SparkContext) -> DataFrame:
    """Get the inpatients waiting list imbalances data

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients WLI data
    :rtype: DataFrame
    """
    return (
        get_op_df(spark)
        .filter(F.col("fyear") >= 201819)
        .groupBy("fyear", "provider", "tretspef")
        .agg(F.sum("attendance").alias("op"))
    )
