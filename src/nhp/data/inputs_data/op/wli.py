"""Outpatients Waiting List Imbalances"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.inputs_data.op import get_op_df


def get_op_wli(spark: SparkSession) -> DataFrame:
    """Get the inpatients waiting list imbalances data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients WLI data
    :rtype: DataFrame
    """
    return (
        get_op_df(spark)
        .filter(F.col("fyear") >= 201819)
        .groupBy("fyear", "provider", "tretspef")
        .agg(F.sum("attendance").alias("op"))
    )
