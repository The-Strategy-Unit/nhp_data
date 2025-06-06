"""Outpatients Baseline Data"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from inputs_data.op import get_op_df


def get_op_baseline(spark: SparkSession) -> DataFrame:
    """Get Outpatients Baseline Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients baseline data
    :rtype: DataFrame
    """
    return (
        get_op_df(spark)
        .groupBy("fyear", "provider", "group", "tretspef")
        .agg(F.count("fyear").alias("count"))
        .withColumn("activity_type", F.lit("op"))
    )
