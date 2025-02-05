"""Outpatients Baseline Data"""

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.op import get_op_df


def get_op_baseline(spark: SparkContext) -> DataFrame:
    """Get Outpatients Baseline Data

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The outpatients baseline data
    :rtype: DataFrame
    """
    return (
        get_op_df(spark)
        .groupBy("fyear", "provider", "group", "tretspef")
        .agg(F.count("fyear").alias("count"))
        .withColumn("activity_type", F.lit("op"))
    )
