"""Outpatients Baseline Data"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.op import get_op_df


def get_op_baseline(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get Outpatients Baseline Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The outpatients baseline data
    :rtype: DataFrame
    """
    return (
        get_op_df(spark)
        .groupBy("fyear", geography_column, "group", "tretspef")
        .agg(F.count("fyear").alias("count"))
        .withColumn("activity_type", F.lit("op"))
    )
