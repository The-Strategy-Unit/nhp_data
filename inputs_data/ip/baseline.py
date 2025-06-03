"""Inpatients Baseline Data"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from inputs_data.ip import get_ip_df


def get_ip_baseline(spark: SparkSession) -> DataFrame:
    """Get Inpatients Baseline Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients baseline data
    :rtype: DataFrame
    """
    return (
        get_ip_df(spark)
        .groupBy("fyear", "provider", "group", "tretspef")
        .agg(F.count("fyear").alias("count"))
        .withColumn("activity_type", F.lit("ip"))
    )
