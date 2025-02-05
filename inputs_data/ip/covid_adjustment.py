"""Inpatients Covid Adjustment Data"""

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.ip import get_ip_df


def get_ip_covid_adjustment(spark: SparkContext) -> DataFrame:
    """Get Inpatients Covid Adjustment Data

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The Inpatients covid adjustment data
    :rtype: DataFrame
    """
    return (
        get_ip_df(spark)
        .filter(F.col("fyear").between(201617, 201920))
        .groupBy("fyear", "provider", "group", F.month("admidate").alias("month"))
        .agg(F.count("fyear").alias("count"))
        .withColumn("activity_type", F.lit("ip"))
    )
