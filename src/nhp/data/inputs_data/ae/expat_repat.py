"""A&E Expat/Repat data"""

from functools import cache

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.inputs_data.ae import get_ae_df


def get_ae_expat_data(spark: SparkSession) -> DataFrame:
    """Get A&E expat data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The A&E expat data
    :rtype: DataFrame
    """
    return (
        get_ae_df(spark)
        .groupBy("fyear", "provider", "group", "tretspef")
        .agg(F.sum("arrival").alias("count"))
        .withColumn("activity_type", F.lit("aae"))
    )


@cache
def _get_icb_df(spark: SparkSession) -> DataFrame:
    return (
        get_ae_df(spark)
        .filter(F.isnotnull("icb"))
        .groupBy("fyear", "icb", "is_main_icb", "provider", "group", "tretspef")
        .agg(F.sum("arrival").alias("count"))
        .persist()
    )


def get_ae_repat_local_data(spark: SparkSession) -> DataFrame:
    """Get A&E repat (local) data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The A&E  repat (local) data
    :rtype: DataFrame
    """
    return (
        _get_icb_df(spark)
        .withColumn(
            "provider",
            F.when(F.col("is_main_icb"), F.col("provider")).otherwise("Other"),
        )
        .groupBy("fyear", "icb", "provider", "group", "tretspef")
        .agg(F.sum("count").alias("count"))
        .withColumn(
            "pcnt",
            F.col("count")
            / F.sum("count").over(
                Window.partitionBy("icb", "fyear", "group", "tretspef")
            ),
        )
        .withColumn("activity_type", F.lit("aae"))
    )


def get_ae_repat_nonlocal_data(spark: SparkSession) -> DataFrame:
    """Get A&E repat (non-local) data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The A&E repat (non-local) data
    :rtype: DataFrame
    """
    return (
        _get_icb_df(spark)
        .withColumn(
            "pcnt",
            F.col("count")
            / F.sum("count").over(
                Window.partitionBy("provider", "fyear", "group", "tretspef")
            ),
        )
        .orderBy(F.desc("pcnt"))
        .withColumn("activity_type", F.lit("aae"))
    )
