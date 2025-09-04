"""Outpatients Expat/Repat data"""

from functools import cache

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.inputs_data.op import get_op_df


def get_op_expat_data(spark: SparkSession) -> DataFrame:
    """Get outpatients expat data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients expat data
    :rtype: DataFrame
    """
    return (
        get_op_df(spark)
        .withColumn("group", F.lit(""))
        .groupBy("fyear", "provider", "group", "tretspef")
        .agg(F.sum("attendance").alias("count"))
        .withColumn("activity_type", F.lit("op"))
    )


@cache
def _get_icb_df(spark: SparkSession) -> DataFrame:
    return (
        get_op_df(spark)
        .filter(F.col("icb").isNotNull())
        .withColumn("group", F.lit(""))
        .groupBy("fyear", "icb", "is_main_icb", "provider", "group", "tretspef")
        .agg(F.sum("attendance").alias("count"))
        .persist()
    )


def get_op_repat_local_data(spark: SparkSession) -> DataFrame:
    """Get outpatients repat (local) data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients repat (local) data
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
        .withColumn("activity_type", F.lit("op"))
    )


def get_op_repat_nonlocal_data(spark: SparkSession) -> DataFrame:
    """Get outpatients repat (non-local) data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients repat (non-local) data
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
        .withColumn("activity_type", F.lit("op"))
    )
