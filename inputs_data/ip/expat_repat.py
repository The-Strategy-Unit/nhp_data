"""Inpatients Expat/Repat data"""

from functools import cache

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from inputs_data.ip import get_ip_df


def get_ip_expat_data(spark: SparkSession) -> DataFrame:
    """Get inpatients expat data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients expat data
    :rtype: DataFrame
    """
    return (
        get_ip_df(spark)
        .withColumn(
            "tretspef",
            F.when(F.col("group") == "maternity", "Other (Medical)").otherwise(
                F.col("tretspef")
            ),
        )
        .groupBy("fyear", "provider", "group", "tretspef")
        .count()
        .withColumn("activity_type", F.lit("ip"))
    )


@cache
def _get_icb_df(spark: SparkSession) -> DataFrame:
    return (
        get_ip_df(spark)
        .withColumn(
            "tretspef",
            F.when(F.col("group") == "maternity", "Other (Medical)").otherwise(
                F.col("tretspef")
            ),
        )
        .filter(F.col("icb").isNotNull())
        .groupBy("fyear", "icb", "is_main_icb", "provider", "group", "tretspef")
        .count()
        .persist()
    )


def get_ip_repat_local_data(spark: SparkSession) -> DataFrame:
    """Get inpatients repat (local) data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients repat (local) data
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
        .withColumn("activity_type", F.lit("ip"))
    )


def get_ip_repat_nonlocal_data(spark: SparkSession) -> DataFrame:
    """Get inpatients repat (non-local) data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients repat (non-local) data
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
        .withColumn("activity_type", F.lit("ip"))
    )
