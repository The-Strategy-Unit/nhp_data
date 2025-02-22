"""Inpatients Data"""

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame

from inputs_data.helpers import age_group, treatment_function_grouping


def get_ip_df(spark: SparkContext) -> DataFrame:
    """Get Inpatients DataFrame

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients data
    :rtype: DataFrame
    """
    return (
        spark.read.table("apc")
        .filter(F.col("age").isNotNull())
        .join(age_group(spark), "age")
        .join(treatment_function_grouping(spark), "tretspef")
        .drop("tretspef")
        .withColumnRenamed("tretspef_grouped", "tretspef")
    )


def get_ip_mitigators(spark: SparkContext) -> DataFrame:
    """Get Inpatients Mitigators DataFrame

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients mitigators data
    :rtype: DataFrame
    """
    return spark.read.table("apc_mitigators")


def get_ip_age_sex_data(spark: SparkContext) -> DataFrame:
    """Get the IP age sex table

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The inpatients age/sex data
    :rtype: DataFrame
    """
    return (
        get_ip_df(spark)
        .join(get_ip_mitigators(spark), "epikey", "inner")
        .groupBy("fyear", "age_group", "sex", "provider", "strategy")
        .agg(
            F.sum("sample_rate").alias("n"),
        )
    )
