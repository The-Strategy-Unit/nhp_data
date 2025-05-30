"""Outpatients Data"""

from functools import cache, reduce

import pyspark.sql.functions as F
from pyspark import SparkSession
from pyspark.sql import DataFrame

from inputs_data.acute_providers import filter_acute_providers
from inputs_data.helpers import age_group, treatment_function_grouping


def get_op_df(spark: SparkSession) -> DataFrame:
    """Get Outpatients DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients data
    :rtype: DataFrame
    """
    return (
        filter_acute_providers(spark, "opa")
        .filter(F.col("age").isNotNull())
        .join(age_group(spark), "age")
        .join(treatment_function_grouping(spark), "tretspef")
        .drop("tretspef")
        .withColumnRenamed("tretspef_grouped", "tretspef")
    )


@cache
def get_op_mitigators(spark: SparkSession) -> DataFrame:
    """Get Outpatients Mitigators DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients mitigators data
    :rtype: DataFrame
    """
    df = get_op_df(spark)

    op_strategies = [
        # Follow-up reduction
        df.filter(~F.col("has_procedures"))
        .withColumn("strategy", F.concat(F.lit("followup_reduction_"), F.col("type")))
        .withColumn("n", (1 - F.col("is_first").cast("int")) * F.col("attendance"))
        .withColumn("d", F.col("attendance"))
        .select("attendkey", "strategy", "n", "d"),
        # Consultant to Consultant reduction
        df.withColumn(
            "strategy",
            F.concat(F.lit("consultant_to_consultant_reduction_"), F.col("type")),
        )
        .withColumn("n", F.col("is_cons_cons_ref").cast("int") * F.col("attendance"))
        .withColumn("d", F.col("attendance"))
        .select("attendkey", "strategy", "n", "d"),
        # GP Referred First Attendance reduction
        df.withColumn(
            "strategy",
            F.concat(F.lit("gp_referred_first_attendance_reduction_"), F.col("type")),
        )
        .withColumn(
            "n",
            (F.col("is_gp_ref") & F.col("is_first")).cast("int") * F.col("attendance"),
        )
        .withColumn("d", F.col("attendance"))
        .select("attendkey", "strategy", "n", "d"),
        # Convert to tele
        df.filter(~F.col("has_procedures"))
        .withColumn("strategy", F.concat(F.lit("convert_to_tele_"), F.col("type")))
        .withColumn("n", F.col("attendance"))
        .withColumn("d", F.col("attendance") + F.col("tele_attendance"))
        .select("attendkey", "strategy", "n", "d"),
    ]

    return reduce(DataFrame.unionByName, op_strategies)


def get_op_age_sex_data(spark: SparkSession) -> DataFrame:
    """Get the op age sex table

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients age/sex data
    :rtype: DataFrame
    """
    mitigators = get_op_mitigators(spark).filter(F.col("n") > 0)

    return (
        get_op_df(spark)
        .join(mitigators, "attendkey", "inner")
        .groupBy("fyear", "age_group", "sex", "provider", "strategy")
        .agg(F.sum("n").alias("n"))
    )
