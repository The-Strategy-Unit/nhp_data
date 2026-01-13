"""Outpatients Data"""

from functools import cache, reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.acute_providers import filter_acute_providers
from nhp.data.inputs_data.helpers import inputs_age_group
from nhp.data.table_names import table_names


def get_op_df(spark: SparkSession) -> DataFrame:
    """Get Outpatients DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients data
    :rtype: DataFrame
    """
    return (
        filter_acute_providers(spark, table_names.raw_data_opa)
        .filter(F.isnotnull("age"))
        .drop("age_group")
        .join(inputs_age_group(spark), "age")
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
    # get the op data, but immediately remove any maternity rows. they aren't intended to be used
    # for these mitigators
    df = get_op_df(spark).filter(F.col("group") != "maternity")

    op_strategies = [
        # Follow-up reduction
        df.filter(~F.col("has_procedures"))
        .withColumn("strategy", F.concat(F.lit("followup_reduction_"), F.col("type")))
        .withColumn("n", (1 - F.col("is_first").cast("int")) * F.col("attendance"))
        .withColumn("d", F.col("attendance")),
        # Consultant to Consultant reduction
        df.withColumn(
            "strategy",
            F.concat(F.lit("consultant_to_consultant_reduction_"), F.col("type")),
        )
        .withColumn("n", F.col("is_cons_cons_ref").cast("int") * F.col("attendance"))
        .withColumn("d", F.col("attendance")),
        # GP Referred First Attendance reduction
        df.withColumn(
            "strategy",
            F.concat(F.lit("gp_referred_first_attendance_reduction_"), F.col("type")),
        )
        .withColumn(
            "n",
            (F.col("is_gp_ref") & F.col("is_first")).cast("int") * F.col("attendance"),
        )
        .withColumn("d", F.col("attendance")),
        # Convert to tele
        df.filter(~F.col("has_procedures"))
        .withColumn("strategy", F.concat(F.lit("convert_to_tele_"), F.col("type")))
        .withColumn("n", F.col("attendance"))
        .withColumn("d", F.col("attendance") + F.col("tele_attendance")),
    ]

    return reduce(DataFrame.unionByName, op_strategies)


def get_op_age_sex_data(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get the op age sex table

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The outpatients age/sex data
    :rtype: DataFrame
    """
    return (
        get_op_mitigators(spark)
        .groupBy("fyear", "age", "sex", geography_column, "strategy")
        .agg(F.sum("n").alias("n"), F.sum("d").alias("d"))
    )
