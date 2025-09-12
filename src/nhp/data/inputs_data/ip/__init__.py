"""Inpatients Data"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.acute_providers import filter_acute_providers
from nhp.data.inputs_data.helpers import inputs_age_group


def get_ip_df(spark: SparkSession) -> DataFrame:
    """Get Inpatients DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients data
    :rtype: DataFrame
    """
    return (
        filter_acute_providers(spark, "apc")
        .filter(F.col("age").isNotNull())
        .drop("age_group")
        .join(inputs_age_group(spark), "age")
        .drop("tretspef")
        .withColumnRenamed("tretspef_grouped", "tretspef")
    )


def get_ip_mitigators(spark: SparkSession) -> DataFrame:
    """Get Inpatients Mitigators DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients mitigators data
    :rtype: DataFrame
    """

    # general los needs to be union'd into the mitigators as we do not create the row in the
    # mitigators table - they are generated at model runtime. but for inputs, we need to create
    # them here
    general_los_df = (
        get_ip_df(spark)
        # only include electives and emergencies (not all non-elective)
        .filter(F.col("admimeth").rlike("^[12]"))
        # only include ordinary admissions, ignore daycases etc.
        .filter(F.col("classpat") == "1").select(
            F.col("fyear"),
            F.col("provider"),
            F.col("epikey"),
            F.lit("efficiency").alias("type"),
            F.concat(
                F.lit("general_los_reduction_"),
                F.when(F.col("group") == "elective", "elective").otherwise("emergency"),
            ).alias("strategy"),
            F.lit(1.0).alias("sample_rate"),
        )
    )

    mitigators_df = spark.read.table("nhp.raw_data.apc_mitigators")

    return DataFrame.unionByName(mitigators_df, general_los_df)


def get_ip_age_sex_data(spark: SparkSession) -> DataFrame:
    """Get the IP age sex table

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients age/sex data
    :rtype: DataFrame
    """
    return (
        get_ip_df(spark)
        .join(get_ip_mitigators(spark), ["fyear", "provider", "epikey"], "inner")
        .groupBy("fyear", "age_group", "sex", "provider", "strategy")
        .agg(
            F.sum("sample_rate").alias("n"),
        )
    )
