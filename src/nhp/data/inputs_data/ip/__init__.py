"""Inpatients Data"""

import logging

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.helpers import inputs_age_group
from nhp.data.table_names import table_names


def get_ip_df(spark: SparkSession) -> DataFrame:
    """Get Inpatients DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients data
    :rtype: DataFrame
    """
    return (
        spark.read.table(table_names.raw_data_apc)
        .filter(F.col("fyear") >= 201516)
        .filter(F.isnotnull("age"))
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
        .filter(F.col("classpat") == "1")
        .select(
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

    mitigators_df = spark.read.table(table_names.raw_data_apc_mitigators)

    return DataFrame.unionByName(mitigators_df, general_los_df)


_IP_AGE_SEX_DF_CACHE = {}


def get_ip_age_sex_data(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get the IP age sex table

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The inpatients age/sex data
    :rtype: DataFrame
    """

    if geography_column in _IP_AGE_SEX_DF_CACHE:
        logging.info("Using cached IP age sex data for %s", geography_column)
        return _IP_AGE_SEX_DF_CACHE[geography_column]

    logging.info("Creating IP age sex data for %s", geography_column)
    _IP_AGE_SEX_DF_CACHE[geography_column] = df = (
        get_ip_df(spark)
        .fillna("unknown", [geography_column])
        .join(
            get_ip_mitigators(spark),
            [
                "fyear",
                "provider",  # join on "provider" column, not geography_column
                "epikey",
            ],
            "inner",
        )
        .groupBy("fyear", "age", "sex", geography_column, "type", "strategy")
        .agg(F.sum("sample_rate").alias("n"), F.sum("speldur").alias("speldur"))
        .persist()
    )

    df.count()  # materialise the cache
    logging.info("Materialised IP age sex data for %s", geography_column)

    return df
