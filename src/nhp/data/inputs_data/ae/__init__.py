"""A&E Data"""

import logging
from functools import cache, reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.helpers import inputs_age_group
from nhp.data.table_names import table_names


def get_ae_df(spark: SparkSession) -> DataFrame:
    """Get A&E DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients data
    :rtype: DataFrame
    """
    return (
        spark.read.table(table_names.raw_data_ecds)
        .filter(F.col("fyear") >= 201516)
        .filter(F.isnotnull("age"))
        .drop("age_group")
        .join(inputs_age_group(spark), "age")
    )


@cache
def get_ae_mitigators(spark: SparkSession) -> DataFrame:
    """Get A&E Mitigators DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients mitigators data
    :rtype: DataFrame
    """

    df = get_ae_df(spark)

    def _create_mitigator(col: str, name: str) -> DataFrame:
        return (
            df.withColumn("strategy", F.concat(F.lit(f"{name}_"), F.col("type")))
            .withColumn("type", F.lit("activity_avoidance"))
            .withColumn("n", F.col(col).cast("int") * F.col("arrival"))
            .withColumn("d", F.col("arrival"))
        )

    ae_strategies = [
        _create_mitigator(*i)
        for i in [
            ("is_frequent_attender", "frequent_attenders"),
            ("is_left_before_treatment", "left_before_seen"),
            ("is_low_cost_referred_or_discharged", "low_cost_discharged"),
            ("is_discharged_no_treatment", "discharged_no_treatment"),
        ]
    ]

    return reduce(DataFrame.unionByName, ae_strategies).persist()


_AE_AGE_SEX_DF_CACHE = {}


def get_ae_age_sex_data(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get the ae age sex table

    :param spark: The spark context to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The inpatients age/sex data
    :rtype: DataFrame
    """
    if geography_column in _AE_AGE_SEX_DF_CACHE:
        logging.info("Using cached AE age sex data for %s", geography_column)
        return _AE_AGE_SEX_DF_CACHE[geography_column]

    logging.info("Creating AE age sex data for %s", geography_column)

    _AE_AGE_SEX_DF_CACHE[geography_column] = df = (
        get_ae_mitigators(spark)
        .fillna("unknown", [geography_column])
        .groupBy("fyear", "age", "sex", geography_column, "type", "strategy")
        .agg(F.sum("n").alias("n"), F.sum("d").alias("d"))
    )

    df.count()  # materialise the cache
    logging.info("Materialised AE age sex data for %s", geography_column)

    return df
