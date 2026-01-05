"""A&E Data"""

from functools import cache, reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.inputs_data.acute_providers import filter_acute_providers
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
        filter_acute_providers(spark, table_names.raw_data_ecds)
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
            df.withColumn("strategy", F.lit(name))
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


def get_ae_age_sex_data(spark: SparkSession) -> DataFrame:
    """Get the ae age sex table

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients age/sex data
    :rtype: DataFrame
    """
    return (
        get_ae_mitigators(spark)
        .groupBy("fyear", "age", "sex", "provider", "type", "strategy")
        .agg(F.sum("n").alias("n"), F.sum("d").alias("d"))
    )
