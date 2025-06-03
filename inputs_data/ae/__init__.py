"""A&E Data"""

from functools import cache, reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from inputs_data.acute_providers import filter_acute_providers
from inputs_data.helpers import age_group


def get_ae_df(spark: SparkSession) -> DataFrame:
    """Get A&E DataFrame

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The outpatients data
    :rtype: DataFrame
    """
    return (
        filter_acute_providers(spark, "ecds")
        .filter(F.col("age").isNotNull())
        .join(age_group(spark), "age")
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
        return df.select(
            F.col("fyear"),
            F.col("key"),
            F.concat(F.lit(f"{name}_"), F.col("type")).alias("strategy"),
            (F.col(col).cast("int") * F.col("arrival")).alias("n"),
            F.col("arrival").alias("d"),
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

    return reduce(DataFrame.unionByName, ae_strategies)


def get_ae_age_sex_data(spark: SparkSession) -> DataFrame:
    """Get the ae age sex table

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The inpatients age/sex data
    :rtype: DataFrame
    """
    mitigators = get_ae_mitigators(spark).filter(F.col("n") > 0)

    return (
        get_ae_df(spark)
        .join(mitigators, ["fyear", "key"], "inner")
        .groupBy("fyear", "age_group", "sex", "provider", "strategy")
        .agg(F.sum("n").alias("n"))
    )
