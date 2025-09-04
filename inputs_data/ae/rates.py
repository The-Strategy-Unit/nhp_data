"""Get A&E Rates Data"""

# when running on databricks, we might need to change directory in order to
# import things correctly
import os

if not os.path.exists("readme.md"):
    os.chdir("..")

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from inputs_data.ae import get_ae_df, get_ae_mitigators


def get_ae_rates(spark: SparkSession) -> DataFrame:
    """Get A&E activity avoidance rates

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The A&E activity avoidances rates
    :rtype: DataFrame
    """

    df = get_ae_df(spark)
    mitigators = get_ae_mitigators(spark)

    w = Window.partitionBy("fyear", "strategy")

    return (
        df.join(
            mitigators,
            ["fyear", "key"],
            "inner",
        )
        .groupBy("fyear", "strategy", "provider")
        .agg(F.sum("n").alias("numerator"), F.sum("d").alias("denominator"))
        .withColumn("rate", F.col("numerator") / F.col("denominator"))
        .withColumn(
            "national_rate", F.sum("numerator").over(w) / F.sum("denominator").over(w)
        )
    )
