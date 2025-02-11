"""Get Outpatients Rates Data"""

from pyspark import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from inputs_data.op import get_op_df, get_op_mitigators


def get_op_rates(spark: SparkContext) -> DataFrame:
    """Get outpatients activity avoidance rates

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The outpatients activity avoidances rates
    :rtype: DataFrame
    """

    df = get_op_df(spark)
    mitigators = get_op_mitigators(spark)

    w = Window.partitionBy("fyear", "strategy")

    return (
        df.join(
            mitigators,
            "attendkey",
            "inner",
        )
        .groupBy("fyear", "strategy", "provider")
        .agg(F.sum("n").alias("numerator"), F.sum("d").alias("denominator"))
        .withColumn(
            "rate",
            F.col("numerator")
            / F.when(
                F.col("strategy").startswith("followup_reduction_"),
                F.col("denominator") - F.col("numerator"),
            ).otherwise(F.col("denominator")),
        )
        .withColumn(
            "national_rate", F.sum("numerator").over(w) / F.sum("denominator").over(w)
        )
    )
