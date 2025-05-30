"""Covid Adjustment Data"""

import sys
from functools import reduce

from pyspark import SparkSession
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from inputs_data.ae.covid_adjustment import get_ae_covid_adjustment
from inputs_data.helpers import get_spark
from inputs_data.ip.covid_adjustment import get_ip_covid_adjustment
from inputs_data.op.covid_adjustment import get_op_covid_adjustment


def get_covid_adjustment(spark: SparkSession = get_spark()) -> DataFrame:
    """Get Covid Adjustment Data

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: The covid adjustment data
    :rtype: DataFrame
    """
    fns = [get_ae_covid_adjustment, get_ip_covid_adjustment, get_op_covid_adjustment]

    df = reduce(DataFrame.unionByName, [f(spark) for f in fns])

    w = Window.partitionBy("fyear", "provider", "group")

    df = (
        df.withColumn("year_total", F.sum("count").over(w))
        .filter(F.col("month").isin([2, 3]))
        .withColumn(
            "wdays",
            F.when(F.col("month") == 3, 31)
            .when(F.col("fyear") == 201617, 29)
            .otherwise(28),
        )
        .withColumn("x", F.col("count") / F.col("wdays"))
        .groupBy("fyear", "provider", "activity_type", "group", "year_total")
        .pivot("month")
        .agg(F.first("x"))
        .withColumn("diff", F.col("3") / F.col("2"))
    )

    df_avg = (
        df.filter(F.col("fyear") < 201920)
        .groupBy("provider", "activity_type", "group")
        .agg(F.mean("diff").alias("a"))
    )

    return (
        df.filter(F.col("fyear") == 201920)
        .join(df_avg, ["provider", "activity_type", "group"])
        .withColumn(
            "covid_adjustment",
            1 + ((F.col("2") * F.col("a") - F.col("3")) * 31) / F.col("year_total"),
        )
        .select(
            "fyear",
            "provider",
            "activity_type",
            "group",
            "covid_adjustment",
        )
    )


if __name__ == "__main__":
    path = sys.argv[1]

    get_covid_adjustment().toPandas().to_parquet(f"{path}/covid_adjustment.parquet")
