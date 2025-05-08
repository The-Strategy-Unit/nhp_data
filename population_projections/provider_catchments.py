"""Create Provider Catchments"""

from databricks.connect import DatabricksSession
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F


def get_provider_catchments(spark: SparkContext, min_pcnt: float = 0.05) -> None:
    """get Provider Catchments

    :param spark: The Spark context
    :type spark: SparkContext
    :param min_pcnt: the minimum percentage of patients in a catchment area (default: 0.05)
    :type min_pcnt: float
    """

    total_window = Window.partitionBy("fyear", "provider")

    return (
        spark.read.table("nhp.raw_data.apc")
        .filter(F.col("fyear") >= 201819)
        .filter(F.col("resladst_ons").rlike("^E0[6-9]"))
        .groupBy("fyear", "provider", "resladst_ons")
        .count()
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .filter(F.col("pcnt") > min_pcnt)
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .withColumnRenamed("resladst_ons", "area_code")
    )


def create_provider_catchments(spark: SparkContext, min_pcnt: float = 0.05) -> None:
    """Create Provider Catchments

    :param spark: The Spark context
    :type spark: SparkContext
    :param min_pcnt: the minimum percentage of patients in a catchment area (default: 0.05)
    :type min_pcnt: float
    """

    df = get_provider_catchments(spark, min_pcnt)
    df.write.mode("overwrite").saveAsTable(
        "nhp.population_projections.provider_catchments"
    )


def _init():
    spark: SparkContext = DatabricksSession.builder.getOrCreate()
    create_provider_catchments(spark)


if __name__ == "__main__":
    _init()
