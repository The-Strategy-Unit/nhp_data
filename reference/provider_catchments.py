"""Create Provider Catchments"""

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def get_provider_catchments(spark: SparkSession) -> DataFrame:
    """get Provider Catchments

    :param spark: The Spark context
    :type spark: SparkSession
    """

    total_window = Window.partitionBy("fyear", "resladst_ons")

    return (
        spark.read.table("nhp.raw_data.apc")
        .filter(F.col("fyear") >= 201819)
        .filter(F.col("resladst_ons").rlike("^E0[6-9]"))
        .groupBy("fyear", "provider", "resladst_ons", "age", "sex")
        .count()
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .withColumnRenamed("resladst_ons", "area_code")
    )


def create_provider_catchments(spark: SparkSession) -> None:
    """Create Provider Catchments

    :param spark: The Spark context
    :type spark: SparkSession
    """

    df = get_provider_catchments(spark)
    df.write.mode("overwrite").saveAsTable("nhp.reference.provider_catchments")


def _init():
    spark: SparkSession = DatabricksSession.builder.getOrCreate()
    create_provider_catchments(spark)


if __name__ == "__main__":
    _init()
