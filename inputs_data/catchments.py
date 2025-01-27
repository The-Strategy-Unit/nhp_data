"""Catchments"""

from pyspark import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from inputs_data.ip import get_ip_df


def get_pop(spark: SparkContext) -> DataFrame:
    """Get the population by year table

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The population by year/lsoa11/age/sex
    :rtype: DataFrame
    """
    pop = spark.read.parquet(
        "/Volumes/su_data/nhp/reference_data/population_by_year.parquet"
    )

    a = pop.select("fyear", "lsoa11").distinct()
    b = pop.select("age_group", "sex").distinct()

    return (
        a.crossJoin(b)
        .join(pop, ["fyear", "lsoa11", "age_group", "sex"], "left")
        .fillna(0, "pop")
        .withColumn("pop", F.round("pop"))
        .persist()
    )


def create_catchments(spark: SparkContext) -> None:
    """Create the catchments table

    :param spark: The spark context to use
    :type spark: SparkContext
    """
    w = Window.partitionBy("fyear", "lsoa11", "age_group", "sex")

    pop = get_pop(spark)

    catchments = (
        get_ip_df(spark)
        .filter(F.col("lsoa11").startswith("E"))
        .groupBy("fyear", "lsoa11", "age_group", "sex", "provider")
        .agg(F.count("provider").alias("n"))
        .withColumn("total", F.sum("n").over(w))
        .withColumn("p", F.col("n") / F.col("total"))
        .join(pop, ["fyear", "sex", "age_group", "lsoa11"], "inner")
        .withColumn("pop_catch", F.col("pop") * F.col("p"))
        .groupBy("fyear", "sex", "age_group", "provider")
        .agg(F.sum("pop_catch").alias("pop_catch"))
    )

    catchments.write.mode("overwrite").saveAsTable("inputs_catchments")


def get_catchments(spark: SparkContext) -> DataFrame:
    """Get the catchments table

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: The catchments data
    :rtype: DataFrame
    """
    return spark.read.table("inputs_catchments").persist()


def get_total_pop(spark: SparkContext) -> DataFrame:
    """_summary_

    :param spark: _description_
    :type spark: SparkContext
    :return: _description_
    :rtype: DataFrame
    """
    return (
        get_pop(spark)
        .groupBy("fyear", "age_group", "sex")
        .agg(F.sum("pop").alias("total_pop"))
        .persist()
    )
