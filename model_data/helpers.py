"""Helper methods/tables"""

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession


def add_tretspef_column(self: DataFrame) -> DataFrame:
    """Add tretspef column to DataFrame

    :param self: The data frame to add the tretpsef column to
    :type df: DataFrame
    :return: The data frame
    :rtype: DataFrame
    """

    specialties = [
        "100",
        "101",
        "110",
        "120",
        "130",
        "140",
        "150",
        "160",
        "170",
        "300",
        "301",
        "320",
        "330",
        "340",
        "400",
        "410",
        "430",
        "520",
    ]

    tretspef_column = (
        F.when(F.col("tretspef_raw").isin(specialties), F.col("tretspef_raw"))
        .when(F.expr("tretspef_raw RLIKE '^1(?!80|9[02])'"), F.lit("Other (Surgical)"))
        .when(
            F.expr("tretspef_raw RLIKE '^(1(80|9[02])|[2346]|5(?!60)|83[134])'"),
            F.lit("Other (Medical)"),
        )
        .otherwise(F.lit("Other"))
    )

    return self.withColumn("tretspef", tretspef_column)


def get_spark() -> SparkSession:
    """Get Spark session to use for model data extract

    :return: get the spark context to use
    :rtype: SparkSession
    """
    spark: SparkSession = DatabricksSession.builder.getOrCreate()
    spark.catalog.setCurrentCatalog("nhp")
    spark.catalog.setCurrentDatabase("default")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def create_population_projections(
    spark: SparkSession, df: DataFrame, fyear: int
) -> DataFrame:
    providers = (
        spark.read.table("strategyunit.reference.ods_trusts")
        .filter(F.col("org_type").startswith("ACUTE"))
        .select(F.col("org_to").alias("provider"))
        .distinct()
    )

    catchments = (
        spark.read.table("nhp.population_projections.provider_catchments")
        .filter(F.col("fyear") == fyear)
        .drop("fyear")
        .join(providers, "provider", how="semi")
    )

    # currently fixed to use the 2018 projection year: awaiting new data from ONS to be published
    return (
        df.filter(F.col("projection_year") == 2018)
        .join(catchments, "area_code")
        .withColumnRenamed("projection", "variant")
        .withColumnRenamed("provider", "dataset")
        .groupBy("dataset", "variant", "age", "sex")
        .pivot("year")
        .agg(F.sum(F.col("value") * F.col("pcnt")))
        .orderBy("dataset", "variant", "age", "sex")
    )
