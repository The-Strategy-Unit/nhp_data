"""Helper methods/tables"""

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession

# what years should we support in the extract?
DEMOGRAPHICS_MIN_YEAR, DEMOGRAPHICS_MAX_YEAR = 2023, 2043


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
    spark: SparkSession, df: DataFrame, fyear: int, projection_year: int = 2022
) -> DataFrame:
    providers = (
        spark.read.table("strategyunit.reference.ods_trusts")
        .filter(F.col("org_type").startswith("ACUTE"))
        .select(F.col("org_to").alias("provider"))
        .distinct()
    )

    catchments = (
        spark.read.table("nhp.reference.provider_catchments")
        .filter(F.col("fyear") == fyear)
        .drop("fyear")
        .join(providers, "provider", how="semi")
    )

    projections_to_include = [
        "migration_category",
        "var_proj_5_year_migration",
        "var_proj_10_year_migration",
        "var_proj_high_intl_migration",
        "var_proj_low_intl_migration",
        "var_proj_zero_net_migration",
    ]

    return (
        df.filter(F.col("projection_year") == projection_year)
        .filter(F.col("projection").isin(projections_to_include))
        .join(catchments, ["area_code", "age", "sex"])
        .withColumnRenamed("projection", "variant")
        .withColumnRenamed("provider", "dataset")
        .groupBy("dataset", "variant", "age", "sex")
        .pivot("year")
        .agg(F.sum(F.col("value") * F.col("pcnt")))
        .orderBy("dataset", "variant", "age", "sex")
    )
