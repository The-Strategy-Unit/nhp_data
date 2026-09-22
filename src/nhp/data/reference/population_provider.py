"""Helper methods/tables"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.reference.lsoa_lookups import get_lad22_to_lad23_lookup
from nhp.data.table_names import table_names

# what years should we support in the extract?
DEMOGRAPHICS_MIN_YEAR, DEMOGRAPHICS_MAX_YEAR = 2023, 2047
PROJECTIONS_TO_INCLUDE = [
    "migration_category",
    "var_proj_5_year_migration",
    "var_proj_10_year_migration",
    "var_proj_high_intl_migration",
    "var_proj_low_intl_migration",
    "var_proj_zero_net_migration",
]
PROJECTION_YEAR = 2022


def create_provider_demographics(
    spark: SparkSession, table_from: str, table_to: str
) -> None:
    providers = (
        spark.read.table(table_names.reference_ods_trusts)
        .filter(F.col("org_type").startswith("ACUTE"))
        .select(F.col("org_to").alias("provider"))
        .distinct()
    )

    min_fyear = DEMOGRAPHICS_MIN_YEAR * 100 + (DEMOGRAPHICS_MIN_YEAR + 1) % 100
    catchments = (
        spark.read.table(table_names.reference_provider_lad23_splits)
        .filter(F.col("fyear") >= min_fyear)
        .join(providers, "provider", how="semi")
    )

    lad22_to_lad23 = get_lad22_to_lad23_lookup(spark)

    df = (
        spark.read.table(table_from)
        .filter(F.col("year").between(DEMOGRAPHICS_MIN_YEAR, DEMOGRAPHICS_MAX_YEAR))
        .filter(F.col("projection_year") == PROJECTION_YEAR)
        .filter(F.col("projection").isin(PROJECTIONS_TO_INCLUDE))
        .withColumnRenamed("projection", "variant")
        .withColumnRenamed("area_code", "lad22cd")
        .join(lad22_to_lad23, "lad22cd")
        .join(catchments, ["lad23cd", "age", "sex"])
        .groupBy("fyear", "dataset", "variant", "sex", "age")
        .pivot("year")
        .agg(F.sum(F.col("value") * F.col("lad23_pcnt")))
        .orderBy("fyear", "dataset", "variant", "sex", "age")
    )
    df.repartition(1).write.mode("overwrite").partitionBy(
        "fyear", "dataset", "variant"
    ).saveAsTable(table_to)


def create_demographic_factors(spark: SparkSession) -> None:
    """Extract Demographic Factors data

    :param spark: the spark session to use
    :type spark: SparkSession
    """
    create_provider_demographics(
        spark,
        table_names.population_projections_demographics,
        table_names.reference_population_provider_demographics,
    )


def create_birth_factors(spark: SparkSession) -> None:
    """Extract Birth Factors data

    :param spark: the spark session to use
    :type spark: SparkSession
    """
    create_provider_demographics(
        spark,
        table_names.population_projections_births,
        table_names.reference_population_provider_births,
    )


def main():
    """Main entry point for generating provider catchment population tables.

    This function creates the population by provider table, which includes calculating
    provider-to-LAD23 splits based on APC data and applying those splits to population
    estimates. The get_pop_by_provider() call will trigger creation of all dependent
    tables if they don't already exist.

    Tables generated:
        - Provider to LAD23 splits (if not exists)
        - Population by provider (if not exists)
    """
    spark = get_spark()

    create_demographic_factors(spark)
    create_birth_factors(spark)
