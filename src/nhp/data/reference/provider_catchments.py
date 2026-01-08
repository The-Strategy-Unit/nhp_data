"""Provider catchment population calculations.

This module calculates population catchments for healthcare providers based on
Admitted Patient Care (APC) data. It determines the percentage of each Local Authority
District's (LAD) population that typically uses each provider, then applies these
percentages to population estimates to derive provider-level population figures.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.get_spark import get_spark
from nhp.data.reference.lsoa_lookups import get_lsoa11_to_lad23_lookup
from nhp.data.reference.population_by_lsoa21 import get_pop_by_lad23
from nhp.data.table_names import table_names


def create_provider_lad23_splits(spark: SparkSession) -> DataFrame:
    """Create provider to LAD23 population splits based on APC data.

    This function calculates the percentage of each LAD23's population (by age and sex)
    that uses each provider, based on historical Admitted Patient Care (APC) data.
    It counts unique patients per provider/LAD/age/sex combination and calculates
    proportions within each LAD.

    For LAD/age/sex combinations with no activity data, the function assigns them to
    the most commonly used provider for that LAD in that year.

    Args:
        spark (SparkSession): The active Spark session used to read APC data and lookups.

    Returns:
        DataFrame: A Spark DataFrame with columns:
            - fyear: Financial year
            - provider: Provider code
            - lad23cd: Local Authority District 2023 code
            - age: Age group
            - sex: Sex
            - lad23_pcnt: Proportion of LAD's population (for this age/sex) using this provider

    Notes:
        - Only includes English LSOAs (codes starting with 'E')
        - Excludes pseudo LSOA code 'E99999999'
        - Uses unique patient counts (person_id) to align with OHID methodology
        - Missing age/sex/LAD combinations are assigned to the most popular provider for that LAD
    """
    lsoa11_to_lad23 = get_lsoa11_to_lad23_lookup(spark)

    df = (
        spark.read.table(table_names.default_apc)
        .filter(F.col("lsoa11").startswith("E"))  # ty:ignore[missing-argument, invalid-argument-type]
        .filter(F.col("lsoa11") != "E99999999")
        .join(lsoa11_to_lad23, F.col("lsoa11") == F.col("lsoa11cd"))
        # get unique patients; more closely related to OHIDs logic
        # (https://tinyurl.com/ohid-nhs-acute-catchments)
        .select("fyear", "provider", "lad23cd", "age", "sex", "person_id")
        .distinct()
        .groupBy("fyear", "provider", "lad23cd", "age", "sex")
        .agg(F.count("person_id").alias("count"))
    )

    default_providers_for_lad = (
        df.groupBy("fyear", "lad23cd", "provider")
        .agg(F.sum("count").alias("count"))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("fyear", "lad23cd").orderBy(F.desc("count"))
            ),
        )
        .filter(F.col("rn") == 1)
        .drop("count", "rn")
    )

    missing_counts = (
        df.select("fyear", "age", "sex")
        .distinct()
        .join(df.select("fyear", "lad23cd").distinct(), "fyear")
        .join(df, ["fyear", "age", "sex", "lad23cd"], "anti")
        .join(default_providers_for_lad, ["fyear", "lad23cd"])
        .withColumn("count", F.lit(1))
    )

    return (
        df.unionByName(missing_counts)
        .withColumn(
            "lad23_pcnt",
            F.col("count")
            / F.sum("count").over(Window.partitionBy("fyear", "lad23cd", "age", "sex")),
        )
        .drop("count")
    )


def get_provider_lad23_splits(spark: SparkSession) -> DataFrame:
    """Retrieve or create the provider to LAD23 population splits table.

    This function checks if the provider LAD23 splits table exists in the Spark catalog.
    If the table does not exist, it creates it using create_provider_lad23_splits()
    and saves it as a table. If the table already exists, it simply reads and returns it.

    Args:
        spark (SparkSession): The active Spark session used to interact with the catalog
                             and read/write tables.

    Returns:
        DataFrame: A Spark DataFrame containing provider to LAD23 population splits.

    Note:
        The table name is defined in `table_names.reference_provider_lad23_splits`.
    """
    table = table_names.reference_provider_lad23_splits
    if not spark.catalog.tableExists(table):
        df = create_provider_lad23_splits(spark)
        df.write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def create_pop_by_provider(spark: SparkSession) -> DataFrame:
    """Calculate population estimates for each provider based on catchment splits.

    This function combines provider-to-LAD23 splits with LAD23 population estimates
    to calculate the catchment population for each provider. It applies the percentage
    splits (from APC activity patterns) to actual population figures to estimate how
    many people of each age/sex in each year are served by each provider.

    Args:
        spark (SparkSession): The active Spark session used to read required tables.

    Returns:
        DataFrame: A Spark DataFrame with columns:
            - fyear: Financial year
            - provider: Provider code
            - age: Age group
            - sex: Sex
            - population: Estimated catchment population for this provider/age/sex combination

    Notes:
        - Populations are calculated by multiplying LAD23 populations by provider split percentages
        - Results are aggregated to provider level (summing across all LADs)
        - Output is ordered by fyear, provider, age, and sex for consistency
    """
    provider_to_lad23_splits = get_provider_lad23_splits(spark)
    pop_by_lad23 = get_pop_by_lad23(spark)

    return (
        provider_to_lad23_splits.join(pop_by_lad23, ["fyear", "lad23cd", "age", "sex"])
        .groupBy("fyear", "provider", "age", "sex")
        .agg(F.sum(F.col("population") * F.col("lad23_pcnt")).alias("population"))
        .orderBy("fyear", "provider", "age", "sex")
    )


def get_pop_by_provider(spark: SparkSession) -> DataFrame:
    """Retrieve or create the population by provider table.

    This function checks if the population by provider table exists in the Spark catalog.
    If the table does not exist, it creates it using create_pop_by_provider() and saves
    it as a table. If the table already exists, it simply reads and returns it.

    Args:
        spark (SparkSession): The active Spark session used to interact with the catalog
                             and read/write tables.

    Returns:
        DataFrame: A Spark DataFrame containing catchment population estimates by provider,
                  age, sex, and financial year.

    Note:
        The table name is defined in `table_names.reference_pop_by_provider`.
    """
    table = table_names.reference_pop_by_provider
    if not spark.catalog.tableExists(table):
        df = create_pop_by_provider(spark)
        df.write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


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

    get_pop_by_provider(spark)
