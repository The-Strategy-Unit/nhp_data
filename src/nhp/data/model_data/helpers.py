"""Helper methods/tables"""

from collections.abc import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.reference.lsoa_lookups import get_lad22_to_lad23_lookup
from nhp.data.table_names import table_names

# what years should we support in the extract?
DEMOGRAPHICS_MIN_YEAR, DEMOGRAPHICS_MAX_YEAR = 2023, 2047


def create_provider_population_projections(
    spark: SparkSession, df: DataFrame, fyear: int, projection_year: int = 2022
) -> DataFrame:
    providers = (
        spark.read.table(table_names.reference_ods_trusts)
        .filter(F.col("org_type").startswith("ACUTE"))
        .select(F.col("org_to").alias("provider"))
        .distinct()
    )

    catchments = (
        spark.read.table(table_names.reference_provider_lad23_splits)
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

    lad22_to_lad23 = get_lad22_to_lad23_lookup(spark)

    return (
        df.filter(F.col("projection_year") == projection_year)
        .filter(F.col("projection").isin(projections_to_include))
        .withColumnRenamed("area_code", "lad22cd")
        .join(lad22_to_lad23, "lad22cd")
        .join(catchments, ["lad23cd", "age", "sex"])
        .withColumnRenamed("projection", "variant")
        .withColumnRenamed("provider", "dataset")
        .groupBy("dataset", "variant", "age", "sex")
        .pivot("year")
        .agg(F.sum(F.col("value") * F.col("lad23_pcnt")))
        .orderBy("dataset", "variant", "age", "sex")
    )


def create_icb_population_projections(
    spark: SparkSession, df: DataFrame, projection_year: int = 2022
) -> DataFrame:
    catchments = spark.read.table(table_names.reference_icb_catchments)

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
        .join(catchments, ["area_code"])
        .withColumnRenamed("projection", "variant")
        .withColumnRenamed("provider", "dataset")
        .groupBy("icb", "variant", "age", "sex")
        .pivot("year")
        .agg(F.sum(F.col("value") * F.col("pcnt")))
        .orderBy("icb", "variant", "age", "sex")
    )


def check_extract_for_nulls(
    df: DataFrame, exclude_cols: set[str] | None = None
) -> None:
    """Check a dataframe for any null columns and raise an error if any are found.

    Args:
        df (DataFrame): the dataframe to check
        exclude_cols (set[str]): columns to exclude from the null check
    """
    if exclude_cols is None:
        exclude_cols = set()

    cols = list(set(df.columns) - exclude_cols)

    melt_str = ", ".join([f"'{c}', `{c}`" for c in cols])

    stack_expr = F.expr(f"stack({len(cols)}, {melt_str}) as (column, null_count)")

    null_check = (
        df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols])
        .select(stack_expr)
        .filter(F.col("null_count") > 0)
        .collect()
    )

    assert len(null_check) == 0, (
        "Nulls found in the following columns ["
        + ", ".join([i["column"] for i in null_check])
        + "]"
    )


def extract(
    extract_name: str,
    check_for_nulls: bool = True,
    exclude_cols: set[str] | None = None,
) -> Callable[
    [Callable[..., DataFrame]],
    Callable[..., None],
]:
    def decorator(func: Callable[..., DataFrame]):
        def wrapper(
            save_path: str, fyear: int, spark: SparkSession, *args, **kwargs
        ) -> None:
            """Extract a dataframe to parquet, optionally checking for nulls.

            Args:
                save_path (str): the path to save the parquet files
                fyear (int): the fiscal year
                spark (SparkSession): the Spark session
            """
            df = func(save_path, fyear, spark, *args, **kwargs)
            if check_for_nulls:
                check_extract_for_nulls(df, exclude_cols)

            print(f"Rows to extract: {df.count():,}")

            (
                df.repartition(1)
                .write.mode("overwrite")
                .partitionBy(["fyear", "dataset"])
                .parquet(f"{save_path}/{extract_name}")
            )

        return wrapper

    return decorator
