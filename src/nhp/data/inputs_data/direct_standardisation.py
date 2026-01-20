from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.reference.population_by_lsoa21 import get_pop_by_lad23

# cache the reference population dataframe and persist in memory
_REFERENCE_POP_DF: DataFrame | None = None


def _reference_pop(spark: SparkSession) -> DataFrame:
    """
    Get or create a cached reference population DataFrame aggregated by year, age, and sex.

    This function implements a singleton pattern using a global variable to cache the reference
    population data. On first call, it retrieves population data by LAD23 (Local Authority District 2023),
    aggregates it by fiscal year, age, and sex, and persists the result for efficient reuse.

    Args:
        spark (SparkSession): The active Spark session used to execute the query.

    Returns:
        DataFrame: A Spark DataFrame with columns:
            - fyear: Fiscal year
            - age: Age group
            - sex: Sex category
            - reference_population: Total population count for the group

    Note:
        The DataFrame is cached in memory after materialization to improve performance
        on subsequent calls. The cache is stored in the global variable _REFERENCE_POP_DF.
    """
    global _REFERENCE_POP_DF

    if _REFERENCE_POP_DF is None:
        _REFERENCE_POP_DF = (
            get_pop_by_lad23(spark)
            .groupBy("fyear", "age", "sex")
            .agg(F.sum("population").alias("reference_population"))
            .persist()
        )
        _REFERENCE_POP_DF.count()  # materialize cache

    return _REFERENCE_POP_DF


def directly_standardise(
    func: Callable[[SparkSession, str], DataFrame],
) -> Callable[[SparkSession, str], DataFrame]:
    """
    Decorator function that applies direct standardisation to health data.

    This function wraps a data loading function and applies direct standardisation
    using a reference population. It calculates both crude and standardised rates
    for different geographies (including a national aggregate).

    Args:
        func (Callable[[SparkSession, str], DataFrame]): A function that takes a SparkSession
            and returns a DataFrame containing health data with columns: fyear, strategy,
            geography_column, age, sex, n (numerator), and d (denominator).

    Returns:
        Callable[[SparkSession], DataFrame]: A wrapper function that returns a DataFrame
            with directly standardised rates containing columns:
            - fyear: Financial year
            - strategy: Strategy identifier
            - geography_column: Geography identifier (including 'national' for aggregated data)
            - crude_rate: Raw rate calculated as sum(n)/sum(d)
            - std_rate: Standardised rate adjusted using reference population weights
            - denominator: Total denominator (sum of d)

    Notes:
        - The function performs a range join with the reference population based on
          age ranges within each fyear, strategy, and sex combination
        - Missing values are filled with 0 for numerators and 1 for denominators
        - A national aggregate is computed by grouping across all geographies
        - Direct standardisation formula: Σ(rate_i × ref_pop_i) / Σ(ref_pop_i)
          where rate_i = n_i/d_i for age group i
    """

    def wrapper(spark: SparkSession, geography_column: str) -> DataFrame:
        df = func(spark, geography_column)

        ref_pop = (
            df.groupBy("fyear", "strategy", "sex")
            .agg(F.min("age").alias("min_age"), F.max("age").alias("max_age"))
            .alias("x")
            .join(
                _reference_pop(spark).alias("y").hint("range_join", 1),
                [
                    F.col("x.fyear") == F.col("y.fyear"),
                    F.col("x.sex") == F.col("y.sex"),
                    (F.col("y.age") >= F.col("x.min_age")),
                    (F.col("y.age") <= F.col("x.max_age")),
                ],
            )
            .select(
                F.col("x.fyear").alias("fyear"),
                F.col("strategy"),
                F.col("y.age").alias("age"),
                F.col("x.sex").alias("sex"),
                F.col("reference_population"),
                F.col("min_age"),
                F.col("max_age"),
            )
        )

        df_with_ref_pop = df.join(
            ref_pop, ["fyear", "strategy", "age", "sex"], "right"
        ).select(
            "fyear",
            "strategy",
            geography_column,
            "age",
            "sex",
            "n",
            "d",
            "reference_population",
        )

        national_df = (
            df_with_ref_pop.groupBy(
                "fyear", "strategy", "age", "sex", "reference_population"
            )
            .agg(
                F.sum("n").alias("n"),
                F.sum("d").alias("d"),
            )
            .withColumn(geography_column, F.lit("national"))
        )

        return (
            df_with_ref_pop.fillna(0, subset=["n"])
            .fillna(1, subset=["d"])
            .filter(F.col(geography_column).isNotNull())  # ty: ignore[missing-argument]
            .unionByName(national_df)
            .groupBy("fyear", "strategy", geography_column)
            .agg(
                (F.sum("n") / F.sum("d")).alias("crude_rate"),
                (
                    (F.sum(F.col("n") / F.col("d") * F.col("reference_population")))
                    / F.sum("reference_population")
                ).alias("std_rate"),
                F.sum("d").alias("denominator"),
            )
        )

    return wrapper
