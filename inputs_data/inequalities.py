"""Generate Inequalities Dataframe"""

import sys
import mlflow
import pandas as pd
import statsmodels.api as sm
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

from inputs_data.helpers import get_spark


def load_inequalities_data(spark: SparkSession, fyears: list) -> DataFrame:
    """
    Load and preprocess inequalities data.

    :param spark: The spark context to use
    :type spark: SparkSession
    :param fyears: The years for the inequalities analysis, with each fyear as an int
    :type fyears: List

    :return: The dataframe containing the data required for inequalities analysis
    :rtype: DataFrame
    """

    imd_df = (
        spark.read.table("nhp.reference.population_by_imd_decile")
        .withColumn("imd_quintile", F.floor((F.col("imd19") - 1) / 2) + 1)
        .drop("imd19")
        .groupby("icb", "provider", "imd_quintile")
        .agg(F.sum("pop").alias("pop"))
        .withColumn(
            "population_share",
            F.col("pop") / F.sum("pop").over(Window.partitionBy(["icb", "provider"])),
        )
        .withColumn(
            "total_catchment_pop",
            F.sum("pop").over(Window.partitionBy(["icb", "provider"])),
        )
    )

    apc = (
        spark.read.table("nhp.default.apc")
        .filter(F.col("fyear").isin(fyears))
        .withColumn("sushrg_trimmed", F.expr("substring(sushrg, 1, 4)"))
        .filter(F.col("admimeth").startswith("1"))
        .select("icb", "provider", "imd_quintile", "sushrg_trimmed", "fyear")
        .groupby("icb", "provider", "imd_quintile", "sushrg_trimmed", "fyear")
        .agg(F.count("*").alias("count"))
    )

    opa = (
        spark.read.table("nhp.default.opa")
        .filter(F.col("fyear").isin(fyears))
        .filter(F.col("has_procedures"))
        .groupby("icb", "provider", "imd_quintile", "sushrg_trimmed", "fyear")
        .agg(F.sum("attendances").alias("count"))
    )

    data = opa.unionByName(apc)

    data_hrg_count = (
        data.groupby(["icb", "provider", "imd_quintile", "sushrg_trimmed", "fyear"])
        .agg(F.sum("count").alias("count"))
        .join(imd_df, on=["icb", "provider", "imd_quintile"])
        .withColumn("activity_rate", F.col("count") / (F.col("pop")))
    )

    return data_hrg_count.cache()


def calculate_inequalities(data_hrg_count: DataFrame, min_count: int = 50) -> DataFrame:
    """
    Run weighted regressions in parallel to calculate inequalities

    :param data_hrg_count: The dataframe containing the counts of activity per IMD quintile and HRG for each icb, provider and year
    :type data_hrg_count: DataFrame
    :param min_count: The minimum count of activity in an IMD quintile for a HRG to be used in analysis
    :type min_count: int

    :return: Dataframe containing the calculated pvalues, slopes, and intercepts
    :rtype: DataFrame
    """

    # Pre-filter HRGs with at least 3 quintiles and min_count >= threshold
    valid_hrgs = (
        data_hrg_count.groupby("icb", "provider", "fyear", "sushrg_trimmed")
        .agg(
            F.countDistinct("imd_quintile").alias("n_quintiles"),
            F.min("count").alias("min_count"),
        )
        .filter((F.col("n_quintiles") > 2) & (F.col("min_count") >= min_count))
    )

    filtered = data_hrg_count.join(
        valid_hrgs.select("icb", "provider", "fyear", "sushrg_trimmed"),
        on=["icb", "provider", "fyear", "sushrg_trimmed"],
        how="inner",
    )

    schema = StructType(
        [
            StructField("icb", StringType()),
            StructField("provider", StringType()),
            StructField("fyear", IntegerType()),
            StructField("sushrg_trimmed", StringType()),
            StructField("pvalue", DoubleType()),
            StructField("slope", DoubleType()),
            StructField("intercept", DoubleType()),
        ]
    )

    def run_regression(pdf: pd.DataFrame) -> pd.DataFrame:
        results = []
        for hrg, hrg_df in pdf.groupby("sushrg_trimmed"):
            y = hrg_df["activity_rate"]
            x = sm.add_constant(hrg_df["imd_quintile"])
            res = sm.WLS(y, x, weights=hrg_df["pop"]).fit()
            results.append(
                {
                    "icb": hrg_df["icb"].iloc[0],
                    "provider": hrg_df["provider"].iloc[0],
                    "fyear": hrg_df["fyear"].iloc[0],
                    "sushrg_trimmed": hrg,
                    "pvalue": res.pvalues["imd_quintile"],
                    "slope": res.params["imd_quintile"],
                    "intercept": res.params["const"],
                }
            )
        return pd.DataFrame(results)

    linreg_df = filtered.groupby("icb", "provider", "fyear").applyInPandas(
        run_regression, schema=schema
    )

    return linreg_df.cache()


def process_calculated_inequalities(
    linreg_df: DataFrame, data_hrg_count: DataFrame
) -> DataFrame:
    """
    Process calculated inequalities in Spark and return a DataFrame
    with values for level_up, level_down, and zero_sum.

    :param linreg_df: The dataframe containing the calculated inequalities for each icb, provider and year
    :type linreg_df: DataFrame

    :param data_hrg_count: The dataframe containing the counts of activity per IMD quintile and HRG for each provider and year
    :type data_hrg_count: DataFrame

    :return: Dataframe containing the full inequalities calculations, in a format usable in inputs app
    :rtype: pd.DataFrame
    """

    hrgs_with_inequalities = linreg_df.filter(
        (F.col("pvalue") < 0.05) & (F.col("slope") > 0)
    ).select("icb", "provider", "sushrg_trimmed", "fyear", "slope", "intercept")

    df = data_hrg_count.join(
        hrgs_with_inequalities,
        on=["icb", "provider", "sushrg_trimmed", "fyear"],
        how="inner",
    )
    df = df.withColumn(
        "fitted_line", F.col("intercept") + F.col("slope") * F.col("imd_quintile")
    )
    agg = df.groupBy("icb", "provider", "sushrg_trimmed", "fyear").agg(
        F.max("fitted_line").alias("max_fitted"),
        F.min("fitted_line").alias("min_fitted"),
        F.avg("fitted_line").alias("mean_fitted"),
        F.min("imd_quintile").alias("min_quintile"),
        F.max("imd_quintile").alias("max_quintile"),
    )
    df = df.join(agg, on=["icb", "provider", "sushrg_trimmed", "fyear"], how="left")

    # level_up: max_fitted / activity_rate, but 1 for least deprived (max quintile)
    df = df.withColumn(
        "level_up",
        F.when(F.col("imd_quintile") == F.col("max_quintile"), 1.0).otherwise(
            F.col("max_fitted") / F.col("activity_rate")
        ),
    )
    # level_down: if min_fitted < 0 then 0, else min_fitted / activity_rate
    df = df.withColumn(
        "level_down",
        F.when(F.col("imd_quintile") == F.col("min_quintile"), 1.0)
        .when(F.col("min_fitted") < 0, 0.0)
        .otherwise(F.col("min_fitted") / F.col("activity_rate")),
    )
    # zero_sum: mean_fitted / activity_rate
    df = df.withColumn("zero_sum", F.col("mean_fitted") / F.col("activity_rate"))

    return df.select(
        "icb",
        "provider",
        "sushrg_trimmed",
        "fyear",
        "imd_quintile",
        "activity_rate",
        "fitted_line",
        "level_up",
        "level_down",
        "zero_sum",
    )


def main(path, spark):
    """
    Loads data, calculates inequalities and saves the results to parquet

    :param path: The path to save the results to
    :type path: str
    :param spark: The spark context to use
    :type spark: SparkSession

    """
    mlflow.autolog(
        log_input_examples=False,
        log_model_signatures=False,
        log_models=False,
        disable=True,
        exclusive=False,
        disable_for_unsupported_versions=True,
        silent=False,
    )
    fyears = [202324]

    data_hrg_count = load_inequalities_data(spark, fyears=fyears)
    linreg_df = calculate_inequalities(data_hrg_count, fyears=fyears)
    inequalities = process_calculated_inequalities(linreg_df, data_hrg_count)
    inequalities.to_parquet(f"{path}/inequalities.parquet")


if __name__ == "__main__":
    path = sys.argv[1]
    spark = get_spark()
    main(path, spark)
