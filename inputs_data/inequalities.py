"""Generate Inequalities Dataframe"""

# when running on databricks, we might need to change directory in order to
# import things correctly
import os

if not os.path.exists("readme.md"):
    os.chdir("..")

import sys

import mlflow
import pandas as pd
import statsmodels.api as sm
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from inputs_data.helpers import get_spark


def load_inequalities_data(spark: SparkSession, fyears: list) -> DataFrame:
    """
    :param spark: The spark context to use
    :type spark: SparkSession
    :param fyears: The financial years for the inequalities analysis, with each fyear as an int
    :type fyears: List

    :return: The dataframe containing the data required for inequalities analysis
    :rtype: DataFrame
    """

    imd_df = (
        spark.read.table("nhp.reference.population_by_imd_decile")
        .withColumn("imd_quintile", F.floor((F.col("imd19") - 1) / 2) + 1)
        .drop("imd19")
        .groupby("provider", "imd_quintile")
        .agg(F.sum("pop").alias("pop"))
        .withColumn(
            "population_share",
            F.col("pop") / F.sum("pop").over(Window.partitionBy("provider")),
        )
        .withColumn(
            "total_catchment_pop",
            F.sum("pop").over(Window.partitionBy("provider")),
        )
    )

    apc = (
        spark.read.table("nhp.default.apc")
        .filter(F.col("fyear").isin(fyears))
        .withColumn("sushrg_trimmed", F.expr("substring(sushrg, 1, 4)"))
        .filter(F.col("admimeth").startswith("1"))
        .select("provider", "imd_quintile", "sushrg_trimmed", "fyear")
        .groupby("provider", "imd_quintile", "sushrg_trimmed", "fyear")
        .agg(F.count("*").alias("count"))
    )

    opa = (
        spark.read.table("nhp.default.opa")
        .filter(F.col("fyear").isin(fyears))
        .filter(F.col("has_procedures"))
        .groupby("provider", "imd_quintile", "sushrg_trimmed", "fyear")
        .agg(F.sum("attendances").alias("count"))
    )

    data = opa.unionByName(apc)

    data_hrg_count = (
        data.groupby(["provider", "imd_quintile", "sushrg_trimmed", "fyear"])
        .agg(F.sum("count").alias("count"))
        .join(imd_df, on=["provider", "imd_quintile"])
        .withColumn("activity_rate", F.col("count") / (F.col("pop")))
    )

    return data_hrg_count


def calculate_inequalities(
    data_hrg_count: DataFrame, fyears: list, min_count: int = 50
) -> pd.DataFrame:
    """Calculate inequalities

    :param data_hrg_count: The dataframe containing the counts of activity per IMD quintile and HRG for each provider and year
    :type data_hrg_count: DataFrame
    :param fyears: The financial years for the inequalities analysis, with each fyear as an int
    :type fyears: List
    :param min_count: The minimum count of activity in an IMD quintile for a HRG to be used in analysis
    :type min_count: int

    :return: Dataframe containing the calculated pvalues, slopes, and intercepts
    :rtype: pd.DataFrame
    """

    results = []
    for fyear in fyears:
        providers = (
            data_hrg_count.filter(F.col("fyear") == fyear)
            .select("provider")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        for provider in providers:
            provider_df = (
                data_hrg_count.filter(F.col("fyear") == fyear)
                .filter(F.col("provider") == provider)
                .toPandas()
            )
            # remove HRGs where fewer than 3 quintiles are represented for that HRG
            hrg_filter = (
                provider_df.groupby(["sushrg_trimmed"])[["imd_quintile"]]
                .count()
                .sort_index()
            )
            hrg_filter = hrg_filter[hrg_filter["imd_quintile"] > 2]
            hrgs_to_keep = list(hrg_filter.index)
            # remove HRGs where counts of hrg activity < min_count for an imd quintile
            count_df = provider_df.groupby(["sushrg_trimmed", "imd_quintile"])[
                ["count"]
            ].sum()
            hrgs_to_exclude = []
            for i in count_df.index:
                if count_df.loc[i, "count"] < min_count:
                    hrgs_to_exclude.append(i[0])
            hrgs_to_exclude = set(hrgs_to_exclude)
            hrgs_to_include = [
                hrg for hrg in hrgs_to_keep if hrg not in hrgs_to_exclude
            ]
            # Perform linear regressions and save results
            for hrg in hrgs_to_include:
                hrg_df = provider_df[provider_df["sushrg_trimmed"] == hrg]
                y = hrg_df["activity_rate"]
                x = sm.add_constant(hrg_df["imd_quintile"])
                res = sm.WLS(y, x, weights=hrg_df["pop"]).fit()
                results.append(
                    {
                        "provider": provider,
                        "fyear": fyear,
                        "sushrg_trimmed": hrg,
                        "pvalue": res.pvalues.imd_quintile,
                        "slope": res.params.loc["imd_quintile"],
                        "intercept": res.params.loc["const"],
                    }
                )
    linreg_df = pd.DataFrame(results)
    return linreg_df


def process_calculated_inequalities(
    linreg_df: pd.DataFrame, data_hrg_count: DataFrame
) -> pd.DataFrame:
    """
    Process the calculated inequalities and return a dataframe containing the values for level_up, level_down, and zero_sum

    :param linreg_df: The dataframe containing the counts of activity per IMD quintile and HRG for each provider and year
    :type linreg_df: pd.DataFrame

    :param data_hrg_count: The dataframe containing the counts of activity per IMD quintile and HRG for each provider and year
    :type data_hrg_count: DataFrame

    :return: Dataframe containing the full inequalities calculations, in a format usable in inputs app
    :rtype: pd.DataFrame
    """

    # Filter to only HRGs with significant inequalities with positive slopes
    hrgs_with_inequalities = (
        linreg_df[(linreg_df["pvalue"] < 0.05) & (linreg_df["slope"] > 0)]
        .reset_index(drop=True)
        .set_index(["provider", "sushrg_trimmed", "fyear"])
    )
    df = (
        data_hrg_count.toPandas()
        .join(
            hrgs_with_inequalities,
            on=["provider", "sushrg_trimmed", "fyear"],
            how="inner",
        )
        .set_index(["provider", "sushrg_trimmed", "fyear", "imd_quintile"])
        .sort_index()
        .reset_index(level="imd_quintile")
    )
    # Calculate values to use for level_up, level_down, and zero_sum for each combination of provider, fyear, and HRG
    for i in df.index:
        mini_df = df.loc[i].set_index("imd_quintile", append=True)
        quintiles = mini_df.index.get_level_values("imd_quintile")
        mini_df.loc[:, "fitted_line"] = (
            mini_df.loc[:, "intercept"] + mini_df.loc[:, "slope"] * quintiles
        )
        # Calculate level_up, set least deprived quintile factor to 1
        mini_df.loc[:, "level_up"] = (
            mini_df.loc[:, "fitted_line"].max() / mini_df.loc[:, "activity_rate"]
        )
        mini_df.loc[
            (slice(None), slice(None), slice(None), quintiles[-1]), "level_up"
        ] = 1
        # Calculate level_down, set most deprived quintile factor to 1
        if mini_df.loc[:, "fitted_line"].min() < 0:
            mini_df.loc[:, "level_down"] = 0
        else:
            mini_df.loc[:, "level_down"] = (
                mini_df.loc[:, "fitted_line"].min() / mini_df.loc[:, "activity_rate"]
            )
        mini_df.loc[
            (slice(None), slice(None), slice(None), quintiles[0]), "level_down"
        ] = 1
        # Calculate zero_sum
        mini_df.loc[:, "zero_sum"] = (
            mini_df.loc[:, "fitted_line"].mean() / mini_df.loc[:, "activity_rate"]
        )
        # Add calculated values back to main dataframe
        df.loc[i, ["fitted_line", "level_up", "level_down", "zero_sum"]] = (
            mini_df.reset_index("imd_quintile")[
                ["fitted_line", "level_up", "level_down", "zero_sum"]
            ]
        )
    return df


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
    fyears = [201920, 202223, 202324]

    data_hrg_count = load_inequalities_data(spark, fyears=fyears)
    linreg_df = calculate_inequalities(data_hrg_count, fyears=fyears)
    inequalities = process_calculated_inequalities(linreg_df, data_hrg_count)
    inequalities.to_parquet(f"{path}/inequalities.parquet")


if __name__ == "__main__":
    path = sys.argv[1]
    spark = get_spark()
    main(path, spark)
