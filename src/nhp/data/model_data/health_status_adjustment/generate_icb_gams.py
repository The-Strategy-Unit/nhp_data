"""Generate GAMs and HSA activity tables"""

import json
import sys
from functools import reduce
from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pygam import GAM
from pyspark.sql import DataFrame, SparkSession, Window

from nhp.data.get_spark import get_spark
from nhp.data.model_data.helpers import create_icb_population_projections
from nhp.data.table_names import table_names


def _get_data(spark: SparkSession, save_path: str, years: list[int]) -> DataFrame:
    dfr = (
        reduce(
            DataFrame.unionByName,
            [
                (
                    spark.read.parquet(f"{save_path}/ip")
                    .groupBy("fyear", "icb", "age", "sex", "hsagrp")
                    .count()
                ),
                (
                    spark.read.parquet(f"{save_path}/op")
                    .groupBy("fyear", "icb", "age", "sex", "hsagrp")
                    .agg(F.sum("attendances").alias("count"))
                ),
                (
                    spark.read.parquet(f"{save_path}/aae")
                    .groupBy("fyear", "icb", "age", "sex", "hsagrp")
                    .agg(F.sum("arrivals").alias("count"))
                ),
            ],
        )
        .filter(~F.col("hsagrp").isin(["birth", "maternity", "paeds", "unknown"]))
        .filter(~F.col("hsagrp").startswith("op_maternity_"))
        .filter(F.col("fyear").isin(years))
        .filter(F.col("age") >= 18)
    )

    # load the demographics data
    demog = (
        create_icb_population_projections(
            spark, spark.read.table(table_names.population_projections_demographics)
        )
        .filter(F.col("variant") == "migration_category")
        .filter(F.col("age") >= 18)
        .selectExpr(
            "age",
            "sex",
            "icb",
            "fyear as base_fyear",
            f"stack({len(years)}, "
            + ", ".join([f"'{y}', `{y}`" for y in years])
            + ") as (fyear, pop)",
        )
        .filter(F.col("base_fyear") == F.col("fyear"))
        .drop("base_fyear")
        .withColumn("fyear", F.col("fyear").cast("int"))
        .join(
            dfr.select("icb", "sex", "hsagrp").distinct(),
            ["icb", "sex"],
            "inner",
        )
    )

    # generate the data. we right join to the demographics and fill the missing rows with 0's,
    # before calculating the activity rate as the amount of activity (count) divided by the
    # population.
    df = (
        dfr.join(demog, ["age", "sex", "icb", "hsagrp", "fyear"], "right")
        .fillna(0)
        .withColumn("activity_rate", F.col("count") / F.col("pop"))
        .drop("count", "pop")
    )

    # remove any rows where all the activity rates are 0
    w = Window.partitionBy("sex", "icb", "hsagrp", "fyear")
    to_remove = (
        df.withColumn(
            "all_zero",
            F.sum((F.col("activity_rate") == 0).cast("int")).over(w)
            == F.count("activity_rate").over(w),
        )
        .filter(F.col("all_zero"))
        .select("icb", "hsagrp", "sex", "fyear")
        .distinct()
    )

    return df.join(to_remove, ["icb", "hsagrp", "sex", "fyear"], "anti")


def _generate_gam(data: pd.DataFrame, progress: bool = False) -> Any:
    x = data[["age"]].to_numpy()
    y = data["activity_rate"].to_numpy()

    return GAM().gridsearch(x, y, progress=progress)


def _generate_gams(spark_df: DataFrame) -> dict:
    # generate the GAMs as a nested dictionary by icb/year/(HSA group, sex).
    # This may be amenable to some parallelisation? or other speed tricks possible with pygam?

    dfr = spark_df.toPandas()
    print("Generating GAMs")
    all_gams = {}
    to_iterate = list(dfr.groupby("icb"))
    n = len(to_iterate)
    for i, (icb, v1) in enumerate(to_iterate):
        all_gams[icb] = {}
        print(f"> {icb} {i}/{n} ({i / n * 100:.1f}%)")
        for fyear, v2 in list(v1.groupby("fyear")):
            g = {k: _generate_gam(v) for k, v in list(v2.groupby(["hsagrp", "sex"]))}
            all_gams[icb][fyear] = g
    return all_gams


def _generate_activity_tables(spark: SparkSession, all_gams: dict) -> None:
    # Generate activity tables
    #
    # we usually rely on interpolated values in the model for efficiency, generate these tables and
    # store in a table in databricks
    all_ages = np.arange(0, 101)

    def to_fyear(year):
        return year * 100 + (year + 1) % 100

    hsa_activity_tables = spark.createDataFrame(
        pd.concat(
            {
                icb: pd.concat(
                    {
                        to_fyear(year): pd.concat(
                            {
                                k: pd.Series(
                                    g.predict(all_ages), index=all_ages, name="activity"
                                )
                                for k, g in v2.items()
                            }
                        )
                        for year, v2 in v1.items()
                    }
                )
                for icb, v1 in all_gams.items()
            }
        )
        .rename_axis(["icb", "fyear", "hsagrp", "sex", "age"])
        .reset_index()
    )

    for i in ["fyear", "sex", "age"]:
        hsa_activity_tables = hsa_activity_tables.withColumn(i, F.col(i).cast("int"))

    hsa_activity_tables.write.mode("overwrite").saveAsTable(
        table_names.default_hsa_activity_tables_icb
    )


def main() -> None:
    """Generate GAMs and HSA activity tables"""
    data_version = sys.argv[1]
    years = [i // 100 for i in json.loads(sys.argv[2])]
    save_path = f"{table_names.model_data_path}/{data_version}"

    spark = get_spark()

    dfr = _get_data(spark, save_path, years)
    all_gams = _generate_gams(dfr)
    _generate_activity_tables(spark, all_gams)
