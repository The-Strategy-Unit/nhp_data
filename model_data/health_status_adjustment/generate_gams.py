"""Generate GAMs and HSA activity tables"""

import os
import pickle as pkl
import sys
from functools import reduce
from typing import Any

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pygam import GAM
from pyspark.sql import DataFrame, SparkSession


def _get_data(spark: SparkSession, save_path: str) -> DataFrame:
    dfr = (
        reduce(
            DataFrame.unionByName,
            [
                (
                    spark.read.parquet(f"{save_path}/{ds}")
                    .groupBy("fyear", "dataset", "age", "sex", "hsagrp")
                    .count()
                )
                for ds in ["ip", "op", "aae"]
            ],
        )
        .filter(~F.col("hsagrp").isin(["birth", "maternity", "paeds"]))
        .filter(F.col("fyear").isin([2019, 2022, 2023]))
        .filter(F.col("age") >= 18)
    )

    # load the demographics data
    demog = (
        spark.read.parquet(f"{save_path}/demographic_factors/fyear=2019/")
        .filter(F.col("variant") == "principal_proj")
        .filter(F.col("age") >= 18)
        .select(
            F.col("age"), F.col("sex"), F.col("dataset"), F.col("2019").alias("pop")
        )
        # join back to the unique combination of dataset/sex/fyear/hsagrp, we
        # will use this below to ensure we have a 0-count row of activity
        .join(
            dfr.select("dataset", "sex", "fyear", "hsagrp").distinct(),
            ["dataset", "sex"],
            "inner",
        )
    )

    # generate the data. we right join to the demographics and fill the missing rows with 0's,
    # before calculating the activity rate as the amount of activity (count) divided by the
    # population.
    return (
        dfr.join(demog, ["age", "sex", "dataset", "hsagrp", "fyear"], "right")
        .fillna(0)
        .withColumn("activity_rate", F.col("count") / F.col("pop"))
        .drop("count", "pop")
    )


def _generate_gam(data: pd.DataFrame, progress: bool = False) -> Any:
    x = data[["age"]].to_numpy()
    y = data["activity_rate"].to_numpy()

    return GAM().gridsearch(x, y, progress=progress)


def _generate_gams(save_path: str, dfr: DataFrame) -> dict:
    # generate the GAMs as a nested dictionary by dataset/year/(HSA group, sex).
    # This may be amenable to some parallelisation? or other speed tricks possible with pygam?

    dfr = dfr.toPandas()
    print("Generating GAMs")
    all_gams = {}
    to_iterate = list(dfr.groupby("dataset"))
    n = len(to_iterate)
    for i, (dataset, v1) in enumerate(to_iterate):
        all_gams[dataset] = {}
        print(f"> {dataset} {i}/{n} ({i/n*100:.1f}%)")
        for fyear, v2 in list(v1.groupby("fyear")):
            g = {k: _generate_gam(v) for k, v in list(v2.groupby(["hsagrp", "sex"]))}
            all_gams[dataset][fyear] = g

            path = f"{save_path}/hsa_gams/{fyear=}/dataset={dataset}"
            os.makedirs(path, exist_ok=True)
            with open(f"{path}/hsa_gams.pkl", "wb") as f:
                pkl.dump(g, f)
    return all_gams


def _generate_activity_tables(
    spark: SparkSession, save_path: str, all_gams: dict
) -> None:
    # Generate activity tables
    #
    # we usually rely on interpolated values in the model for efficiency, generate these tables and
    # store in a table in databricks
    all_ages = np.arange(0, 101)

    def to_fyear(year):
        return year * 100 + (year + 1) % 100

    def from_fyear(fyear):
        return fyear // 100

    hsa_activity_tables = spark.createDataFrame(
        pd.concat(
            {
                dataset: pd.concat(
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
                for dataset, v1 in all_gams.items()
            }
        )
        .rename_axis(["dataset", "fyear", "hsagrp", "sex", "age"])
        .reset_index()
    )

    for i in ["fyear", "sex", "age"]:
        hsa_activity_tables = hsa_activity_tables.withColumn(i, F.col(i).cast("int"))

    hsa_activity_tables.write.mode("overwrite").saveAsTable("hsa_activity_tables")

    # Save out to the storage location used by the docker containers

    (
        spark.read.table("hsa_activity_tables")
        .filter(F.col("fyear").isin([201920, 202223, 202324]))
        .withColumn("fyear", F.udf(from_fyear)("fyear"))
        .withColumnRenamed("provider", "dataset")
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("fyear", "dataset")
        .parquet(f"{save_path}/hsa_activity_tables")
    )


def main(save_path: str) -> None:
    """Generate GAMs and HSA activity tables

    :param save_path: where to save the gams
    :type save_path: str
    """
    spark: SparkSession = DatabricksSession.builder.getOrCreate()
    spark.catalog.setCurrentCatalog("nhp")
    spark.catalog.setCurrentDatabase("default")

    dfr = _get_data(spark, save_path)
    all_gams = _generate_gams(save_path, dfr)
    _generate_activity_tables(spark, save_path, all_gams)


if __name__ == "__main__":
    main(*sys.argv[1:])
