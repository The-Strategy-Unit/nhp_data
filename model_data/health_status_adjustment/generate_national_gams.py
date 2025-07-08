"""Generate GAMs and HSA activity tables"""

import sys
from functools import reduce

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pygam import GAM
from pyspark.sql import DataFrame, SparkSession


def _get_data(spark: SparkSession, save_path: str) -> pd.DataFrame:
    dfr = (
        reduce(
            DataFrame.unionByName,
            [
                (
                    spark.read.parquet(f"{save_path}/{ds}")
                    .groupBy("fyear", "age", "sex", "hsagrp")
                    .count()
                )
                for ds in ["ip", "op", "aae"]
            ],
        )
        .filter(~F.col("hsagrp").isin(["birth", "maternity", "paeds"]))
        .filter(F.col("fyear").isin([2023]))
    )

    # load the demographics data, then cross join to the distinct HSA groups

    demog = (
        spark.read.table("nhp.population_projections.demographics")
        .filter(F.col("area_code").rlike("^E0[6-9]"))
        .filter(F.col("projection") == "migration_category")
        .filter(F.col("age") >= 18)
        .filter(F.col("year") == 2023)
        .groupBy("age", "sex")
        .agg(F.sum("value").alias("pop"))
        .crossJoin(dfr.select("hsagrp").distinct())
    )

    # generate the data. we right join to the demographics and fill the missing rows with 0's,
    # before calculating the activity rate as the amount of activity (count) divided by the
    # population.

    return (
        dfr.join(demog, ["age", "sex", "hsagrp"], "right")
        .fillna(0)
        .filter(F.col("fyear") > 0)
        .withColumn("activity_rate", F.col("count") / F.col("pop"))
        .drop("count", "pop")
        .toPandas()
    )


def _generate_gams(spark: SparkSession, save_path: str) -> dict:
    dfr = _get_data(spark, save_path)

    # generate the GAMs as a nested dictionary by dataset/year/(HSA group, sex).
    # This may be amenable to some parallelisation? or other speed tricks possible with pygam?

    all_gams = {"NATIONAL": {}}
    for fyear, v2 in list(dfr.groupby("fyear")):
        g = {
            k: GAM().gridsearch(
                v[["age"]].to_numpy(), v["activity_rate"].to_numpy(), progress=False
            )
            for k, v in list(v2.groupby(["hsagrp", "sex"]))
        }
        all_gams["NATIONAL"][fyear] = g
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

    hsa_activity_tables.write.mode("overwrite").saveAsTable(
        "hsa_activity_tables_NATIONAL"
    )


def main(save_path: str) -> None:
    """Generate GAMs and HSA activity tables

    :param save_path: where to save the gams
    :type save_path: str
    """
    spark: SparkSession = DatabricksSession.builder.getOrCreate()
    spark.catalog.setCurrentCatalog("nhp")
    spark.catalog.setCurrentDatabase("default")

    all_gams = _generate_gams(spark, save_path)
    _generate_activity_tables(spark, all_gams)


if __name__ == "__main__":
    main(*sys.argv[1:])
