"""Generate GAMs and HSA activity tables"""

import json
import sys
from functools import reduce

import joblib
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from sklearn.linear_model import Ridge
from sklearn.model_selection import GridSearchCV, KFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import SplineTransformer

from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def _get_data(spark: SparkSession, save_path: str, years: list[int]) -> pd.DataFrame:
    dfr = (
        reduce(
            DataFrame.unionByName,
            [
                (
                    spark.read.parquet(f"{save_path}/ip")
                    .groupBy("fyear", "age", "sex", "hsagrp")
                    .count()
                ),
                (
                    spark.read.parquet(f"{save_path}/op")
                    .groupBy("fyear", "age", "sex", "hsagrp")
                    .agg(F.sum("attendances").alias("count"))
                ),
                (
                    spark.read.parquet(f"{save_path}/aae")
                    .groupBy("fyear", "age", "sex", "hsagrp")
                    .agg(F.sum("arrivals").alias("count"))
                ),
            ],
        )
        .filter(~F.col("hsagrp").isin(["birth", "maternity", "paeds", "unknown"]))
        .filter(F.col("fyear").isin(years))
    )

    # load the demographics data, then cross join to the distinct HSA groups

    demog = (
        spark.read.table(table_names.population_projections_demographics)
        .filter(F.col("area_code").rlike("^E0[6-9]"))
        .filter(F.col("projection") == "migration_category")
        .filter(F.col("age") >= 18)
        .filter(F.col("year").isin(years))
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


def _create_spline_ridge_model(ages: np.ndarray, rates: np.ndarray) -> Pipeline:
    """Create spline ridge model for activity rate by age.

    Args:
        ages (np.ndarray): Array of ages.
        rates (np.ndarray): Array of activity rates corresponding to the ages.

    Returns:
        Pipeline: Fitted spline ridge model pipeline.
    """
    # spline + ridge model
    pipe = Pipeline(
        [
            (
                "spline",
                SplineTransformer(degree=3, include_bias=False, extrapolation="linear"),
            ),
            ("ridge", Ridge()),
        ]
    )

    # tune flexibility (n_knots) and smoothness (ridge alpha)
    param_grid = {
        "spline__n_knots": [5, 7, 9, 11, 13],
        "ridge__alpha": np.logspace(-4, 3, 20),
    }

    n_splits = max(2, min(5, len(ages)))
    cv = KFold(n_splits=n_splits, shuffle=True, random_state=42)

    search = GridSearchCV(
        estimator=pipe,
        param_grid=param_grid,
        scoring="neg_mean_squared_error",
        cv=cv,
        n_jobs=-1,
    )
    search.fit(ages, rates)

    return search.best_estimator_


def _generate_models(spark: SparkSession, save_path: str, years: list[int]) -> dict:
    dfr = _get_data(spark, save_path, years)

    models = {}
    for fyear, v2 in list(dfr.groupby("fyear")):
        models[fyear] = {
            k: _create_spline_ridge_model(
                v[["age"]].to_numpy(), v["activity_rate"].to_numpy()
            )
            for k, v in list(v2.groupby(["hsagrp", "sex"]))
        }

    return models


def main() -> None:
    """Generate HSA Activity Rates models"""
    data_version = sys.argv[1]
    years = [i // 100 for i in json.loads(sys.argv[2])]
    save_path = f"{table_names.model_data_path}/{data_version}"

    spark = get_spark()

    models = _generate_models(spark, save_path, years)
    joblib.dump(models, f"{save_path}/hsa_activity_rates_models.joblib")
