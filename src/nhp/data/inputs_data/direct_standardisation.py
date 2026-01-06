from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.reference.population_by_lsoa21 import get_pop_by_lad23

# cache the reference population dataframe and persist in memory
_REFERENCE_POP_DF: DataFrame | None = None


def reference_pop(spark: SparkSession) -> DataFrame:
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
    func: Callable[[SparkSession], DataFrame],
) -> Callable[[SparkSession], DataFrame]:
    def wrapper(spark: SparkSession) -> DataFrame:
        df = func(spark)

        ref_pop = (
            df.groupBy("fyear", "strategy", "sex")
            .agg(F.min("age").alias("min_age"), F.max("age").alias("max_age"))
            .alias("x")
            .join(
                reference_pop(spark).alias("y").hint("range_join", 1),
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

        df_with_ref_pop = (
            df.join(ref_pop, ["fyear", "strategy", "age", "sex"], "right")
            .fillna(0, subset=["n"])
            .fillna(1, subset=["d"])
            .filter(F.col("provider").isNotNull())  # ty: ignore[missing-argument]
            .select(
                "fyear",
                "strategy",
                "provider",
                "age",
                "sex",
                "n",
                "d",
                "reference_population",
            )
        )

        national_df = (
            df_with_ref_pop.groupBy("fyear", "strategy", "age", "sex")
            .agg(
                F.sum("n").alias("n"),
                F.sum("d").alias("d"),
                F.sum("reference_population").alias("reference_population"),
            )
            .withColumn("provider", F.lit("national"))
        )

        return (
            df_with_ref_pop.unionByName(national_df)
            .groupBy("fyear", "strategy", "provider")
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
