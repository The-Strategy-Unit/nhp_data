from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from nhp.data.get_spark import get_spark
from nhp.data.hes.aae import get_hes_aae
from nhp.data.table_names import table_names


def _hes_aae_arrivals_remove_normalised_columns(df: DataFrame) -> DataFrame:
    """Remove normalised columns

    These columns are normalised in other tasks in the workflow."""

    return df.drop(
        *[i for i in df.columns for j in ["diag", "treat", "invest"] if i.startswith(j)]
    )


def _hes_aae_arrivals_add_imd_decile(df: DataFrame) -> DataFrame:
    """Recreate the IMD decile.

    - IMD04 on activity up to and including 2006-07
    - IMD07 on activity between 2007-08 and 2009-10
    - IMD10 on activity from 2010-11 and M10 2022-23
    - IMD19 from M11 2022-23
    """

    imdrk_to_ntile = (
        df.select("fyear", "imd04rk")
        .filter(F.col("imd04rk").isNotNull())
        .distinct()
        .withColumn(
            "imd_decile",
            F.ntile(10).over(Window.partitionBy("fyear").orderBy("imd04rk")),
        )
        .withColumn(
            "imd_quintile",
            F.ntile(5).over(Window.partitionBy("fyear").orderBy("imd04rk")),
        )
    )

    return (
        df.drop("imd04_decile")
        .join(imdrk_to_ntile, ["fyear", "imd04rk"], "left")
        .withColumn(
            "imd_version",
            F.when(F.col("imd04rk").isNull(), F.lit(None).cast("string"))
            .when(F.col("fyear") >= 202324, F.lit("IMD19"))
            # change happened in M10 2022/23, e.g. January 2023
            .when(F.year(F.col("arrivaldate")) == 2023, F.lit("IMD19"))
            .when(F.col("fyear") >= 201011, F.lit("IMD10"))
            .when(F.col("fyear") >= 200708, F.lit("IMD07"))
            .otherwise(F.lit("IMD04")),
        )
    )


def get_hes_aae_arrivals(spark: SparkSession) -> DataFrame:
    df = get_hes_aae(spark)

    fns = [
        _hes_aae_arrivals_remove_normalised_columns,
        _hes_aae_arrivals_add_imd_decile,
    ]
    return reduce(lambda x, fn: fn(x), fns, df)


def generate_aae_arrivals(spark: SparkSession) -> None:
    """Generate AAE data"""
    df = get_hes_aae_arrivals(spark)

    (
        df.select(*sorted(df.columns))
        .repartition("procode3")
        .write.mode("overwrite")
        .partitionBy(["fyear", "procode3"])
        .saveAsTable(table_names.hes_aae)
    )


def main() -> None:
    """main method"""
    spark = get_spark()
    generate_aae_arrivals(spark)
    generate_aae_arrivals(spark)
    generate_aae_arrivals(spark)
