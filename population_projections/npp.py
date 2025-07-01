"""Process National Population Projections (NPP) data from ONS."""

import re
import sys

import pandas as pd
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def _process_npp_demographics(
    spark: SparkSession, df: DataFrame, projection_name: str, projection_year: int
):
    w = Window.partitionBy("year", "sex", "age")

    (
        spark.read.table("nhp.population_projections.demographics")
        .filter(F.col("projection") == "principal_proj")
        .filter(F.col("projection_year") == projection_year)
        .join(df, ["year", "sex", "age"])
        .withColumn("value", F.col("value") * F.col("pop") / F.sum("value").over(w))
        .withColumn("projection", F.lit(projection_name))
        .drop("pop")
        .orderBy("age")
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("projection_year", "projection", "sex", "area_code")
        .saveAsTable("nhp.population_projections.demographics")
    )


def _process_npp_births(
    spark: SparkSession, df: DataFrame, projection_name: str, projection_year: int
):
    w = Window.partitionBy("year", "sex", "age")

    (
        spark.read.table("nhp.population_projections.births")
        .filter(F.col("projection") == "principal_proj")
        .filter(F.col("projection_year") == projection_year)
        .join(df.filter(F.col("sex") == 2), ["year", "age"])
        .withColumn("value", F.col("value") * F.col("pop") / F.sum("value").over(w))
        .withColumn("projection", F.lit(projection_name))
        .drop("pop", "sex")
        .orderBy("age")
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("projection_year", "projection", "area_code")
        .saveAsTable("nhp.population_projections.births")
    )


def process_npp_variant(
    spark: SparkSession, path: str, projection_year: int, file: str
) -> None:
    """Process the NPP variant data from ONS.

    Process the national projections at subnational level.

    :param spark: The Spark context
    :type spark: SparkSession
    :param path: The path to the data
    :type path: str
    :param projection_year: The year the projections were created in
    :type projection_year: int
    :param file: The name of the file to load
    :type file: str
    """
    projection_names = {
        "hhh": "high_population",
        "hlh": "young_age_structure",
        "hpp": "high_fertility",
        "lhl": "old_age_structure",
        "lll": "low_population",
        "lpp": "low_fertility",
        "php": "high_life_expectancy",
        "plp": "low_life_expectancy",
        "pnp": "no_mortality_improvement",
        "pph": "high_intl_migration",
        "ppl": "low_intl_migration",
        "ppz": "zero_net_migration",
        "rpp": "replacement_fertility",
        # not in 2022 npp extract
        "cnp": "const_fertility_no_mortality_improvement",
        "cpp": "const_fertility",
        "ppr": "half_eu_migration",
        "ppq": "zero_eu_migration",
    }
    projection_name = projection_names[file]

    file_type = "xlsx" if projection_year >= 2022 else "xls"
    df = pd.read_excel(
        f"{path}/{projection_year}-projections/demographics/npp/{file}.{file_type}",
        sheet_name="Population",
    )
    df = df.rename(columns={"Age": "age", "Sex": "sex"})
    df["age"] = [
        min(90, int(re.sub(r"\s*(\d+).*", r"\1", i))) for i in df["age"].to_list()
    ]
    df = df.groupby(["sex", "age"], as_index=False).sum()
    df = df.melt(id_vars=["age", "sex"], var_name="year", value_name="pop")
    df["year"] = df["year"].astype("int")
    df.set_index(["year", "sex", "age"], inplace=True)

    df = df.loc[
        (slice(projection_year, projection_year + 25), slice(None), slice(None))
    ].reset_index()

    df = spark.createDataFrame(df)

    _process_npp_demographics(spark, df, projection_name, projection_year)
    _process_npp_births(spark, df, projection_name, projection_year)


def _init():
    path = sys.argv[1]
    projection_year = int(sys.argv[2])
    file = sys.argv[3]

    spark: SparkSession = DatabricksSession.builder.getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    process_npp_variant(spark, path, projection_year, file)


if __name__ == "__main__":
    _init()
