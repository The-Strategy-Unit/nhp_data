"""Process Subnational Population Projections (SNPP) data from ONS."""

import sys

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _process_demographics_file(
    spark: SparkSession, path: str, projection_year: int, projection_name: str
) -> None:
    years = [
        str(y)
        for y in range(
            projection_year, projection_year + (26 if projection_year == 2022 else 27)
        )
    ]
    stack_str = ", ".join(f"'{y}', `{y}`" for y in years)
    path = f"{path}/{projection_year}-projections/demographics"
    for sex_int, sex_string in [(1, "males"), (2, "females")]:
        (
            spark.read.csv(
                f"{path}/{projection_name}/{sex_string}.csv",
                header=True,
            )
            .filter(F.col("AGE_GROUP") != "All ages")
            .filter(F.col("AREA_CODE").rlike("^E0[6-9]"))
            .withColumn(
                "age",
                F.when(F.col("AGE_GROUP") == "90 and over", 90).otherwise(
                    F.col("AGE_GROUP").astype("int")
                ),
            )
            .selectExpr(
                "AREA_CODE as area_code",
                "age",
                f"stack({len(years)}, {stack_str}) as (year, value)",
            )
            .withColumn("value", F.col("value").cast("double"))
            .withColumn("sex", F.lit(sex_int))
            .withColumn("projection", F.lit(projection_name))
            .withColumn("projection_year", F.lit(projection_year))
            .orderBy("age")
            .repartition(1)
            .write.mode("overwrite")
            .partitionBy("projection_year", "projection", "sex", "area_code")
            .saveAsTable("nhp.population_projections.demographics")
        )


def _process_births_file(
    spark: SparkSession, path: str, projection_year: int, projection_name: str
) -> None:
    # births starts 1 year after demographics, so we need to adjust the years accordingly
    years = [
        str(y)
        for y in range(
            projection_year + 1,
            projection_year + (25 if projection_year == 2022 else 26),
        )
    ]
    stack_str = ", ".join(f"'{y}', `{y}`" for y in years)
    path = f"{path}/{projection_year}-projections/births"
    (
        spark.read.csv(
            f"{path}/{projection_name}/persons.csv",
            header=True,
        )
        .filter(F.col("AGE_GROUP") != "All ages")
        .filter(F.col("AREA_CODE").rlike("^E0[6-9]"))
        .withColumn(
            "age",
            F.when(F.col("AGE_GROUP") == "90 and over", 90).otherwise(
                F.col("AGE_GROUP").astype("int")
            ),
        )
        .selectExpr(
            "AREA_CODE as area_code",
            "age",
            f"stack({len(years)}, {stack_str}) as (year, value)",
        )
        .withColumn("value", F.col("value").cast("double"))
        .withColumn("projection", F.lit(projection_name))
        .withColumn("projection_year", F.lit(projection_year))
        .orderBy("age")
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("projection_year", "projection", "area_code")
        .saveAsTable("nhp.population_projections.births")
    )


def process_snpp_variant(
    spark: SparkSession, path: str, projection_year: int, projection_name: str
) -> None:
    """Process the SNPP variant data from ONS.

    :param spark: Spark session
    :type spark: SparkSession
    :param path: The path to the data
    :type path: str
    :param projection_year: The year the projections were created in
    :type projection_year: int
    :param projection_name: The name of the projection
    :type projection_name: str
    """
    _process_demographics_file(spark, path, projection_year, projection_name)
    _process_births_file(spark, path, projection_year, projection_name)


def main():
    path = sys.argv[1]
    projection_year = int(sys.argv[2])
    projection_name = sys.argv[3]

    spark: SparkSession = DatabricksSession.builder.getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    process_snpp_variant(spark, path, projection_year, projection_name)
