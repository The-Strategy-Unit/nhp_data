"""Extract demographic factors data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from model_data.helpers import create_population_projections, get_spark


# pylint: disable=invalid-name
def _create_custom_demographic_factors_RD8(
    spark: SparkSession = get_spark(),
) -> DataFrame:
    """Create custom demographic factors file for RD8 using agreed methodology

    :param spark: the spark context to use
    :type spark: SparkSession
    """
    # Load demographics - principal projection only
    custom_file = (
        spark.read.csv(
            "/Volumes/nhp/population_projections/files/RD8_population_projection V2.csv",
            header=True,
            inferSchema=True,
        )
        .withColumnRenamed("Sex", "sex")
        .withColumnRenamed("Age", "age")
        .drop("Type")
        .withColumn("dataset", F.lit("RD8"))
        .withColumn("variant", F.lit("custom_projection_RD8"))
    )
    return custom_file


# pylint: disable=invalid-name
def _create_custom_demographic_factors_R0A66(
    spark: SparkSession = get_spark(),
) -> DataFrame:
    """Create custom demographic factors file for R0A66 using agreed methodology

    :param spark: the spark context to use
    :type spark: SparkSession
    """
    # Load demographics - principal projection only
    demographics = (
        spark.read.table("nhp.population_projections.demographics")
        .filter(F.col("projection") == "principal_proj")
        .filter(F.col("projection_year") == 2018)
        .filter(F.col("area_code") != "E08000003")
        .drop("projection", "projection_year")
    )
    # Load custom file
    # TODO: this should be moved into population_projections scope
    years = [str(y) for y in range(2018, 2044)]
    stack_str = ", ".join(f"'{y}', `{y}`" for y in years)
    custom_file = (
        spark.read.csv(
            "/Volumes/nhp/population_projections/files/ManchesterCityCouncil_custom_E08000003.csv",
            header=True,
            inferSchema=True,
        )
        .withColumnRenamed("Sex", "sex")
        .withColumnRenamed("Age", "age")
        .withColumn(
            "sex", F.when(F.col("sex") == "male", 1).when(F.col("sex") == "female", 2)
        )
        .withColumn("area_code", F.lit("E08000003"))
        .selectExpr(
            "area_code",
            "sex",
            "age",
            f"stack({len(years)}, {stack_str}) as (year, value)",
        )
        .orderBy("age")
    )
    demographics = demographics.unionByName(custom_file)
    # Work out catchment with patched demographics
    total_window = Window.partitionBy("provider")
    df = (
        spark.read.table("nhp.raw_data.apc")
        .filter(F.col("sitetret") == "R0A66")
        .filter(F.col("fyear") == 202324)
        .filter(F.col("resladst_ons").rlike("^E0[6-9]"))
        .groupBy("provider", "resladst_ons")
        .count()
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .filter(F.col("pcnt") > 0.05)
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .withColumnRenamed("resladst_ons", "area_code")
        .withColumnRenamed("provider", "dataset")
        .join(demographics, "area_code")
        .withColumn("variant", F.lit("custom_projection_R0A66"))
        .withColumn("value", F.col("value") * F.col("pcnt"))
        .groupBy("dataset", "age", "sex", "variant")
        .pivot("year")
        .agg(F.sum("value"))
        .orderBy("dataset", "age", "sex", "variant")
    )
    return df


def extract(save_path: str, fyear: int, spark: SparkSession = get_spark()) -> None:
    """Extract Demographic Factors data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    demographics = spark.read.table("nhp.population_projections.demographics")

    custom_R0A = _create_custom_demographic_factors_R0A66(spark)
    custom_RD8 = _create_custom_demographic_factors_RD8(spark)

    (
        # using a fixed year of 2018/19 to match prior logic
        create_population_projections(spark, demographics, 201819)
        .unionByName(custom_R0A)
        .unionByName(custom_RD8)
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/demographic_factors/fyear={fyear // 100}")
    )


def main():
    path = sys.argv[1]
    fyear = int(sys.argv[2])

    extract(path, fyear)


if __name__ == "__main__":
    main()
