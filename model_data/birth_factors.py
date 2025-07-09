"""Extract birth factors data for model"""

import sys

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from model_data.helpers import (
    DEMOGRAPHICS_MAX_YEAR,
    DEMOGRAPHICS_MIN_YEAR,
    create_population_projections,
    get_spark,
)


def create_custom_birth_factors(
    path: str,
    fyear: int,
    spark: SparkSession,
    dataset: str,
    custom_projection_name: str,
) -> tuple[DataFrame, str]:
    """Create custom birth factors file for R0A66, using migration category variant

    :param path: where to read the demographics from
    :type path: str
    :type fyear: int
    :param spark: the spark context to use
    :type spark: SparkSession
    :param dataset: the dataset to extract
    :type dataset: str
    :param custom_projection_name: the name of the custom projection
    :type custom_projection_name: str
    :return: dataframe containing the custom birth factors, and the path where to save the births factors to
    :rtype: tuple[DataFrame, str]
    """

    demographics_path = (
        f"{path}/demographic_factors/fyear={fyear//100}/dataset={dataset}"
    )
    births_path = f"{path}/birth_factors/fyear={fyear//100}/dataset={dataset}"

    demographics = (
        spark.read.parquet(demographics_path)
        .filter(F.col("age").between(15, 44))
        .filter(F.col("sex") == 2)
        .filter(F.col("variant").isin("migration_category", custom_projection_name))
        .drop("sex")
        .toPandas()
        .set_index(["variant", "age"])
    )

    principal_projection = demographics.loc[("migration_category", slice(None))]

    custom_projection = demographics.loc[(custom_projection_name, slice(None))]

    multipliers = custom_projection / principal_projection

    df = (
        spark.read.parquet(births_path)
        .filter(F.col("variant") == "migration_category")
        .drop("variant", "sex")
        .toPandas()
        .set_index("age")
    ) * multipliers

    return (
        spark.createDataFrame(df.reset_index())
        .withColumn("sex", F.lit(2))
        .withColumn("variant", F.lit(custom_projection_name))
        .withColumn("dataset", F.lit(dataset))
    )


def extract(
    save_path: str, fyear: int, projection_year: int, spark: SparkSession = get_spark()
) -> None:
    """Extract Birth Factors data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    births = (
        spark.read.table("nhp.population_projections.births")
        .withColumn("sex", F.lit(2))
        .filter(F.col("year").between(DEMOGRAPHICS_MIN_YEAR, DEMOGRAPHICS_MAX_YEAR))
    )

    (
        create_population_projections(spark, births, fyear, projection_year)
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/birth_factors/fyear={fyear // 100}")
    )

    # iterate over and create the custom birth projections. we need to make sure that the standard
    # birth projections have been created before running this step
    for dataset, projection_name in [
        ("R0A", "custom_projection_R0A66"),
        ("RD8", "custom_projection_RD8"),
    ]:
        custom_birth_factors = create_custom_birth_factors(
            save_path, fyear, spark, dataset, projection_name
        )
        births = births.unionByName(custom_birth_factors)
    births.repartition(1).write.mode("overwrite").partitionBy(
        "dataset"
    ).parquet(f"{save_path}/birth_factors/fyear={fyear // 100}")


def main():
    path = sys.argv[1]
    fyear = int(sys.argv[2])
    projection_year = int(sys.argv[3])

    extract(path, fyear, projection_year)


if __name__ == "__main__":
    main()