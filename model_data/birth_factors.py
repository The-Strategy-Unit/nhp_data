"""Extract birth factors data for model"""

import sys
from functools import partial

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from model_data.helpers import (
    DEMOGRAPHICS_MAX_YEAR,
    DEMOGRAPHICS_MIN_YEAR,
    create_population_projections,
    get_spark,
)


def _create_custom_birth_factors(
    path: str,
    fyear: int,
    spark: SparkSession,
    dataset: str,
    custom_projection_name: str,
) -> DataFrame:
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
    :return: dataframe containing the custom birth factors
    :rtype: DataFrame
    """

    demographics = (
        spark.read.parquet(
            f"{path}/demographic_factors/fyear={fyear//100}/dataset={dataset}"
        )
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
        spark.read.parquet(f"{path}/birth_factors/fyear={fyear//100}/dataset={dataset}")
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

    fn = partial(_create_custom_birth_factors, save_path, fyear, spark)

    (
        create_population_projections(spark, births, fyear, projection_year)
        .unionByName(fn("R0A", "custom_projection_R0A66"))
        .unionByName(fn("RD8", "custom_projection_RD8"))
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/birth_factors/fyear={fyear // 100}")
    )


def main():
    path = sys.argv[1]
    fyear = int(sys.argv[2])
    projection_year = int(sys.argv[3])

    extract(path, fyear, projection_year)


if __name__ == "__main__":
    main()
