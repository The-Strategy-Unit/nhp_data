"""Extract birth factors data for model"""

import sys
from functools import partial

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from model_data.helpers import create_population_projections, get_spark


def _create_custom_birth_factors(
    fyear: int, spark: SparkSession, dataset: str, custom_projection_name: str
) -> DataFrame:
    """Create custom birth factors file for R0A66, using principal projection

    :param fyear: what year to extract
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
            f"/Volumes/nhp/model_data/files/dev/demographic_factors/fyear={fyear//100}/dataset={dataset}"
        )
        .filter(F.col("age").between(15, 44))
        .filter(F.col("sex") == 2)
        .filter(F.col("variant").isin("principal_proj", custom_projection_name))
        .drop("sex", "2018")
        .toPandas()
        .set_index("variant", "age")
    )

    principal_projection = demographics.loc[("principal_proj", slice(None))]

    custom_projection = demographics.loc[(custom_projection_name, slice(None))]

    multipliers = custom_projection / principal_projection

    df = (
        spark.read.parquet(
            f"/Volumes/nhp/model_data/files/dev/birth_factors/fyear={fyear//100}/dataset={dataset}"
        )
        .filter(F.col("variant") == "principal_proj")
        .drop("variant", "sex")
        .toPandas()
        .set_index("age")
    ) * multipliers

    df = (
        spark.createDataFrame(df.reset_index())
        .withColumn("sex", F.lit(2))
        .withColumn("variant", F.lit(custom_projection_name))
    )

    return df


def extract_birth_factors_data(
    save_path: str, fyear: int, spark: SparkSession = get_spark()
) -> None:
    """Extract Birth Factors data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    births = spark.read.table("nhp.population_projections.births").withColumn(
        "sex", F.lit(2)
    )

    fn = partial(_create_custom_birth_factors, fyear, spark)

    (
        # using a fixed year of 2018/19 to match prior logic
        create_population_projections(spark, births, 201819)
        .unionByName(fn("R0A", "custom_projection_R0A66"))
        .unionByName(fn("RD8", "custom_projection_RD8"))
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/birth_factors/fyear={fyear // 100}")
    )


if __name__ == "__main__":
    path = sys.argv[1]
    fyear = int(sys.argv[2])

    extract_birth_factors_data(path, fyear)
