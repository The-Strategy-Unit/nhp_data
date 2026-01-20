"""Generate Age/Sex Dataframe"""

import sys
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.acute_providers import filter_acute_providers
from nhp.data.inputs_data.ae import get_ae_age_sex_data
from nhp.data.inputs_data.helpers import inputs_age_group
from nhp.data.inputs_data.ip import get_ip_age_sex_data
from nhp.data.inputs_data.op import get_op_age_sex_data
from nhp.data.table_names import table_names


def get_age_sex(spark: SparkSession, geography_column: str) -> DataFrame:
    """Get age/sex (combined)

    :param spark: The spark session to use
    :type spark: SparkSession
    :param geography_column: The geography column to use
    :type geography_column: str
    :return: The age/sex data
    :rtype: DataFrame
    """
    fns = [
        get_ae_age_sex_data,
        get_ip_age_sex_data,
        get_op_age_sex_data,
    ]

    cols = ["fyear", geography_column, "strategy", "sex", "age", "n"]

    df = reduce(
        DataFrame.unionByName, [f(spark, geography_column).select(*cols) for f in fns]
    )

    age_groups = inputs_age_group(spark)

    return (
        df.join(age_groups, "age")
        .groupBy(*cols[:-2], "age_group")
        .agg(F.sum("n").alias("n"))
    )


def save_age_sex(path: str, spark: SparkSession, geography_column: str) -> None:
    """Save age/sex (combined) data.

    :param path: The path to save the data to
    :type path: str
    :param geography_column: The geography column to use
    :type geography_column: str
    :param spark: The spark session to use
    :type spark: SparkSession
    """
    df = get_age_sex(spark, geography_column).filter(F.col("n") > 5)

    if geography_column == "provider":
        df = filter_acute_providers(spark, df, "provider")
    df = df.filter(F.col(geography_column) != "unknown")

    df.toPandas().to_parquet(f"{path}/age_sex.parquet")


def main():
    geography_column = sys.argv[1]
    assert geography_column in ["provider", "lad23cd"], "invalid geography_column"

    path = f"{table_names.inputs_save_path}/{geography_column}"
    spark = get_spark()
    save_age_sex(path, spark, geography_column)
