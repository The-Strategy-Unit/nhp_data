"""Generate Age/Sex Dataframe"""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.get_spark import get_spark
from nhp.data.inputs_data.ae import get_ae_age_sex_data
from nhp.data.inputs_data.ip import get_ip_age_sex_data
from nhp.data.inputs_data.op import get_op_age_sex_data
from nhp.data.table_names import table_names


def get_age_sex(spark: SparkSession) -> DataFrame:
    """Get age/sex (combined)

    :param spark: The spark session to use
    :type spark: SparkSession
    :return: The age/sex data
    :rtype: DataFrame
    """
    fns = [
        get_ae_age_sex_data,
        lambda spark: get_ip_age_sex_data(spark).drop("speldur"),
        get_op_age_sex_data,
    ]

    return reduce(DataFrame.unionByName, [f(spark) for f in fns])


def save_age_sex(path: str, spark: SparkSession) -> None:
    """Save age/sex (combined) data.

    :param path: The path to save the data to
    :type path: str
    :param spark: The spark sesssion to use
    :type spark: SparkSession
    """
    df = get_age_sex(spark).filter(F.col("n") > 5).toPandas()
    df.to_parquet(f"{path}/age_sex.parquet")


def main():
    path = table_names.inputs_save_path
    spark = get_spark()
    save_age_sex(path, spark)
