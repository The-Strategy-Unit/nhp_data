"""Extract data for model"""

# dbutils.widgets.text("version", "dev")
# dbutils.widgets.text("fyear", "201920")


import sys

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pyspark.context import SparkContext
from pyspark.sql import DataFrame


def add_tretspef_column(self: DataFrame) -> DataFrame:
    """Add tretspef column to DataFrame

    :param self: The data frame to add the tretpsef column to
    :type df: DataFrame
    :return: The data frame
    :rtype: DataFrame
    """

    specialties = [
        "100",
        "101",
        "110",
        "120",
        "130",
        "140",
        "150",
        "160",
        "170",
        "300",
        "301",
        "320",
        "330",
        "340",
        "400",
        "410",
        "430",
        "520",
    ]

    tretspef_column = (
        F.when(F.col("tretspef_raw").isin(specialties), F.col("tretspef_raw"))
        .when(F.expr("tretspef_raw RLIKE '^1(?!80|9[02])'"), F.lit("Other (Surgical)"))
        .when(
            F.expr("tretspef_raw RLIKE '^(1(80|9[02])|[2346]|5(?!60)|83[134])'"),
            F.lit("Other (Medical)"),
        )
        .otherwise(F.lit("Other"))
    )

    return self.withColumn("tretspef", tretspef_column)


def extract_apc_data(spark: SparkContext, save_path: str, fyear: int) -> None:
    """Extract APC (+mitigators) data

    :param spark: the spark context to use
    :type spark: SparkContext
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    apc = (
        spark.read.table("apc")
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("epikey", "rn")
        .withColumnRenamed("provider", "dataset")
        .withColumn("tretspef_raw", F.col("tretspef"))
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .withColumn("sex", F.col("sex").cast("int"))
        .withColumn("sushrg_trimmed", F.expr("substring(sushrg, 1, 4)"))
    )

    apc = add_tretspef_column(apc)

    (
        apc.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/ip")
    )

    for k, v in [
        ("activity_avoidance", "activity_avoidance"),
        ("efficiencies", "efficiency"),
    ]:

        (
            spark.read.table("apc_mitigators")
            .filter(F.col("type") == v)
            .drop("type")
            .withColumnRenamed("epikey", "rn")
            .join(apc, "rn", "inner")
            .select("dataset", "fyear", "rn", "strategy", "sample_rate")
            .repartition(1)
            .write.mode("overwrite")
            .partitionBy(["fyear", "dataset"])
            .parquet(f"{save_path}/ip_{k}_strategies")
        )


def extract_opa_data(spark: SparkContext, save_path: str, fyear: int) -> None:
    """Extract OPA data

    :param spark: the spark context to use
    :type spark: SparkContext
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    opa = (
        spark.read.table("opa")
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .withColumn("tretspef_raw", F.col("tretspef"))
        .withColumn("is_wla", F.lit(True))
    )

    opa = add_tretspef_column(opa)

    (
        opa.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/op")
    )


def extract_ecds_data(spark: SparkContext, save_path: str, fyear: int) -> None:
    """Extract ECDS data

    :param spark: the spark context to use
    :type spark: SparkContext
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    ecds = (
        spark.read.table("ecds")
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
    )

    (
        ecds.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/aae")
    )


def extract_birth_factors_data(spark: SparkContext, save_path: str, fyear: int) -> None:
    """Extract Birth Factors data

    :param spark: the spark context to use
    :type spark: SparkContext
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    df = spark.read.table("birth_factors").withColumnRenamed("provider", "dataset")

    (
        df.repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/birth_factors/fyear={fyear // 100}")
    )


def extract_demographic_factors_data(
    spark: SparkContext, save_path: str, fyear: int
) -> None:
    """Extract Birth Factors data

    :param spark: the spark context to use
    :type spark: SparkContext
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    df = spark.read.table("demographic_factors").withColumnRenamed(
        "provider", "dataset"
    )

    (
        df.repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/demographic_factors/fyear={fyear // 100}")
    )


def main(save_path: str, fyear: int) -> None:
    """Main method

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    spark: SparkContext = DatabricksSession.builder.getOrCreate()

    spark.catalog.setCurrentCatalog("nhp")
    spark.catalog.setCurrentDatabase("default")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    extract_apc_data(spark, save_path, fyear)
    extract_opa_data(spark, save_path, fyear)
    extract_ecds_data(spark, save_path, fyear)
    extract_birth_factors_data(spark, save_path, fyear)
    extract_demographic_factors_data(spark, save_path, fyear)


if __name__ == "__main__":
    main(*sys.argv[1:])
