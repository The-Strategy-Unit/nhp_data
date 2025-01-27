"""Helper methods/tables"""

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame, Window


def age_group(spark: SparkContext) -> DataFrame:
    """Get age groupings

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: Age Grouping Table
    :rtype: DataFrame
    """

    def _get_age_str(age):
        age = (age // 5) * 5
        return f"{str(age).rjust(2, '0')}-{str(age + 4).rjust(2, '0')}"

    return spark.createDataFrame(
        [(age, _get_age_str(age)) for age in range(0, 90)] + [(90, "90+")],
        ["age", "age_group"],
    )


def create_tretspef_grouping(spark: SparkContext) -> None:
    """Create Treatment Functrion Groupings

    :param spark: The spark context to use
    :type spark: SparkContext
    """
    df = spark.read.table("apc").select("tretspef").distinct().orderBy("tretspef")

    rtt_specialties = [
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
        "502",
    ]

    tretspef = F.col("tretspef")

    (
        df.withColumn(
            "tretspef_grouped",
            F.when(tretspef.isin(rtt_specialties), F.col("tretspef"))
            .when(tretspef.rlike("^1(?!80|9[02])"), "Other (Surgical)")
            .when(
                tretspef.rlike("^(1(80|9[02])|[2346]|5(?!60)|83[134])"),
                "Other (Medical)",
            )
            .otherwise("Other"),
        )
        .write.mode("overwrite")
        .saveAsTable("tretspef_grouping")
    )


def treatment_function_grouping(spark: SparkContext) -> DataFrame:
    """Get Treatment Function Groupings

    :param spark: The spark context to use
    :type spark: SparkContext
    :return: Treatment Function Grouping Table
    :rtype: DataFrame
    """
    return spark.read.table("tretspef_grouping")
