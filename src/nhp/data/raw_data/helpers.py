"""Helper methods/tables"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.table_names import table_names


def add_tretspef_grouped_column(self: DataFrame) -> DataFrame:
    """Add tretspef grouped column to DataFrame

    :param self: The data frame to add the tretpsef grouped column to
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
        "502",
    ]

    tretspef_column = (
        F.when(F.col("tretspef").isin(specialties), F.col("tretspef"))
        .when(F.expr("tretspef RLIKE '^1(?!80|9[02])'"), F.lit("Other (Surgical)"))
        .when(
            F.expr("tretspef RLIKE '^(1(80|9[02])|[2346]|5(?!60)|83[134])'"),
            F.lit("Other (Medical)"),
        )
        .otherwise(F.lit("Other"))
    )

    return self.withColumn("tretspef_grouped", tretspef_column)


def add_age_group_column(self: DataFrame) -> DataFrame:
    return self.withColumn(
        "age_group",
        F.when(F.isnull("age"), "Unknown")
        .when(F.col("age") == 0, "0")
        .when(F.col("age") <= 4, "1-4")
        .when(F.col("age") <= 9, "5-9")
        .when(F.col("age") <= 15, "10-15")
        .when(F.col("age") <= 17, "16-17")
        .when(F.col("age") <= 34, "18-34")
        .when(F.col("age") <= 49, "35-49")
        .when(F.col("age") <= 64, "50-64")
        .when(F.col("age") <= 74, "65-74")
        .when(F.col("age") <= 84, "75-84")
        .otherwise("85+"),
    )


def remove_mental_health_providers(
    spark: SparkSession, df: DataFrame, provider_col: str = "provider"
) -> DataFrame:
    """Filter out mental health providers from DataFrame

    :param spark: The Spark session
    :type spark: SparkSession
    :param df: The data frame to filter
    :type df: DataFrame
    :param provider_col: The column name containing provider codes
    :type provider_col: str
    :return: The data frame with mental health providers filtered out
    :rtype: DataFrame
    """

    mental_health_providers = (
        spark.read.table(table_names.reference_ods_trusts)
        .filter(F.col("org_type") == "MENTAL HEALTH AND LEARNING DISABILITY")
        .select(F.col("org_to").alias(provider_col))
        .distinct()
    )

    return df.join(mental_health_providers, provider_col, "anti")
