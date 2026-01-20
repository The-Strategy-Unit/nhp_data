"""Helper methods/tables"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.table_names import table_names


def inputs_age_group(spark: SparkSession) -> DataFrame:
    """Get age groupings

    :param spark: The spark session to use
    :type spark: SparkSession
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


def treatment_function_grouping(spark: SparkSession) -> DataFrame:
    """Get Treatment Function Groupings

    :param spark: The spark session to use
    :type spark: SparkSession
    :return: Treatment Function Grouping Table
    :rtype: DataFrame
    """
    return spark.read.table(table_names.reference_tretspef_grouping)


def complete_age_sex_rows(
    spark: SparkSession, df: DataFrame, geography_column: str
) -> DataFrame:
    """Complete missing rows in the DataFrame by creating all combinations of dimensions.

    This function ensures the DataFrame has a complete set of rows for all valid combinations
    of fyear, type, strategy, geography, sex, and age. It generates age ranges (0-90) based on
    the minimum and maximum ages present for each type/strategy/sex combination.

    :param spark: The Spark session to use for creating DataFrames
        column. Missing combinations will be added with values filled as 0.
    :param geography_column: The name of the column containing geography information
    :type geography_column: str

    :return: The completed DataFrame with all combinations of the specified dimensions,
        with missing values filled as 0
    """
    ages = spark.createDataFrame([(i,) for i in range(0, 91)], schema=["age"])

    age_range = (
        df.groupBy("type", "strategy", "sex")
        .agg(F.min("age").alias("min_age"), F.max("age").alias("max_age"))
        .join(
            ages,
            (F.col("age") >= F.col("min_age")) & (F.col("age") <= F.col("max_age")),
        )
        .drop("min_age", "max_age")
    )

    geographies = df.select("fyear", geography_column).distinct()

    all_rows = (
        df.select("fyear", "type", "strategy")
        .distinct()
        .join(age_range, ["type", "strategy"], "cross")
        .join(geographies, ["fyear"], "inner")
    )

    return df.join(
        all_rows,
        ["fyear", "type", "strategy", geography_column, "sex", "age"],
        "right",
    ).fillna(0)
