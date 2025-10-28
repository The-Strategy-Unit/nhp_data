"""Helper methods/tables"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.table_names import table_names


def inputs_age_group(spark: SparkSession) -> DataFrame:
    """Get age groupings

    :param spark: The spark context to use
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

    :param spark: The spark context to use
    :type spark: SparkSession
    :return: Treatment Function Grouping Table
    :rtype: DataFrame
    """
    return spark.read.table(table_names.reference_tretspef_grouping)


def complete_age_sex_data(
    spark: SparkSession, df: DataFrame, complete_type: str = "full"
) -> DataFrame:
    """Complete Age/Sex data

    Make sure all providers/strategies have complete age group/sex rows. If the rows are missing,
    then add a "0" row.

    :param spark: The spark context to use
    :type spark: SparkSession
    :param df: The DataFrame to complete
    :type df: DataFrame
    :param complete_type: which method to use, one of "full", "age_range", or "simple", defaults to
        "full"
    :type complete_type: str, optional
    :raises ValueError: If an invalid complete_type is provided
    :return: the completed DataFrame
    :rtype: DataFrame
    """
    age_groups = inputs_age_group(spark).select("age_group").distinct()

    # TODO: probably should decide on one of these methods (ether full or age_range) and remove
    # the other cases
    match complete_type:
        case "full":
            # full cross join of age groups/sex for every strategy
            sexes = spark.createDataFrame([("1",), ("2",)], ["sex"])
            a = (
                df.select("fyear", "strategy")
                .distinct()
                .crossJoin(age_groups)
                .crossJoin(sexes)
            )
        case "age_range":
            # cross join for the age range for a strategy (regardless of sex)
            # but only keeping in sexes where rows appear
            sexes = df.select("strategy", "sex").distinct()
            a = (
                df.groupBy("fyear", "strategy")
                .agg(
                    F.min("age_group").alias("min"),
                    F.max("age_group").alias("max"),
                )
                .join(
                    age_groups, [F.col("age_group").between(F.col("min"), F.col("max"))]
                )
                .join(sexes, ["strategy"])
            )
        case "simple":
            # simplest method: only include cases where age_group/sex appear for a strategy in a
            # year
            a = df.select("fyear", "age_group", "sex", "strategy").distinct()
        case _:
            raise ValueError("incorrect complete_type")

    b = df.select("strategy", "provider").distinct()

    return (
        a.join(b, "strategy", "inner")
        .join(
            df,
            ["fyear", "age_group", "sex", "strategy", "provider"],
            "left",
        )
        .fillna(0, ["n"])
    )
