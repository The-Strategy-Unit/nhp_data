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
