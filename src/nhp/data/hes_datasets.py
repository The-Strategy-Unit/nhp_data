"""HES Datasets

Provides variables for connecting to HES datasets, and methods for working with these datasets.

- `hes_apc`: the main admitted patient care dataset
- `diagnoses`: the diagnoses for the `hes_apc` dataset
- `procedures`: the procedures for the `hes_apc` dataset
- `nhp_apc`: the view of `hes_apc` used for the NHP model
"""

from typing import Callable

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

spark = DatabricksSession.builder.getOrCreate()


hes_apc = spark.read.table("hes.silver.apc")
diagnoses = spark.read.table("hes.silver.apc_diagnoses")
procedures = spark.read.table("hes.silver.apc_procedures")

nhp_apc = spark.read.table("nhp.raw_data.apc")


def combine_into_regex(*args) -> str:
    """Combine into a single regex

    Takes a variable amount of arguments and combines into a single regex.
    Each argument is concatenated together with a |

    .. code-block:: python
      combine_into_regex("a", "b", "c(1|2)")
      # "^(a|b|c(1|2))"

    :param *args: strings to combine into a single regex
    :type *args: string
    :return: A single regex pattern
    :rtype: string
    """
    return f"^({'|'.join(args)})"


def primary_diagnosis(codes: str) -> DataFrame:
    """Filter for primary diagnosis

    :param codes: a regex for the diagnosis codes to search for
    :type codes: string
    :return: Filtered diagnosis DataFrame
    :rtype: DataFrame
    """
    return any_diagnosis(codes).filter(F.col("diag_order") == 1)


def secondary_diagnosis(codes: str) -> DataFrame:
    """Filter for secondary diagnosis

    :param codes: a regex for the diagnosis codes to search for
    :type codes: string
    :return: Filtered diagnosis DataFrame
    :rtype: DataFrame
    """
    return any_diagnosis(codes).filter(F.col("diag_order") > 1)


def any_diagnosis(codes: str) -> DataFrame:
    """Filter for a diagnosis

    :param codes: a regex for the diagnosis codes to search for
    :type codes: string
    :return: Filtered diagnosis DataFrame
    :rtype: DataFrame
    """
    return diagnoses.filter(F.col("diagnosis").rlike(codes))


def primary_procedure(codes: str) -> DataFrame:
    """Filter for primary procedure

    :param codes: a regex for the procedure codes to search for
    :type codes: string
    :return: Filtered procedures DataFrame
    :rtype: DataFrame
    """
    return any_procedure(codes).filter(F.col("procedure_order") == 1)


def secondary_procedure(codes: str) -> DataFrame:
    """Filter for secondary procedure

    :param codes: a regex for the procedure codes to search for
    :type codes: string
    :return: Filtered procedures DataFrame
    :rtype: DataFrame
    """
    return any_procedure(codes).filter(F.col("procedure_order") > 1)


def any_procedure(codes: str) -> DataFrame:
    """Filter for a procedure

    :param codes: a regex for the procedure codes to search for
    :type codes: string
    :return: Filtered procedures DataFrame
    :rtype: DataFrame
    """
    return procedures.filter(F.col("procedure_code").rlike(codes))


def _admission_has(
    df: DataFrame, filter_function: Callable[[str], DataFrame], *args: str
) -> DataFrame:
    """Filter admissions where they have...

    :param df: the data we want to filter
    :type df: DataFrame
    :param filter_function: the function to filter the df by
    :type filter_function: Callable[[str], DataFrame]
    :param *args: a list of codes which will be combined into a single regex
    :type *args: str
    :return: Filtered DataFrame
    :rtype: DataFrame
    """
    regex = combine_into_regex(*args)
    return df.join(filter_function(regex), ["epikey", "fyear"], "semi")


def _admission_not(
    df: DataFrame, filter_function: Callable[[str], DataFrame], *args: str
) -> DataFrame:
    """Filter admissions where they don't have...

    :param df: the data we want to filter
    :type df: DataFrame
    :param filter_function: the function to filter the df by
    :type filter_function: Callable[[str], DataFrame]
    :param *args: a list of codes which will be combined into a single regex
    :type *args: str
    :return: Filtered DataFrame
    :rtype: DataFrame
    """
    regex = combine_into_regex(*args)
    return df.join(filter_function(regex), ["epikey", "fyear"], "anti")


DataFrame.admission_has = _admission_has
DataFrame.admission_not = _admission_not
