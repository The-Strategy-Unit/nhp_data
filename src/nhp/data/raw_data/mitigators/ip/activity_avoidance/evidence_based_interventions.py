"""Helper methods for Evidence Based Interventions Mitigators"""

from functools import reduce
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nhp.data.hes_datasets import any_diagnosis


def evidence_based_interventions(*args: Callable[[], DataFrame]) -> DataFrame:
    """Evidence Based Interventions Helper

    :param *args: a list of functions to return subsets of activity
    :type *args: Callable[[], DataFrame]
    :return: a filtered version of the nhp_apc dataset for the mitigator
    :rtype: DataFrame
    """
    return (
        reduce(DataFrame.unionByName, [f() for f in args])
        .filter(~F.col("admimeth").startswith("2"))
        # .filter(F.col("admincat") != "02")
        .admission_not(any_diagnosis, "C", "D(0|3[789]|4[0-8])")  # ty: ignore[call-non-callable]
        .select("fyear", "provider", "epikey")
        .distinct()
        .withColumn("sample_rate", F.lit(1.0))
    )
