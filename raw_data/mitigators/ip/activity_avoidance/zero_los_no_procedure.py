"""Admission with no overnight stay and no procedure

Emergency admissions which are discharged on the same day of admission and where no procedure is
undertaken could be are avoidable if the appropriate preventative services are in place. The model
identifies admissions of this type and categorises them into adult and child.

## Available breakdowns

- Admission with no overnight stay and no procedure (adults) (IP-AA-032)
- Admission with no overnight stay and no procedure (children) (IP-AA-033)

"""

from pyspark.sql import functions as F

from hes_datasets import nhp_apc, procedures
from raw_data.mitigators import activity_avoidance_mitigator


def _zero_los(age_filter):
    return (
        nhp_apc.filter(F.col("admimeth").rlike("^2"))
        .join(procedures, ["epikey", "fyear"], "anti")
        .filter(F.col("speldur") == 0)
        .filter(F.col("dismeth").isin(["1", "2", "3"]))
        .filter(age_filter)
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@activity_avoidance_mitigator()
def _zero_los_no_procedure_adult():
    return _zero_los(F.col("age") >= 18)


@activity_avoidance_mitigator()
def _zero_los_no_procedure_child():
    return _zero_los(F.col("age") < 18)
