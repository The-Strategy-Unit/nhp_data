"""Pre-Op Length of Stay

In most cases patients do not need to be admitted before the day of surgery. This model identifies
elective admissions that are admitted either 1 day prior to surgery, or 2 days prior to surgery.
Patients who are admitted more than 2 days prior to surgery are not included as it is assumed that
in these cases there is a valid clinical reason for the extended pre-op LOS.

# Available breakdowns

- Pre-op Length of Stay of 1 day (IP-EF-022)
- Pre-op Length of Stay of 2 days (IP-EF-023)
"""

from pyspark.sql import functions as F

from nhp.data.nhp_datasets.apc import apc_primary_procedures
from nhp.data.raw_data.mitigators import efficiency_mitigator
from nhp.data.raw_data.mitigators.ip.hes_datasets import nhp_apc


def _preop_los(days):
    return (
        nhp_apc.filter(F.col("admimeth").startswith("1"))  # ty: ignore[missing-argument, invalid-argument-type]
        .join(apc_primary_procedures.drop("fyear"), ["epikey"])
        .filter(F.col("admidate") <= F.col("date"))
        .filter(F.col("date") <= F.col("disdate"))
        .filter(F.date_diff(F.col("date"), F.col("admidate")) == days)
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@efficiency_mitigator("pre-op_los_1-day")
def _preop_los_1_day():
    return _preop_los(1)


@efficiency_mitigator("pre-op_los_2-day")
def _preop_los_2_day():
    return _preop_los(2)
