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

from hes_datasets import nhp_apc, procedures
from mitigators import efficiency_mitigator


def _preop_los(days):
    return (
        nhp_apc.filter(F.col("admimeth").startswith("1"))
        .join(procedures.filter(F.col("procedure_order") == 1), ["epikey", "fyear"])
        .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23]|)"))
        .filter(~F.col("procedure_code").rlike("^X[6-9]"))
        .filter(~F.col("procedure_code").rlike("^[UYZ]"))
        .filter(F.col("admidate") <= F.col("date"))
        .filter(F.col("date") <= F.col("disdate"))
        .filter(F.date_diff(F.col("date"), F.col("admidate")) == days)
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@efficiency_mitigator("pre-op_los_1-day")
def _preop_los_1_day():
    return _preop_los(1)


@efficiency_mitigator("pre-op_los_2-day")
def _preop_los_2_day():
    return _preop_los(2)
