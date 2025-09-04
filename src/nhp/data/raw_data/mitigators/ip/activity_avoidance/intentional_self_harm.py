"""Intentional Self Harm Related Admissions (IP-AA-019)

Emergency admissions to hospital resulting from acts of self harm may be preventable, if appropriate
support is available in the community or accessible through primary care.

The model identifies admissions with a cause code relating to intentional self harm/poisoning.

### Source of Intentional Self Harm Related Admissions codes

The [ICD10 diagnosis coding framework][ish_1] includes a range of explicit self harm codes.

[ish_1]: https://icd.who.int/browse10/2019/en#/X60-X84
"""

from pyspark.sql import functions as F

from nhp.data.hes_datasets import any_diagnosis, nhp_apc
from nhp.data.raw_data.mitigators import activity_avoidance_mitigator


@activity_avoidance_mitigator()
def _intentional_self_harm():
    return (
        nhp_apc.admission_has(any_diagnosis, "X([67]|8[0-4])", "Y870")
        .filter(F.col("admimeth").rlike("^2"))
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
