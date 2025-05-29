"""Intentional Self Harm Related Admissions (IP-AA-019)

Emergency admissions to hospital resulting from acts of self harm may be preventable, if appropriate
support is available in the community or accessible through primary care.

The model identifies admissions with a cause code relating to intentional self harm/poisoning.  

### Source of Intentional Self Harm Related Admissions codes

The [ICD10 diagnosis coding framework][ish_1] includes a range of explicit self harm codes.

[ish_1]: https://icd.who.int/browse10/2019/en#/X60-X84
"""

from pyspark.sql import functions as F

from hes_datasets import nhp_apc, primary_diagnosis
from raw_data.mitigators import activity_avoidance_mitigator


@activity_avoidance_mitigator()
def _medically_unexplained_related_admissions():
    return (
        nhp_apc.admission_has(
            primary_diagnosis,
            "F510",
            "G(4(4[028]|70)|501)",
            "H931",
            "K5(80|9[01])",
            "M545",
            "R(0(02|7[14])|12X|251|42X|5[13]X)",
        )
        .filter(F.col("admimeth").rlike("^2"))
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
