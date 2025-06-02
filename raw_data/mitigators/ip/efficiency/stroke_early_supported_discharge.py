"""Stroke Early Supported Discharge (IP-EF-025)

Early supported discharge is an intervention for adults after a stroke that allows their care to be
transferred from an inpatient environment to a community setting.

It enables people to continue their rehabilitation therapy at home, with the same intensity and
expertise that they would receive in hospital.

The model identifies patients who have a stroke related HRG code who may benefit from a reduced
inpatient LOS through supported discharge.
"""

from pyspark.sql import functions as F

from hes_datasets import nhp_apc
from raw_data.mitigators import efficiency_mitigator


@efficiency_mitigator()
def _stroke_early_supported_discharge():
    return (
        nhp_apc.filter(F.col("admimeth").rlike("^2"))
        .filter(F.col("dismeth") != "4")
        .filter(F.col("sushrg").rlike("^AA(2[23]|35)"))
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
