"""Admissions with mental health comorbidities (IP-EF-024)

Patients with mental health problems admitted to hospital in an emergency can have longer lengths of
stay (LOS) due to added complexities this creates in treating and supporting such patients.

Psychiatric liaison services (sometimes referred to as RAID) can help to reduce the LOS for such
patients by providing support to ward staff whilst in hospital, and facilitating timely discharge
through the provision of appropriate post discharge support.

The model identifies patients who may benefit as those with a recorded mental or behavioural
diagnosis.
"""

from pyspark.sql import functions as F

from hes_datasets import any_diagnosis, nhp_apc
from raw_data.mitigators import efficiency_mitigator


@efficiency_mitigator()
def _raid_ip():
    return (
        nhp_apc.admission_has(any_diagnosis, "F")
        .filter(F.col("admimeth").rlike("^2"))
        .filter(F.col("dismeth") != "4")
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
