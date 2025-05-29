"""Mental health admissions via Emergency Department (IP-AA-027)

Patients with mental health conditions are sometimes admitted to an inpatient bed after attending an
emergency department.
The reasons for admission can be for a range of reasons including self harm, acute psychosis, or
drug and alcohol related issues.
These patients may benefit more from appropriate treatment in the community or an alternative
psychiatric facility.
Provision of a psychiatric liaison type service (sometimes known as RAID) could avoid admissions of
this type.

This model identifies patients who may benefit from psychiatric liaison as those with a primary
diagnosis in ICD10 chapter F (mental and behavioural disorders) who were admitted from ED and did
not undergo a procedure.
"""

from pyspark.sql import functions as F

from hes_datasets import nhp_apc, primary_diagnosis, procedures
from raw_data.mitigators import activity_avoidance_mitigator


@activity_avoidance_mitigator()
def _raid_ae():
    return (
        nhp_apc.admission_has(primary_diagnosis, "F")
        .filter(F.col("admimeth") == "21")
        .filter(F.col("dismeth") != "4")
        .join(procedures, ["epikey", "fyear"], "anti")
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
