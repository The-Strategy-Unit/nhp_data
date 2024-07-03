"""End of Life Care Related Admissions

The evidence is clear that most people would prefer to die at home but despite some improvements in
recent years most deaths still occur in hospital.

Enabling people to die at home can reduce the burden on hospital resources and on commissioners'
budgets. The model identifies admissions where alternative support in an alternative setting may
have been possible. These spells are those where no procedure is carried out and there is no
indication that the patient experienced any trauma and the patient dies in hospital.

## Available Breakdowns

1. Patients who die within 0-2 days of admission (IP-AA-008)
2. Patients who die within 3-14 days of admission (IP-AA-009)

"""

from pyspark.sql import functions as F

from hes_datasets import any_diagnosis, nhp_apc, procedures
from mitigators import activity_avoidance_mitigator


# base function for the two mitigators
def _eol_care(min_speldur, max_speldur):
    return (
        nhp_apc.admission_not(any_diagnosis, "[V-Y]")
        .join(procedures, ["epikey", "fyear"], "anti")
        .filter(F.col("dismeth") == 4)
        .filter(F.col("speldur") >= min_speldur)
        .filter(F.col("speldur") <= max_speldur)
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@activity_avoidance_mitigator()
def _eol_care_2_days():
    return _eol_care(0, 2)


@activity_avoidance_mitigator()
def _eol_care_3_to_14_days():
    return _eol_care(3, 14)
