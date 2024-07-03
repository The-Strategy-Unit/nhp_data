"""Virtual Wards Admission Avoidance

Virtual wards allow patients to receive the care they need at home, safely and conveniently rather
than in hospital. They also provide systems with a significant opportunity to narrow the gap between
demand and capacity for secondary care beds, by providing an alternative to admission and/or early
discharge. 

Whilst virtual wards may be beneficial for patients on a variety of clinical pathways, guidance has
been produced relating to three pathways which represent the majority of patients who may be
clinically suitable to benefit from a virtual ward. These pathways are Frailty, Acute Respiratory
Infections (ARI) and Heart failure. 

This activity avoidance mitigator identifies patients who may be suitable for admission to an ARI or
Heart Failure virtual ward. 

### Available breakdowns

- Acute Respiratory Infection (IP-AA-030)
- Heart Failure (IP-AA-031)
"""

from pyspark.sql import functions as F

from hes_datasets import nhp_apc, primary_diagnosis, procedures
from mitigators import activity_avoidance_mitigator


def _virtual_wards_admissions(*args):
    return (
        nhp_apc.filter(F.col("admimeth").rlike("^2"))
        .filter(F.col("dismeth").isin(["1", "2", "3"]))
        .join(procedures, ["epikey", "fyear"], "anti")
        .admission_has(primary_diagnosis, *args)
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


# define these methods so they can be used for both activity avoidance and efficiencies


def virtual_wards_ari():
    """Virtual Wards: Acute Respiratory Infection (ARI)"""
    return _virtual_wards_admissions("B(3[34]|97)", "J(?!0[^69])", "U0[467]")


def virtual_wards_heart_failure():
    """Virtual Wards: Heart Failure"""
    return _virtual_wards_admissions("I(110|255|42[09]|50[019])")


@activity_avoidance_mitigator()
def _virtual_wards_activity_avoidance_ari():
    return virtual_wards_ari()


@activity_avoidance_mitigator()
def _virtual_wards_activity_avoidance_heart_failure():
    return virtual_wards_heart_failure()
