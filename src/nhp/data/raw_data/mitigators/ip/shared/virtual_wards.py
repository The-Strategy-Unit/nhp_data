"""Virtual Wards

Virtual wards allow patients to receive the care they need at home, safely and conveniently rather
than in hospital. They also provide systems with a significant opportunity to narrow the gap between
demand and capacity for secondary care beds, by providing an alternative to admission and/or early
discharge.

Whilst virtual wards may be beneficial for patients on a variety of clinical pathways, guidance has
been produced relating to three pathways which represent the majority of patients who may be
clinically suitable to benefit from a virtual ward. These pathways are Frailty, Acute Respiratory
Infections (ARI) and Heart failure.
"""

from pyspark.sql import functions as F

from nhp.data.hes_datasets import nhp_apc, primary_diagnosis, procedures


def _virtual_wards_admissions(*args):
    return (
        nhp_apc.filter(F.col("admimeth").rlike("^2"))
        .filter(F.col("dismeth").isin(["1", "2", "3"]))
        .filter(F.col("age") >= 18)
        .admission_has(primary_diagnosis, *args)  # ty: ignore[call-non-callable]
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


# define these methods so they can be used for both activity avoidance and efficiencies


def ari():
    """Virtual Wards: Acute Respiratory Infection (ARI)"""
    return _virtual_wards_admissions("B(3[34]|97)", "J(0[6-9]|[1-9])", "U0[467]").join(
        procedures, ["epikey"], "anti"
    )


def heart_failure():
    """Virtual Wards: Heart Failure"""
    return _virtual_wards_admissions("I(110|255|42[09]|50[019])")
