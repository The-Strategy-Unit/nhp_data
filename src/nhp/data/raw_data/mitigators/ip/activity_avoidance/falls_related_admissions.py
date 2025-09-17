# pylint: disable=line-too-long
"""Falls Related Admissions (IP-AA-016)

Some falls that result in an emergency admission to hospital are potentially avoidable if the
appropriate preventative services are in place.
A range of evidence based services and interventions exist that can reduce falls in the elderly.
Implementation across the country is variable and as such there remains significant scope to reduce
the incidence of falls further.
Examples of preventative services include home risk assessments for trip hazards, strength and
balance training for those at risk and falls telemonitoring.

Falls related admissions are identified in the model in a number of ways:

- Spells where there is a falls related ICD10 cause code and a traumatic injury (explicit),
- Spells where the primary diagnosis is `R296` (Tendency to fall)
- Spells where there is no external cause ICD10 code but there is a diagnosis of one of a number of
  fractures that are commonly associated with a fall

Within the model the three sets of admissions are grouped, in this case, into a single sub factor.

## Source of Falls Related Admissions codes

The codes for explicit falls are taken from the [PHE outcome indicator framework][fra_1].

[fra_1]: https://fingertips.phe.org.uk/profile/public-health-outcomes-framework/data#page/6/gid/1000042/pat/6/par/E12000004/ati/102/are/E06000015/iid/22401/age/27/sex/4
"""
# pylint: enable=line-too-long

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nhp.data.hes_datasets import (
    any_diagnosis,
    nhp_apc,
    primary_diagnosis,
    secondary_diagnosis,
)
from nhp.data.raw_data.mitigators import activity_avoidance_mitigator


def _explicit_fractures():
    return nhp_apc.admission_has(
        primary_diagnosis, "[ST]"
    ).admission_has(  # ty: ignore[call-non-callable]
        secondary_diagnosis, "W[01]"
    )


def _implicit_fractures():
    return nhp_apc.admission_has(
        any_diagnosis,
        "M(48[45]|80[^67])",
        "S(22[01]|32[0-47]|42[234]|52|620|72[0-48])",
        "T08X",
    ).admission_not(  # ty: ignore[call-non-callable]
        any_diagnosis, "[V-Y]"
    )


def _implicit_tendency_to_fall():
    return nhp_apc.admission_has(
        primary_diagnosis, "R296"
    )  # ty: ignore[call-non-callable]


@activity_avoidance_mitigator()
def _falls_related_admissions():
    dfs = [_explicit_fractures(), _implicit_fractures(), _implicit_tendency_to_fall()]
    return (
        reduce(DataFrame.unionByName, dfs)
        .filter(F.col("admimeth").rlike("^2"))
        .filter(F.col("age") >= 65)
        .select("fyear", "provider", "epikey")
        .distinct()
        .withColumn("sample_rate", F.lit(1.0))
    )
