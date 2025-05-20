"""Evidence-Based Interventions: General Surgery (IP-AA-011)

The Evidence-based Interventions programme is an initiative led by the Academy of Medical Royal
Colleges to improve the quality of care. It is designed to reduce the number of medical or surgical
interventions as well as some other tests and treatments which the evidence tells us are
inappropriate for some patients in some circumstances.

The programme identified a range of tests, treatments and procedures that could in some instances be
unnecessary and as such avoidable through for example improved policies, pathway redesign, or
referral management.

The model adapts SQL code developed by the programme to identify spells where relevant surgical
procedures (along with specified diagnoses) are undertaken. It groups those procedures into surgical
specialties.

## Source of Evidence-Based Interventions codes

Codes used within the model to identify spells of this type can be found on the
[AOMRC website][ebi_1].

[ebi_1]: https://ebi.aomrc.org.uk/specialty/gastrointestinal-disease/
"""

from pyspark.sql import functions as F

from hes_datasets import any_diagnosis, nhp_apc, primary_diagnosis, primary_procedure
from raw_data.mitigators import activity_avoidance_mitigator
from raw_data.mitigators.ip.activity_avoidance.evidence_based_interventions import (
    evidence_based_interventions,
)


def _appendectomy():
    # pylint: disable=line-too-long
    """Appendicectomy without confirmation of appendicitis

    source: https://ebi.aomrc.org.uk/interventions/appendicectomy-without-confirmation-of-appendicitis/
    """
    # pylint: enable=line-too-long

    return nhp_apc.admission_has(primary_procedure, "H01[12389]").admission_not(
        any_diagnosis, "K3(5[238]|67)"
    )


def _cholecystectomy():
    # pylint: disable=line-too-long
    """Cholecystectomy

    source https://ebi.aomrc.org.uk/interventions/cholecystectomy/
    """
    # pylint: enable=line-too-long
    return (
        nhp_apc.admission_has(primary_procedure, "J18[1234589]")
        .admission_has(primary_diagnosis, "K851")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


def _haemorrhoid():
    # pylint: disable=line-too-long
    """Haemorrhoid surgery

    source: https://ebi.aomrc.org.uk/interventions/haemorrhoid-surgery/
    """
    # pylint: enable=line-too-long
    return nhp_apc.admission_has(primary_procedure, "H51(12389)").admission_has(
        any_diagnosis, "K64", "O(224|872)"
    )


def _hernia():
    # pylint: disable=line-too-long
    """Repair of minimally symptomatic inguinal hernia

    source: https://ebi.aomrc.org.uk/interventions/repair-of-minimally-symptomatic-inguinal-hernia/
    """
    # pylint: enable=line-too-long
    return (
        nhp_apc.admission_has(primary_procedure, "T20")
        .admission_has(primary_diagnosis, "K40[29]")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


@activity_avoidance_mitigator()
def _evidence_based_interventions_general_surgery():
    return evidence_based_interventions(
        _appendectomy, _cholecystectomy, _haemorrhoid, _hernia
    )
