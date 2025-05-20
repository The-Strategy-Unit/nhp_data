"""Evidence-Based Interventions: ENT (IP-AA-010)

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

[ebi_1]: https://ebi.aomrc.org.uk/specialty/ears-nose-and-throat/
"""

from pyspark.sql import functions as F

from hes_datasets import (
    any_diagnosis,
    any_procedure,
    nhp_apc,
    primary_diagnosis,
    primary_procedure,
    secondary_diagnosis,
)
from raw_data.mitigators import activity_avoidance_mitigator
from raw_data.mitigators.ip.activity_avoidance.evidence_based_interventions import (
    evidence_based_interventions,
)


def _adenoids():
    # pylint: disable=line-too-long
    """Removal of adenoids for treatment of glue ear

    source: https://ebi.aomrc.org.uk/interventions/removal-of-adenoids-for-treatment-of-glue-ear/
    """
    # pylint: enable=line-too-long

    return (
        nhp_apc.admission_has(any_procedure, "E20[1489]")
        .admission_has(any_procedure, "D151")
        .admission_has(primary_diagnosis, "H65[2349]")
        .admission_not(
            secondary_diagnosis, "G473", "J3(2[0123489]|52)", "Q3(5[13579]|7[01234589])"
        )
        .filter(
            (F.col("age") <= 18) | ((F.col("age") >= 7001) & (F.col("age") <= 7007))
        )
    )


def _grommets():
    # pylint: disable=line-too-long
    """Grommets for glue ear in children

    source: https://ebi.aomrc.org.uk/interventions/grommets-for-glue-ear-in-children/
    """
    # pylint: enable=line-too-long
    return (
        nhp_apc.admission_has(primary_procedure, "D151")
        .admission_has(any_procedure, "D151")
        .admission_has(primary_diagnosis, "H6(5[^01]|6)")
        .filter(
            (F.col("age") <= 18) | ((F.col("age") >= 7001) & (F.col("age") <= 7007))
        )
    )


def _snoring():
    # pylint: disable=line-too-long
    """Snoring surgery (in the absence of obstructive sleep apnoea)

    source: https://ebi.aomrc.org.uk/interventions/snoring-surgery-in-the-absence-of-obstructive-sleep-apnoea/
    """
    # pylint: enable=line-too-long
    return (
        nhp_apc.admission_has(primary_procedure, "F32[456]")
        .admission_has(primary_diagnosis, "R065")
        .admission_not(secondary_diagnosis, "G473")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


def _rhinosinusitis():
    """Surgical intervention for chronic rhinosinusitis

    source: https://ebi.aomrc.org.uk/interventions/surgical-intervention-for-chronic-rhinosinusitis/
    """
    return (
        nhp_apc.admission_has(
            any_procedure, "E(081|1([257][123489]|[34][1-9]|4[1-9]|6[1289])|641)"
        )
        .admission_has(any_procedure, "Y76[12]")
        .admission_has(primary_diagnosis, "J3(10|3[23])")
    )


def _tonsillectomy():
    """Tonsillectomy for recurrent tonsillitis

    source: https://ebi.aomrc.org.uk/interventions/tonsillectomy-for-recurrent-tonsillitis/
    """
    return (
        nhp_apc.admission_has(any_procedure, "F3(4[1-9]|61)")
        .admission_has(any_diagnosis, "J(03|350)")
        .admission_not(any_diagnosis, "G473", "J3(6|90)")
    )


@activity_avoidance_mitigator()
def _evidence_based_interventions_ent():
    return evidence_based_interventions(
        _adenoids,
        _grommets,
        _snoring,
        _rhinosinusitis,
        _tonsillectomy,
    )
