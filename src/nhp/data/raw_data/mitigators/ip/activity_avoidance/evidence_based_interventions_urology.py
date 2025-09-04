"""Evidence-Based Interventions: Urology (IP-AA-014)

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

[ebi_1]: https://ebi.aomrc.org.uk/specialty/kidney-disease-urology/
"""

from pyspark.sql import functions as F

from nhp.data.hes_datasets import (
    any_diagnosis,
    nhp_apc,
    primary_diagnosis,
    primary_procedure,
    secondary_procedure,
)
from nhp.data.raw_data.mitigators import activity_avoidance_mitigator
from nhp.data.raw_data.mitigators.ip.activity_avoidance.evidence_based_interventions import (
    evidence_based_interventions,
)


def _cystocopy():
    # pylint: disable=line-too-long
    """Cystoscopy for uncomplicated lower urinary tract symptoms

    source: https://ebi.aomrc.org.uk/interventions/cystoscopy-for-uncomplicated-lower-urinary-tract-symptoms/
    """
    # pylint: enable=line-too-long

    return (
        nhp_apc.admission_has(primary_procedure, "M45[589]")
        .admission_not(secondary_procedure, "M45[1-4]")
        .admission_has(any_diagnosis, "F171", "N390", "R3(1|98)")
        .filter(F.col("sex") == "1")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


def _prostatic_hyperplasia():
    # pylint: disable=line-too-long
    """Surgical intervention for benign prostatic hyperplasia (BPH)

    source: https://ebi.aomrc.org.uk/interventions/surgical-intervention-for-benign-prostatic-hyperplasia/
    """
    # pylint: enable=line-too-long

    return (
        nhp_apc.admission_has(
            primary_procedure, "M(6(1[123489]|41|5[1-689]|6[12]|8[13])|7(04|1[189]))"
        )
        .admission_has(primary_diagnosis, "N40")
        .admission_not(any_diagnosis, "N1([39]|7[01289]|8[1-59])")
        .filter(F.col("sex") == "1")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


@activity_avoidance_mitigator()
def _evidence_based_interventions_urology():
    return evidence_based_interventions(
        _cystocopy,
        _prostatic_hyperplasia,
    )
