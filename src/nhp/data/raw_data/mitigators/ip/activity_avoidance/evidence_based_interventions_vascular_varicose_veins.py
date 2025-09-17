"""Evidence-Based Interventions: Vascular (IP-AA-015)

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

[ebi_1]: https://ebi.aomrc.org.uk/specialty/cardiovascular-disease/
"""

from nhp.data.hes_datasets import nhp_apc, primary_diagnosis, primary_procedure
from nhp.data.raw_data.mitigators import activity_avoidance_mitigator
from nhp.data.raw_data.mitigators.ip.activity_avoidance.evidence_based_interventions import (
    evidence_based_interventions,
)


def _varicose_vein():
    # pylint: disable=line-too-long
    """Varicose vein interventions

    source: https://ebi.aomrc.org.uk/interventions/varicose-vein-interventions/
    """
    # pylint: enable=line-too-long

    return nhp_apc.admission_has(
        primary_procedure, "L8(4[1-689]|[568][12389]|7[1-9])"
    ).admission_has(  # ty: ignore[call-non-callable]
        primary_diagnosis, "I83", "O(220|878)", "Q278"
    )


@activity_avoidance_mitigator()
def _evidence_based_interventions_vascular_varicose_veins():
    return evidence_based_interventions(
        _varicose_vein,
    )
