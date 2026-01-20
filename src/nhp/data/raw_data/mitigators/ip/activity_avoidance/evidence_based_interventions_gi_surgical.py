"""Evidence-Based Interventions: GI surgery (IP-AA-012)

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

from nhp.data.raw_data.mitigators import activity_avoidance_mitigator
from nhp.data.raw_data.mitigators.ip.activity_avoidance.evidence_based_interventions import (
    evidence_based_interventions,
)
from nhp.data.raw_data.mitigators.ip.hes_datasets import (
    any_diagnosis,
    any_procedure,
    nhp_apc,
    primary_procedure,
)


def _colonoscopy():
    # pylint: disable=line-too-long
    """Appropriate colonoscopy in the management of hereditary colorectal cancer

    source: https://ebi.aomrc.org.uk/interventions/appropriate-colonoscopy-in-the-management-of-hereditary-colorectal-cancer/
    """
    # pylint: enable=line-too-long
    return (
        nhp_apc.admission_has(primary_procedure, "H(22[189]|68[2489])")  # ty: ignore[call-non-callable]
        .admission_not(any_procedure, "H68[13]")
        .admission_not(any_diagnosis, "D126", "Q858", "Z(0[89][012789]|121)")
    )


def _endoscopy():
    # pylint: disable=line-too-long
    """Upper GI endoscopy

    source: https://ebi.aomrc.org.uk/interventions/upper-gi-endoscopy/
    """
    # pylint: enable=line-too-long
    return nhp_apc.admission_has(
        primary_procedure, "G(1(6[12389]|9[1289])|45[123489]|55[189]|65[189]|80[1389])"
    )  # ty: ignore[call-non-callable]


@activity_avoidance_mitigator()
def _evidence_based_interventions_gi_surgical():
    return evidence_based_interventions(
        _colonoscopy,
        _endoscopy,
    )
