"""Evidence-Based Interventions: MSK (IP-AA-013)

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

[ebi_1]: https://ebi.aomrc.org.uk/specialty/musculoskeletal-spine/
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nhp.data.raw_data.mitigators import activity_avoidance_mitigator
from nhp.data.raw_data.mitigators.ip.activity_avoidance.evidence_based_interventions import (
    evidence_based_interventions,
)
from nhp.data.raw_data.mitigators.ip.hes_datasets import (
    any_diagnosis,
    nhp_apc,
    primary_diagnosis,
    primary_procedure,
    secondary_diagnosis,
    secondary_procedure,
)


def _arthroscopic_meniscal_tear():
    # pylint: disable=line-too-long
    """Arthroscopic surgery for meniscal tears

    source: https://ebi.aomrc.org.uk/interventions/arthroscopic-surgery-for-meniscal-tears/
    """
    # pylint: enable=line-too-long
    df = nhp_apc.admission_not(any_diagnosis, "S832")  # ty: ignore[call-non-callable]

    # either the primary procedure is W82,
    # or it is W71[45] with Y767 and Z846 as secondary procedures
    procedures = [
        lambda x: x.admission_has(primary_procedure, "W82"),
        lambda x: (
            x.admission_has(primary_procedure, "W71[45]")
            .admission_has(secondary_procedure, "Y767")
            .admission_has(secondary_procedure, "Z846")
        ),
    ]

    # either there is a diagnosis of M323, or they have both M233 and M238 diagnoses
    diags = [
        lambda x: x.admission_has(any_diagnosis, "M232"),
        lambda x: (
            x.admission_has(any_diagnosis, "M233").admission_has(any_diagnosis, "M238")
        ),
    ]

    # create all combinations of the procedures and diagnoses from above
    return reduce(DataFrame.unionByName, [g(f(df)) for f in diags for g in procedures])


def _arthroscopic_shoulder_compression():
    # pylint: disable=line-too-long
    """Arthroscopic shoulder decompression for subacromial pain

    source: https://ebi.aomrc.org.uk/interventions/arthroscopic-shoulder-decompression-for-subacromial-pain/
    """
    # pylint: enable=line-too-long
    df = nhp_apc.admission_has(any_diagnosis, "M754").admission_not(  # ty: ignore[call-non-callable]
        any_diagnosis, "M751"
    )

    return DataFrame.unionByName(
        (
            df.admission_has(primary_procedure, "O291").admission_has(
                secondary_procedure, "Y767"
            )
        ),
        (
            df.admission_has(primary_procedure, "W(572|844)").admission_has(
                secondary_procedure, "Z812"
            )
        ),
    )


def _back_injections():
    # pylint: disable=line-too-long
    """Injections for nonspecific low back pain without sciatica

    source: https://ebi.aomrc.org.uk/interventions/injections-for-nonspecific-low-back-pain-without-sciatica-2/
    """
    # pylint: enable=line-too-long
    return nhp_apc.admission_has(
        primary_procedure, "A(5(2[1289]|77)|735)", "V544"
    ).admission_has(  # ty: ignore[call-non-callable]
        primary_diagnosis, "M545"
    )


def _dupuytrens():
    # pylint: disable=line-too-long
    """Dupuytren’s contracture release in adults

    source: https://ebi.aomrc.org.uk/interventions/dupuytrens-contracture-release-in-adults/
    """
    # pylint: enable=line-too-long
    return (
        nhp_apc.admission_has(primary_procedure, "T5(2[1256]|4[13])")  # ty: ignore[call-non-callable]
        .admission_has(primary_diagnosis, "M720")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


def _fusion_surgery():
    # pylint: disable=line-too-long
    """Fusion surgery for mechanical axial low back pain

    source: https://ebi.aomrc.org.uk/interventions/fusion-surgery-for-mechanical-axial-low-back-pain-2/
    """
    # pylint: enable=line-too-long

    pregnancy_diagnoses = secondary_diagnosis("O268").join(
        secondary_diagnosis("M5(33|4[345])"), ["epikey", "fyear"], "semi"
    )

    return (
        nhp_apc.admission_has(primary_procedure, "V(3(8[23456]|9[34567])|404)")  # ty: ignore[call-non-callable]
        .admission_has(primary_diagnosis, "M54[34589]")
        .admission_not(secondary_diagnosis, "M4(0[012]|[12]|3[01589]|45)")
        .join(pregnancy_diagnoses, ["epikey", "fyear"], "anti")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


def _ganglion_excision():
    # pylint: disable=line-too-long
    """Ganglion excision

    source: https://ebi.aomrc.org.uk/interventions/ganglion-excision/
    """
    # pylint: enable=line-too-long

    return (
        nhp_apc.admission_has(primary_procedure, "T(59[12]|60[12])")  # ty: ignore[call-non-callable]
        .admission_has(primary_diagnosis, "M(255|674)")
        .admission_has(secondary_diagnosis, "M258")
    )


def _knee_athroscopy_for_osteoarhritis():
    # pylint: disable=line-too-long
    """Knee arthroscopy for patients with osteoarthritis

    source: https://ebi.aomrc.org.uk/interventions/knee-arthroscopy-for-patients-with-osteoarthritis/
    """
    # pylint: enable=line-too-long

    procs = DataFrame.unionByName(
        nhp_apc.admission_has(primary_procedure, "W85[12]"),  # ty: ignore[call-non-callable]
        nhp_apc.admission_has(primary_procedure, "W802")  # ty: ignore[call-non-callable]
        .admission_has(secondary_procedure, "Y767")
        .admission_has(secondary_procedure, "Z846"),
    )

    return (
        procs.admission_has(primary_diagnosis, "M1[57]")  # ty: ignore[call-non-callable]
        .admission_not(secondary_diagnosis, "M238")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


def _lumbar_disectomy():
    # pylint: disable=line-too-long
    """Lumbar disectomy

    source: https://ebi.aomrc.org.uk/interventions/lumbar-discectomy/
    """
    # pylint: enable=line-too-long

    procs = DataFrame.unionByName(
        nhp_apc.admission_has(primary_procedure, "V(33[1-9]|5(1[189]|83)|603)"),  # ty: ignore[call-non-callable]
        nhp_apc.admission_has(
            primary_procedure, "V(52[12589]|(58|60)[89])"
        ).admission_has(  # ty: ignore[call-non-callable]
            secondary_procedure, "Z993"
        ),
    )

    return procs.admission_has(primary_diagnosis, "M5(1[01]|4[134])").filter(  # ty: ignore[call-non-callable]
        (F.col("age") >= 19) & (F.col("age") <= 120)
    )


def _lumbar_rf_facet_joint_denervation():
    # pylint: disable=line-too-long
    """Lumbar radiofrequency facet joint denervation

    source: https://ebi.aomrc.org.uk/interventions/lumbar-radiofrequency-facet-joint-denervation/
    """
    # pylint: enable=line-too-long

    return (
        nhp_apc.admission_has(primary_procedure, "V48[57]")  # ty: ignore[call-non-callable]
        .admission_has(primary_diagnosis, "M5(1[289]|4[59])")
        .admission_has(any_diagnosis, "Z(67[567]|993)")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


def _trigger_finger_release():
    # pylint: disable=line-too-long
    """Trigger finger release in adults

    source: https://ebi.aomrc.org.uk/interventions/trigger-finger-release-in-adults/
    """
    # pylint: enable=line-too-long

    return (
        nhp_apc.admission_has(primary_procedure, "T(69[1289]|7(0[12]|1[189]|2[389]))")  # ty: ignore[call-non-callable]
        .admission_has(primary_diagnosis, "M65(3|[89]4)")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


def _vertebral_augmentation():
    # pylint: disable=line-too-long
    """Vertebral augmentation for painful osteoporotic vertebral fractures

    source: https://ebi.aomrc.org.uk/interventions/vertebral-augmentation-for-painful-osteoporotic-vertebral-fractures/
    """
    # pylint: enable=line-too-long

    return (
        nhp_apc.admission_has(primary_procedure, "V44[45]")  # ty: ignore[call-non-callable]
        .admission_has(primary_diagnosis, "M80[01234589]")
        .filter((F.col("age") >= 19) & (F.col("age") <= 120))
    )


@activity_avoidance_mitigator()
def _evidence_based_interventions_msk():
    return evidence_based_interventions(
        _arthroscopic_meniscal_tear,
        _arthroscopic_shoulder_compression,
        _back_injections,
        _dupuytrens,
        _fusion_surgery,
        _ganglion_excision,
        _knee_athroscopy_for_osteoarhritis,
        _lumbar_disectomy,
        _lumbar_rf_facet_joint_denervation,
        _trigger_finger_release,
        _vertebral_augmentation,
    )
