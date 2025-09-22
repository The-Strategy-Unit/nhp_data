# pylint: disable=line-too-long
"""Ambulatory Care Sensitive Admissions

Ambulatory Care Sensitive Conditions (ACSC) admissions are those that can potentially be avoided
with effective management and treatment such as improved primary or community health care services
such as screening, vaccination, immunisation and health monitoring.
Admissions of this type are identified using a basket of ICD10 codes for 19 chronic and acute
conditions.

These conditions are categorised into three groups; vaccine preventable conditions, chronic
conditions, and acute conditions.
These three groups can then be broken down into 19 health groups.

### Vaccine Preventable (IP-AA-006)

1.  Influenza and Pneumonia
2.  Other vaccine-preventable

### Chronic (IP-AA-005)

3.  Asthma
4.  Congestive heart failure
5.  Diabetes complications
6.  Chronic obstructive pulmonary disease
7.  Angina
8.  Iron-deficiency anaemia
9.  Hypertension
10. Nutritional deficiencies

### Acute (IP-AA-004)

11. Dehydration and gastroenteritis
12. Pyelonephritis
13. Perforated/bleeding ulcer
14. Cellulitis
15. Pelvic inflammatory disease
16. Ear, nose and throat infections
17. Dental conditions
18. Convulsions and epilepsy
19. Gangrene

## Source of Ambulatory Care Sensitive Admissions codes

The list of conditions used within this factor comes from the
[NHS Institute for Innovation and Improvement from the document below][asc_1].

The full condition list and coding logic can be found within the [appendix][asc_2].

The list of conditions that comprise Ambulatory Care Sensitive Conditions used within this model is
one of several that are available for example a slightly Different set of conditions are used in the
[NHS Outcomes Framework][asc_3].

[asc_1]: https://webarchive.nationalarchives.gov.uk/ukgwa/20220301011245/https://digital.nhs.uk/data-and-information/data-tools-and-services/data-services/innovative-uses-of-data/demand-on-healthcare/ambulatory-care-sensitive-conditions
[asc_2]: https://webarchive.nationalarchives.gov.uk/ukgwa/20210206234503/https://digital.nhs.uk/binaries/content/assets/website-assets/data-and-information/data-tools-and-services/data-services/innovative-uses-of-data/acsc-appendix-a.pdf
[asc_3]: https://digital.nhs.uk/data-and-information/publications/statistical/nhs-outcomes-framework/february-2021/domain-2-enhancing-quality-of-life-for-people-with-long-term-conditions-nof/2.3.i-unplanned-hospitalisation-for-chronic-ambulatory-care-sensitive-conditions
"""
# pylint: enable=line-too-long

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from nhp.data.hes_datasets import (
    any_diagnosis,
    any_procedure,
    nhp_apc,
    primary_diagnosis,
    primary_procedure,
    secondary_diagnosis,
)
from nhp.data.raw_data.mitigators import activity_avoidance_mitigator


@activity_avoidance_mitigator()
def _ambulatory_care_conditions_acute():
    cellulitis = nhp_apc.admission_has(
        primary_diagnosis, "L(0([34]|8[089])|88X|980)"
    ).admission_not(  # ty: ignore[call-non-callable]
        any_procedure, "[A-HJ-RTVW]", "S([123]|4[1-589])", "X0[1245]"
    )

    gangrene = nhp_apc.admission_has(any_diagnosis, "R02X")  # ty: ignore[call-non-callable]

    other_conditions = nhp_apc.admission_has(  # ty: ignore[call-non-callable]
        primary_diagnosis,
        # convulsions_and_epilepsy
        "G4[01]|O15|R56",
        # dehydration_and_gastroenteritis
        "E86X|K52[289]",
        # dental_conditions
        "A690|K(0([2-68]|9[89])|1[23])",
        # ent_infections
        "H6[67]|J(0[236]|312)",
        # pelvic_inflammatory_disease
        "N7[034]",
        # perforated_bleeding_ulcer
        "K2[5-8][012456]",
        # pyelonephritis,
        "N1([012]|36)",
    )

    return (
        reduce(DataFrame.unionByName, [cellulitis, gangrene, other_conditions])
        .filter(F.col("admimeth").rlike("^2"))
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
        .distinct()
    )


@activity_avoidance_mitigator()
def _ambulatory_care_conditions_chronic():
    def df(*args):
        return nhp_apc.admission_has(primary_diagnosis, *args)  # ty: ignore[call-non-callable]

    conditions = [
        # angina
        df("I2(0|4[089])").admission_not(
            primary_procedure, "[ABCDEFGHJKLMNOPQRSTVW]", "X0[1245]"
        ),
        # asthma
        df("J4[56]"),
        # congestive_heart_failure / hypertension
        df("I((11|5)0|1(0X|19))", "J81").admission_not(
            any_procedure, "K([0-4]|5[02567]|6[016-9]|71)"
        ),
        # copd
        df("J4[12347]"),
        df("J20").admission_has(secondary_diagnosis, "J4[12347]"),  # ty: ignore[call-non-callable]
        # diabetes_complications
        nhp_apc.admission_has(any_diagnosis, "E1[0-4][0-8]"),  # ty: ignore[call-non-callable]
        # iron-deficiency_anaemia
        df("D50[189]"),
        # nutritional_deficiencies
        df("E(4[0123]X|550|643)"),
    ]

    return (
        reduce(DataFrame.unionByName, conditions)
        .filter(F.col("admimeth").startswith("2"))
        .select("fyear", "provider", "epikey")
        .distinct()
        .withColumn("sample_rate", F.lit(1.0))
    )


@activity_avoidance_mitigator()
def _ambulatory_care_conditions_vaccine_preventable():
    def df(*args):
        return nhp_apc.admission_has(any_diagnosis, *args)  # ty: ignore[call-non-callable]

    conditions = [
        # influenza_and_pneumonia
        df("J1([0134]|5[3479]|68|8[18])").admission_not(secondary_diagnosis, "D57"),  # ty: ignore[call-non-callable]
        # other
        df("A(3[567]|80)", "B(0[56]|1(6[19]|8[01])|26)", "G000", "M014"),
    ]

    return (
        reduce(DataFrame.unionByName, conditions)
        .filter(F.col("admimeth").startswith("2"))
        .select("fyear", "provider", "epikey")
        .distinct()
        .withColumn("sample_rate", F.lit(1.0))
    )
