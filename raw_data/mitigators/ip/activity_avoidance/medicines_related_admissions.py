# pylint: disable=line-too-long
"""Medicines Related Admissions

Between 5 and 10%[^mra_1] of all hospital admissions may be medicines related.
Such admissions can occur due to drug therapy failure, adverse reactions to drug therapy, failure to
adhere to prescribed regimen or unintentional overdose. 
Admissions of this type could be prevented with better self management on the patients' part or
improved medicines management.

The model identifies these admissions using two approaches. The first identifies admissions where
the presence of one or more of a number of ICD10 cause codes indicate the use of a medicine is a
factor in the admission.

Admissions where there is an explicit cause code probably underestimate the true number of medicines
related admissions. Therefore, the second approach identifies admissions where a medicines related
cause is not explicitly recorded but may be a factor in the admission. In order to do this, the
model identifies admissions with a specific combination of primary and secondary diagnoses that are
indicative of a medicine related cause. For example the long term use of high dose NSAIDs is a
possible cause of an admission with a primary diagnosis of gastric ulcer and a secondary diagnosis
of arthritis. The list of diagnoses used within the coding of this factor is based on exploratory
work undertaken by the Strategy Unit. Other valid combinations could potentially be added to current
set.  

[^mra_1]: Kongkaew, C., Noyce, P.R. and Ashcroft, D.M., 2008. Hospital admissions associated with adverse drug reactions: a systematic review of prospective observational studies. *Annals of Pharmacotherapy*, 42(7-8), pp.1017-1025.

### Available breakdowns

- Medicines related admissions - Explicitly coded (IP-AA-021)
- Medicines related admissions - Implicitly coded
  - Admissions potentially related to NSAIDs (IP-AA-025)
  - Admissions potentially related to Anti-Diabetics (IP-AA-022)
  - Admissions potentially related to Benzodiazepines (IP-AA-023)
  - Admissions potentially related to Diuretics (IP-AA-024)
"""
# pylint: enable=line-too-long

from pyspark.sql import functions as F

from hes_datasets import any_diagnosis, nhp_apc, primary_diagnosis, secondary_diagnosis
from raw_data.mitigators import activity_avoidance_mitigator


def _medicines_related_admissions_implicit(primary_diags, secondary_diags):
    return (
        nhp_apc.filter(F.col("admimeth").rlike("^2"))
        .admission_has(primary_diagnosis, *primary_diags)
        .admission_has(secondary_diagnosis, *secondary_diags)
        .admission_not(secondary_diagnosis, "Y(4|5[0-7])")
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@activity_avoidance_mitigator()
def _medicines_related_admissions_explicit():
    return (
        nhp_apc.filter(F.col("admimeth").rlike("^2"))
        .admission_has(any_diagnosis, "Y(4|5[0-7])")
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@activity_avoidance_mitigator("medicines_related_admissions_implicit_anti-diabetics")
def _medicines_related_admissions_implicit_anti_diabetics():
    return _medicines_related_admissions_implicit(
        ["E(16[012]|781)", "R(55X|739)", "T383"], ["E1[0-4]"]
    )


@activity_avoidance_mitigator()
def _medicines_related_admissions_implicit_benzodiasepines():
    return _medicines_related_admissions_implicit(
        ["R55X", "S(060|52[0-8]|628|720)", "W"], ["F"]
    )


@activity_avoidance_mitigator()
def _medicines_related_admissions_implicit_diurectics():
    return _medicines_related_admissions_implicit(
        ["E8(6X|7[56])", "I4(70|9[89])", "R5(5X|71)"], ["I1(0X|[12][09]|3[01239]|50)"]
    )


@activity_avoidance_mitigator()
def _medicines_related_admissions_implicit_nsaids():
    return _medicines_related_admissions_implicit(
        ["E87[56]", "I50[019]", "K(25[059]|922)", "R10[34]"],
        ["M(0(5[389]|6[089]|80)|1(5[0-489]|6[0-79]|[78][0-59]|9[01289]))"],
    )
