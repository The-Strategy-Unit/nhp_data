"""Enhanced Recovery

[Enhanced Recovery][er_1] (ER) is an evidence-based model that is intended to help patients recover
from major elective surgery quicker than they normally would do.
It is based on the following principles:

- the patient is in the best possible condition for surgery
- the patient has the best possible management during and after their operation
- the patient experiences the best post-operative rehabilitation

Whilst many procedures would benefit from adopting enhanced recovery approaches, the most common
specialties where enhanced recovery programmes have been implemented are musculoskeletal (MSK),
colorectal surgery, gynaecology and urology, as these were the areas targeted by the Department of
Health enhanced recovery programme.

The 2011 programme report specified a number of high volume procedures within these specialties that
would be the focus of ER programmes and it is these that are included within th ER efficiency
factors.

In addition, we have subsequently also included a number of breast surgery procedures as more recent
[NHS information][er_2] identifies this as an area where ER programmes are commonly in place.

[er_1]: https://www.gov.uk/government/publications/enhanced-recovery-partnership-programme
[er_2]: https://www.nhs.uk/conditions/enhanced-recovery/

## Available breakdowns

- Bladder (IP-EF-010)
- Breast (IP-EF-011)
- Colectomy (IP-EF-012)
- Hip (IP-EF-013)
- Hysterectomy (IP-EF-014)
- Knee (IP-EF-015)
- Prostate (IP-EF-016)
- Rectum (IP-EF-017)
"""

from pyspark.sql import functions as F

from nhp.data.hes_datasets import nhp_apc, primary_procedure
from nhp.data.raw_data.mitigators import efficiency_mitigator


def _enhanced_recovery(*args, sex="[12]"):
    return (
        nhp_apc.filter(F.col("admimeth").rlike("^1"))
        .filter(F.col("sex").rlike(f"^{sex}"))
        .admission_has(primary_procedure, *args)
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@efficiency_mitigator()
def _enhanced_recovery_bladder():
    return _enhanced_recovery("M34")


@efficiency_mitigator()
def _enhanced_recovery_breast():
    return _enhanced_recovery("B2[789]")


@efficiency_mitigator()
def _enhanced_recovery_colectomy():
    return _enhanced_recovery("H(0[5-9]|10)")


@efficiency_mitigator()
def _enhanced_recovery_hip():
    return _enhanced_recovery("W(3[789]|4[678]|9[345])")


@efficiency_mitigator()
def _enhanced_recovery_hysterectomy():
    return _enhanced_recovery("Q0[78]", sex="2")


@efficiency_mitigator()
def _enhanced_recovery_knee():
    return _enhanced_recovery("W4[012]1")


@efficiency_mitigator()
def _enhanced_recovery_prostate():
    return _enhanced_recovery("M61", "1")


@efficiency_mitigator()
def _enhanced_recovery_rectum():
    return _enhanced_recovery("H33[^7]")
