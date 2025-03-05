"""Emergency Elderly (75+)

Emergency admissions of elderly patients often result in extended stays in hospital due to the
challenges in organising for the timely provision of home support or residential/nursing
accommodation upon discharge. SUch delays are referred to as delayed transfers of care. Ensuring
that patients stay in hospital only as long as is clinically necessary is particularly important for
elderly patients as even short stays in hospital can result in rapid irreversible decompensation.
In addition minimising the LOS for such patients would also free up capacity within the hospital.
"""

from pyspark.sql import functions as F

from hes_datasets import nhp_apc
from mitigators import efficiency_mitigator


@efficiency_mitigator()
def _emergency_elderly():
    return (
        nhp_apc.filter(F.col("age") >= 75)
        .filter(F.col("admimeth").rlike("^2"))
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
