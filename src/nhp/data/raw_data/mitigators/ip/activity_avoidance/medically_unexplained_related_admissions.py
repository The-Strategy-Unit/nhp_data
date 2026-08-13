"""Medically Unexplained Admissions (IP-AA-020)

Around 1 in 4 people who visit their GP have symptoms that the GP is [unable to explain][mua_1]
even after conducting appropriate testing. Sometimes the symptoms are severe enough to
precipitate an admission to hospital yet ultimately an underlying physical cause cannot be
determined.

Some of these symptoms may have a psychological cause and as such may be avoidable through improved
access to psychological therapy services or through population health measures that promote
psychological well being.

The model identifies admissions where the primary diagnosis is one of a small basket of symptoms
where a physiological cause is in many cases undetectable.

[mua_1]: https://www.rcpsych.ac.uk/mental-health/problems-disorders/medically-unexplained-symptoms
"""

from pyspark.sql import functions as F

from nhp.data.raw_data.mitigators import activity_avoidance_mitigator
from nhp.data.raw_data.mitigators.ip.hes_datasets import nhp_apc, primary_diagnosis


@activity_avoidance_mitigator()
def _medically_unexplained_related_admissions():
    return (
        nhp_apc.admission_has(  # ty: ignore[call-non-callable]
            primary_diagnosis,
            "F510",
            "G(4(4[028]|70)|501)",
            "H931",
            "K5(80|9[01])",
            "M545",
            "R(0(02|7[14])|12X|251|42X|5[13]X)",
        )
        .filter(F.col("admimeth").rlike("^2"))
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
