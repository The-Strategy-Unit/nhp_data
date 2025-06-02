# pylint: disable=line-too-long
"""Cancelled Operations

Operations can be cancelled after a patient has been admitted this is inefficient for the provider
and costly for the commissioner as they both incur an unnecessary opportunity cost.
Cancelled operations can be for a variety of clinical, administrative or patient related reasons.
Some of these admissions could be avoided through for example improved administration and scheduling
or enhanced pre-op patient optimisation and assessment.

These spells are identified via the HRG codes which identity spells where the planned procedure was
not carried out.

## Differences between years

HRG codes are subject to change these should therefore be reviewed periodically.

## Source of Cancelled Operations codes

HRG codes are published annually by [NHS England][co_1].

[co_1]: https://www.england.nhs.uk/publication/national-tariff-payment-system-documents-annexes-and-supporting-documents/
"""
# pylint: enable=line-too-long

from pyspark.sql import functions as F

from hes_datasets import nhp_apc
from raw_data.mitigators import activity_avoidance_mitigator


@activity_avoidance_mitigator()
def _cancelled_operations():
    return (
        nhp_apc.filter(F.col("admimeth").rlike("^1[123]"))
        .filter(F.col("dismeth") != 4)
        .filter(F.col("sushrg").rlike("^(S22|W(A14|H50))"))
        .select("fyear", "provider", "epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )
