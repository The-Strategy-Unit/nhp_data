"""Virtual Wards Admission Avoidance

Virtual wards allow patients to receive the care they need at home, safely and conveniently rather
than in hospital. They also provide systems with a significant opportunity to narrow the gap between
demand and capacity for secondary care beds, by providing an alternative to admission and/or early
discharge.

Whilst virtual wards may be beneficial for patients on a variety of clinical pathways, guidance has
been produced relating to three pathways which represent the majority of patients who may be
clinically suitable to benefit from a virtual ward. These pathways are Frailty, Acute Respiratory
Infections (ARI) and Heart failure.

This activity avoidance mitigator identifies patients who may be suitable for admission to an ARI or
Heart Failure virtual ward.

### Available breakdowns

- Acute Respiratory Infection (IP-AA-030)
- Heart Failure (IP-AA-031)
"""

from raw_data.mitigators import activity_avoidance_mitigator
from raw_data.mitigators.ip.shared import virtual_wards


@activity_avoidance_mitigator()
def _virtual_wards_activity_avoidance_ari():
    return virtual_wards.ari()


@activity_avoidance_mitigator()
def _virtual_wards_activity_avoidance_heart_failure():
    return virtual_wards.heart_failure()
