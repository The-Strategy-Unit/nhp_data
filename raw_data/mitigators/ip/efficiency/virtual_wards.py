"""Virtual Wards LOS reduction

Virtual wards allow patients to receive the care they need at home, safely and conveniently rather
than in hospital. They also provide systems with a significant opportunity to narrow the gap between
demand and capacity for secondary care beds, by providing an alternative to admission and/or early
discharge. 

Whilst virtual wards may be beneficial for patients on a variety of clinical pathways guidance has
been produced relating to three pathways which represent the majority of patients who may be
clinically suitable to benefit from a virtual ward. These pathways are Frailty, Acute Respiratory
Infections (ARI) and Heart failure. 

This efficiency mitigator identifies patients who may be suitable for earlier discharge through
admission to step down ARI or Heart Failure virtual wards. 
"""

from raw_data.mitigators import efficiency_mitigator
from raw_data.mitigators.ip.activity_avoidance.virtual_wards import (
    virtual_wards_ari,
    virtual_wards_heart_failure,
)

# use the definitions from activity avoidance


@efficiency_mitigator()
def _virtual_wards_efficiencies_ari():
    return virtual_wards_ari()


@efficiency_mitigator()
def _virtual_wards_efficiencies_heart_failure():
    return virtual_wards_heart_failure()
