from functools import reduce
from itertools import product

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from nhp.data.functional_areas.classifications import (
    class_age_adult,
    class_age_child,
    class_birth_assisted,
    class_birth_elective_csection,
    class_birth_event,
    class_birth_nonelective_c_section,
    class_birth_normal,
    class_daycase,
    class_elective,
    class_endoscopy,
    class_haem_onc,
    class_has_procedure,
    class_maternity,
    class_medical,
    class_no_birth_event,
    class_non_elective,
    class_non_zero_los,
    class_regular_day_night,
    class_renal,
    class_surgical,
    class_zero_los,
)


# ------------------------------------------------------------------------------
# Maternity and delivery related groupings
# ------------------------------------------------------------------------------
def is_normal_delivery_nonzerolos():
    return (
        class_maternity()
        & class_birth_event()
        & class_birth_normal()
        & class_non_zero_los()
    )


def is_normal_delivery_zerolos():
    return (
        class_maternity()
        & class_birth_event()
        & class_birth_normal()
        & class_zero_los()
    )


def is_assisted_delivery_nonzerolos():
    return (
        class_maternity()
        & class_birth_event()
        & class_birth_assisted()
        & class_non_zero_los()
    )


def is_assisted_delivery_zerolos():
    return (
        class_maternity()
        & class_birth_event()
        & class_birth_assisted()
        & class_zero_los()
    )


def is_maternity_assessment():
    return class_maternity() & class_zero_los() & class_no_birth_event()


def is_nonelective_csection_nonzerolos():
    return (
        class_maternity()
        & class_birth_event()
        & class_birth_nonelective_c_section()
        & class_non_zero_los()
    )


def is_nonelective_csection_zerolos():
    return (
        class_maternity()
        & class_birth_event()
        & class_birth_nonelective_c_section()
        & class_zero_los()
    )


def is_elective_csection_nonzerolos():
    return (
        class_maternity()
        & class_birth_event()
        & class_birth_elective_csection()
        & class_non_zero_los()
    )


def is_elective_csection_zerolos():
    return (
        class_maternity()
        & class_birth_event()
        & class_birth_elective_csection()
        & class_zero_los()
    )


def is_overnight_no_birth_event():
    return class_maternity() & class_non_zero_los() & class_no_birth_event()


# ------------------------------------------------------------------------------
# Daycase related groupings
# ------------------------------------------------------------------------------
def is_renal_elective():
    return class_renal() & class_elective() & class_zero_los()


def is_renal_regular_day_night():
    return class_renal() & class_regular_day_night()


def is_daycase_haem_onc():
    return class_daycase() & class_haem_onc() & class_has_procedure()


def is_daycase_endoscopy():
    return class_daycase() & class_endoscopy()


def is_specialty_daycase():
    return (
        is_renal_elective()
        | is_renal_regular_day_night()
        | is_daycase_haem_onc()
        | is_daycase_endoscopy()
    )


def is_daycase_adult_medical():
    return (
        class_daycase()
        & class_age_adult()
        & class_medical()
        & ~F.coalesce(is_specialty_daycase(), F.lit(False))
    )


def is_daycase_adult_surgical():
    return (
        class_daycase()
        & class_age_adult()
        & class_surgical()
        & ~F.coalesce(is_specialty_daycase(), F.lit(False))
    )


def is_daycase_child_medical():
    return (
        class_daycase()
        & class_age_child()
        & class_medical()
        & ~F.coalesce(is_specialty_daycase(), F.lit(False))
    )


def is_daycase_child_surgical():
    return (
        class_daycase()
        & class_age_child()
        & class_surgical()
        & ~F.coalesce(is_specialty_daycase(), F.lit(False))
    )


# ------------------------------------------------------------------------------
# Ordinary admissions related groupings
# ------------------------------------------------------------------------------


def build_ordinary_admissions_ward_groupings():
    """Builds the F.when(...).when(...)... chain from the 4 classification
    dimensions instead of 16 hand-written predicate functions."""

    # Each dimension: label -> predicate-producing function
    AGE_DIMENSION = {
        "adult": class_age_adult,
        "paediatric": class_age_child,
    }
    ADMISSION_DIMENSION = {
        "elective": class_elective,
        "nonelective": class_non_elective,
    }
    TREATMENT_DIMENSION = {
        "medical": class_medical,
        "surgical": class_surgical,
    }
    LOS_DIMENSION = {
        "nonzerolos": class_non_zero_los,
        "zerolos": class_zero_los,
    }

    DIMENSIONS = [
        AGE_DIMENSION,
        ADMISSION_DIMENSION,
        TREATMENT_DIMENSION,
        LOS_DIMENSION,
    ]

    combinations = product(*[d.items() for d in DIMENSIONS])

    when_chain = F.when(F.lit(False), "-")

    for combo in combinations:
        labels, predicate_fns = zip(*combo)
        label = "_".join(labels)
        condition = reduce(lambda a, b: a & b, (fn() for fn in predicate_fns))

        when_chain = when_chain.when(condition, label)

    return when_chain.otherwise("unknown")


# ------------------------------------------------------------------------------
# Overall function
# ------------------------------------------------------------------------------
def create_ip_ward_groupings(df: DataFrame) -> DataFrame:
    """Adds "functional_area" column to the IP data with the functional areas for Inpatient activity (including
    maternity and daycases).

    Args:
        df (DataFrame): DataFrame representing the IP data

    Returns:
        DataFrame: DataFrame representing the IP data with the added "functional_area" column for the daycase functional
        area grouping
    """
    return df.withColumn(
        "functional_area",
        F
        # maternity
        .when(is_normal_delivery_zerolos(), "maternity_normal_delivery_zerolos")
        .when(is_normal_delivery_nonzerolos(), "maternity_normal_delivery_nonzerolos")
        .when(is_assisted_delivery_zerolos(), "maternity_assisted_delivery_zerolos")
        .when(
            is_assisted_delivery_nonzerolos(),
            "maternity_assisted_delivery_nonzerolos",
        )
        .when(is_maternity_assessment(), "maternity_assessment")
        .when(
            is_nonelective_csection_zerolos(),
            "maternity_nonelective_csection_zerolos",
        )
        .when(
            is_nonelective_csection_nonzerolos(),
            "maternity_nonelective_csection_nonzerolos",
        )
        .when(is_elective_csection_zerolos(), "maternity_elective_csection_zerolos")
        .when(
            is_elective_csection_nonzerolos(),
            "maternity_elective_csection_nonzerolos",
        )
        .when(is_overnight_no_birth_event(), "maternity_overnight_no_birth")
        # daycase
        .when(
            is_renal_elective() | is_renal_regular_day_night(),
            "daycase_renal",
        )
        .when(is_daycase_haem_onc(), "daycase_haem_onc")
        .when(is_daycase_endoscopy(), "daycase_endoscopy")
        .when(is_daycase_adult_medical(), "adult_daycase_medical")
        .when(is_daycase_adult_surgical(), "adult_daycase_surgical")
        .when(is_daycase_child_medical(), "paediatric_daycase_medical")
        .when(is_daycase_child_surgical(), "paediatric_daycase_surgical")
        .otherwise(build_ordinary_admissions_ward_groupings()),
    )
