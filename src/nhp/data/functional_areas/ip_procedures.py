import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.column import Column

from nhp.data.functional_areas.classifications import (
    class_age_adult,
    class_age_child,
    class_cardiac_cath,
    class_cardiology,
    class_daycase,
    class_elective,
    class_has_procedure,
    class_int_radiology,
    class_non_elective,
    class_surgical,
)


def is_unknown_time() -> Column:
    return F.col("theatre_time").isNull()


def is_adult_elective_surgical_procedures() -> Column:
    return (
        class_age_adult() & class_elective() & class_has_procedure() & class_surgical()
    )


def is_adult_nonelective_surgical_procedures() -> Column:
    return (
        class_age_adult()
        & class_non_elective()
        & class_has_procedure()
        & class_surgical()
    )


def is_adult_surgical_daycase_procedures() -> Column:
    return (
        class_age_adult() & class_daycase() & class_has_procedure() & class_surgical()
    )


def is_paediatric_elective_procedures() -> Column:
    return class_age_child() & class_elective() & class_has_procedure()


def is_paediatric_nonelective_procedures() -> Column:
    return class_age_child() & class_non_elective() & class_has_procedure()


def is_paediatric_daycase_procedures() -> Column:
    return class_age_child() & class_daycase() & class_has_procedure()


def is_cardiology_procedure():
    return class_cardiology() & class_has_procedure()


def is_catheter_procedure():
    return class_has_procedure() & class_cardiac_cath()


def is_cardiac_catheter_procedure():
    return is_cardiology_procedure() | is_catheter_procedure()


def is_int_radiology_proc():
    return class_int_radiology() & class_has_procedure()


def create_ip_procedure_groupings(df: DataFrame) -> DataFrame:
    """Adds "functional_area" column to the IP data with the functional areas for Inpatient activity (including
    maternity and daycases).

    Args:
        df (DataFrame): DataFrame representing the IP data

    Returns:
        DataFrame: DataFrame representing the IP data with the added "functional_area" column for procedure groupings
    """
    GROUPINGS = [
        (
            "adult_elective_surgical_procedures",
            is_adult_elective_surgical_procedures(),
        ),
        (
            "adult_nonelective_surgical_procedures",
            is_adult_nonelective_surgical_procedures(),
        ),
        (
            "adult_surgical_daycase_procedures",
            is_adult_surgical_daycase_procedures(),
        ),
        (
            "paediatric_elective_procedures",
            is_paediatric_elective_procedures(),
        ),
        (
            "paediatric_nonelective_procedures",
            is_paediatric_nonelective_procedures(),
        ),
        (
            "paediatric_daycase_procedures",
            is_paediatric_daycase_procedures(),
        ),
    ]

    when_chain = F.when(
        is_int_radiology_proc(), "interventional_radiology_procedure"
    ).when(is_cardiac_catheter_procedure(), "cardiac_catheter_procedure")
    for label, predicate_fn in GROUPINGS:
        when_chain = when_chain.when(
            predicate_fn & is_unknown_time(),
            f"{label}_unknown_time",
        ).when(
            predicate_fn,
            label,
        )

    return df.withColumn(
        "functional_area",
        when_chain.otherwise("ip_procedures_unknown"),
    )
