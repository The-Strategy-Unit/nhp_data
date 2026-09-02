import pyspark.sql.functions as F
from pyspark.sql.column import Column


def class_op_first() -> Column:
    return ~F.col("has_procedures") & F.col("is_first")


def class_op_follow_up() -> Column:
    return ~F.col("has_procedures") & ~F.col("is_first")


def class_op_virtual() -> Column:
    return F.col("tele_attendances")


def class_op_face_to_face() -> Column:
    return F.col("attendances")
