"""Generate inpatient mitigators"""

import importlib
import os

import raw_data.mitigators as mitigators


def generate_inpatients_mitigators() -> None:
    """Generrate Inpatients Mitigators"""
    path = ["mitigators", "ip"]

    import_errors = False
    for i in ["activity_avoidance", "efficiency"]:
        for j in sorted(os.listdir("/".join(path + [i]))):
            if j == "__init__.py":
                continue
            module = ".".join(path + [i, j])[:-3]
            try:
                importlib.import_module(module)
            except:  # noqa: E722
                import_errors = True
                print(f"Error: {module}")

    if import_errors:
        raise ImportError("Error importing modules")

    all_mitigators = [
        v2
        for v1 in mitigators.__registered_mitigators.values()  # pylint: disable=protected-access
        for v2 in v1.values()
    ]

    for m in all_mitigators:
        m.save()


if __name__ == "__main__":
    generate_inpatients_mitigators()
