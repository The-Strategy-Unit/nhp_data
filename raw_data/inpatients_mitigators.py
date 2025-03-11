"""Generate inpatient mitigators"""

import importlib
import os

import raw_data.mitigators as mitigators


def generate_inpatients_mitigators() -> None:
    """Generrate Inpatients Mitigators"""
    path = ["mitigators", "ip"]

    for i in ["activity_avoidance", "efficiency"]:
        for j in sorted(os.listdir("/".join(path + [i]))):
            if j == "__init__.py":
                continue
            module = ".".join(path + [i, j])[:-3]
            try:
                importlib.import_module(module)
            except:  # pylint: disable=bare-except
                print(f"Error: {module}")

    all_mitigators = [
        v2
        for v1 in mitigators.__registered_mitigators.values()  # pylint: disable=protected-access
        for v2 in v1.values()
    ]

    errors = {}

    for m in all_mitigators:
        try:
            m.save()
        except Exception as e:  # pylint: disable=broad-exception-caught
            errors[str(m)] = e

    print(errors)


if __name__ == "__main__":
    generate_inpatients_mitigators()
