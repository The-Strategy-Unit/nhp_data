"""Generate inpatient mitigators"""

import importlib
import os
import pkgutil

from nhp.data.raw_data import mitigators


def generate_inpatients_mitigators() -> None:
    """Generrate Inpatients Mitigators"""
    import_errors = False

    for i in ["activity_avoidance", "efficiency"]:
        path = os.path.join(mitigators.__path__[0], "ip", i)
        for _, name, _ in pkgutil.walk_packages([path]):
            try:
                m = f"{mitigators.__name__}.ip.{i}.{name}"
                importlib.import_module(name)
                print(f"Imported: {name}")
            except ImportError:
                import_errors = True
                print(f"Error: {name}")

    if import_errors:
        raise ImportError("Error importing modules")

    all_mitigators = [
        v2
        for v1 in mitigators.__registered_mitigators.values()  # pylint: disable=protected-access
        for v2 in v1.values()
    ]

    for m in all_mitigators:
        m.save()
