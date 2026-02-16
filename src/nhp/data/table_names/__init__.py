from nhp.data.table_names.mlcsu import mlcsu
from nhp.data.table_names.table_names import TableNames
from nhp.data.table_names.udal import udal

# TODO: this should be set via an environment variable or similar when deploying asset bundles
environment = "mlcsu"  # mlcsu / udal


_selected_table_names: TableNames
match environment:
    case "mlcsu":
        _selected_table_names = mlcsu
    case "udal":
        _selected_table_names = udal
    case _:
        raise ValueError(f"Unknown environment: {environment}")

table_names = _selected_table_names
