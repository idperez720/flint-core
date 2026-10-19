"""Logical Pandas contract validation, independent of storage formats."""

import math

from flint_core.runtime.errors import DataValidationError


def _pandas():
    try:
        import pandas
    except ImportError:
        raise DataValidationError("Install the pandas extra for Pandas execution.") from None
    return pandas


def validate_pandas(frame, definition):
    pandas = _pandas()
    if not isinstance(frame, pandas.DataFrame):
        raise DataValidationError("Only Pandas DataFrames are supported in a Pandas pipeline.")
    owned = frame.copy(deep=True)
    names = list(owned.columns)
    if any(not isinstance(name, str) or not name.strip() for name in names) or len(set(names)) != len(names):
        raise DataValidationError("Column labels must be distinct non-empty strings.")
    declared = definition.schema.columns
    if set(declared) - set(names):
        raise DataValidationError("Required columns are missing.")
    if definition.schema.extra_columns == "forbid" and set(names) - set(declared):
        raise DataValidationError("Extra columns are forbidden.")
    for name in names:
        series = owned[name]
        column = declared.get(name)
        if column is not None and not column.nullable and series.isna().any():
            raise DataValidationError("A non-nullable column contains missing values.")
        data_type = column.type if column is not None else "string"
        dtype = str(series.dtype)
        if data_type == "int64" and dtype not in ("int64", "Int64"):
            raise DataValidationError("Expected int64 without coercion.")
        if data_type == "boolean" and dtype not in ("bool", "boolean"):
            raise DataValidationError("Expected boolean without coercion.")
        if data_type == "float64":
            if dtype not in ("float64", "Float64") or not all(math.isfinite(value) for value in series.dropna()):
                raise DataValidationError("Expected finite float64 values without coercion.")
        if data_type == "string":
            string_dtype = isinstance(series.dtype, pandas.StringDtype) or pandas.api.types.is_object_dtype(
                series.dtype
            )
            if not string_dtype or not all(isinstance(value, str) for value in series.dropna()):
                raise DataValidationError("Expected string values without coercion.")
    for constraint in definition.constraints:
        if constraint.severity != "error" or owned.duplicated(subset=list(constraint.columns)).any():
            raise DataValidationError("A declared uniqueness constraint failed.")
    return owned
