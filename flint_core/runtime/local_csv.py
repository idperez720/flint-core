"""Strict local CSV codec and non-overwriting, same-filesystem publication."""

import csv
import math
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path

from flint_core.runtime.errors import DataValidationError, PublicationUncertain

_INTEGER = re.compile(r"-?(?:0|[1-9][0-9]*)")
_FLOAT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_NULL = "\\N"


def _pandas():
    try:
        import pandas
    except ImportError:
        raise DataValidationError("Install the pandas extra to execute local CSV pipelines.") from None
    return pandas


@dataclass(frozen=True)
class PublicationReceipt:
    cleanup_pending: bool = False


class LocalCsvAdapter:
    def validate(self, frame, definition):
        pandas = _pandas()
        if not isinstance(frame, pandas.DataFrame):
            raise DataValidationError("Only Pandas DataFrames are supported.")
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
            if data_type == "string" and not all(isinstance(value, str) for value in series.dropna()):
                raise DataValidationError("Expected string values without coercion.")
        for constraint in definition.constraints:
            if constraint.severity != "error" or owned.duplicated(subset=list(constraint.columns)).any():
                raise DataValidationError("A declared uniqueness constraint failed.")
        return owned

    def _path(self, snapshot, binding):
        path = binding.target.path
        root = snapshot.project_root
        if not path.is_relative_to(root):
            raise DataValidationError("Target is outside the project root.")
        current = root
        for component in ("", *path.relative_to(root).parts):
            if component:
                current /= component
            if current.is_symlink():
                raise DataValidationError("Dataset paths cannot traverse symlinks.")
        return path

    def preflight(self, snapshot, dataset, *, write=False):
        binding = snapshot.require_binding(dataset, access="write" if write else "read")
        path = self._path(snapshot, binding)
        if write:
            if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
                raise DataValidationError("This atomic CSV publication requires the certified POSIX path.")
            if not path.parent.is_dir():
                raise DataValidationError("Create the output parent directory before running.")
            if path.exists():
                raise FileExistsError("The create target already exists.")
        elif not path.is_file():
            raise DataValidationError("Source dataset is not a regular file.")
        return path

    def _decode(self, value, data_type):
        pandas = _pandas()
        if value == _NULL:
            return pandas.NA
        if data_type == "string":
            return value[1:] if value.startswith("\\\\") else value
        if data_type == "boolean":
            if value not in ("true", "false"):
                raise DataValidationError("CSV boolean values must be true or false.")
            return value == "true"
        if data_type == "int64":
            if _INTEGER.fullmatch(value) is None:
                raise DataValidationError("Invalid CSV int64 value.")
            number = int(value)
            if not -(2**63) <= number < 2**63:
                raise DataValidationError("CSV int64 is outside its canonical range.")
            return number
        if _FLOAT.fullmatch(value) is None or not math.isfinite(float(value)):
            raise DataValidationError("Invalid CSV float64 value.")
        return float(value)

    def read(self, snapshot, dataset):
        pandas = _pandas()
        path = self.preflight(snapshot, dataset)
        definition = snapshot.definitions[dataset]
        try:
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            with os.fdopen(descriptor, "r", encoding="utf-8", newline="") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise DataValidationError("Source must be a regular file.")
                reader = csv.reader(stream, strict=True)
                names = next(reader, None)
                if not names or len(names) != len(set(names)) or any(not name.strip() for name in names):
                    raise DataValidationError("CSV requires a unique, non-empty header.")
                values = {name: [] for name in names}
                for row in reader:
                    if len(row) != len(names):
                        raise DataValidationError("CSV rows must match the header width.")
                    for name, value in zip(names, row):
                        column = definition.schema.columns.get(name)
                        values[name].append(self._decode(value, column.type if column else "string"))
            dtypes = {"int64": "Int64", "float64": "Float64", "boolean": "boolean", "string": "string"}
            frame = pandas.DataFrame(
                {
                    name: pandas.array(
                        items,
                        dtype=dtypes[definition.schema.columns[name].type]
                        if name in definition.schema.columns
                        else "string",
                    )
                    for name, items in values.items()
                }
            )
            return self.validate(frame, definition)
        except (OSError, UnicodeError, csv.Error, ValueError, TypeError, OverflowError):
            raise DataValidationError("Cannot decode the source CSV under its declared contract.") from None

    def _encode(self, value, data_type):
        if _pandas().isna(value):
            return _NULL
        if data_type == "string":
            return "\\" + value if value.startswith("\\") else value
        if data_type == "boolean":
            return "true" if value else "false"
        return str(value)

    def _cleanup(self, stage, identity):
        try:
            status = stage.lstat()
            if stat.S_ISREG(status.st_mode) and (status.st_dev, status.st_ino) == identity:
                stage.unlink()
                return False
        except FileNotFoundError:
            return False
        except OSError:
            pass
        return True

    def _sync_parent(self, parent):
        descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def publish(self, snapshot, dataset, frame):
        definition = snapshot.definitions[dataset]
        owned = self.validate(frame, definition)
        path = self.preflight(snapshot, dataset, write=True)
        descriptor, name = tempfile.mkstemp(prefix=".flint-", suffix=".tmp", dir=path.parent)
        stage = Path(name)
        status = os.fstat(descriptor)
        identity = (status.st_dev, status.st_ino)
        uncertain = False
        failure = None
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream, lineterminator="\n")
                writer.writerow(owned.columns)
                for row in owned.itertuples(index=False, name=None):
                    writer.writerow(
                        [
                            self._encode(
                                value,
                                definition.schema.columns[column].type
                                if column in definition.schema.columns
                                else "string",
                            )
                            for column, value in zip(owned.columns, row)
                        ]
                    )
                stream.flush()
                os.fsync(stream.fileno())
            self._path(snapshot, snapshot.bindings[dataset])
            try:
                os.link(stage, path)
            except FileExistsError:
                raise
            except OSError:
                uncertain = True
                raise
            try:
                self._sync_parent(path.parent)
            except OSError:
                uncertain = True
                raise
        except Exception as error:
            failure = error
        finally:
            pending = self._cleanup(stage, identity)
        if uncertain:
            raise PublicationUncertain(cleanup_pending=pending) from None
        if failure is not None:
            if isinstance(failure, FileExistsError):
                raise FileExistsError("The create target already exists.") from None
            raise DataValidationError("CSV preparation failed before publication.") from None
        return PublicationReceipt(cleanup_pending=pending)
