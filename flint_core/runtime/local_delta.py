"""Strict local Delta bindings with native snapshots and owned create publication."""

import json
import stat
from importlib import import_module
from pathlib import Path
from urllib.parse import unquote, urlparse

from flint_core.runtime.directory import DirectoryPublisher, dataset_path
from flint_core.runtime.errors import DataValidationError
from flint_core.runtime.frames import _pandas, validate_pandas


def _check_log(path):
    log = path / "_delta_log"
    if not log.is_dir() or log.is_symlink():
        raise DataValidationError("A local Delta transaction log is required.")
    for child in log.rglob("*"):
        status = child.lstat()
        if not (stat.S_ISDIR(status.st_mode) or stat.S_ISREG(status.st_mode)):
            raise DataValidationError("Delta log paths cannot contain symlinks or special files.")


def _check_files(root, locations):
    for location in locations:
        parsed = urlparse(location)
        if parsed.scheme not in ("", "file") or parsed.netloc or parsed.query or parsed.fragment:
            raise DataValidationError("Delta data files must be local to the bound table.")
        path = Path(unquote(parsed.path))
        if any(character in str(path) for character in ("%", "?", "#")):
            raise DataValidationError("Ambiguous URI characters are not supported in Delta file references.")
        if not path.is_absolute():
            path = root / path
        if ".." in path.parts or not path.is_relative_to(root):
            raise DataValidationError("Delta file references cannot escape the bound table.")
        current = root
        for component in path.relative_to(root).parts:
            current /= component
            if current.is_symlink():
                raise DataValidationError("Delta data files cannot traverse symlinks.")
        if not path.is_file():
            raise DataValidationError("A referenced Delta data file is unavailable.")


def _check_prepared(stage):
    _check_log(stage)
    with (stage / "_delta_log/00000000000000000000.json").open(encoding="utf-8") as stream:
        for line in stream:
            action = json.loads(line)
            if "add" not in action:
                continue
            relative = unquote(action["add"]["path"])
            parsed = urlparse(relative)
            if (
                parsed.scheme
                or parsed.netloc
                or Path(relative).is_absolute()
                or ".." in Path(relative).parts
                or "\\" in relative
            ):
                raise DataValidationError("Prepared Delta data references must remain relative after publication.")
            _check_files(stage, [str(stage / relative)])


class PandasDeltaAdapter:
    def __init__(self):
        self.publisher = DirectoryPublisher()

    def validate(self, frame, definition):
        return validate_pandas(frame, definition)

    def preflight(self, snapshot, dataset, *, write=False):
        try:
            import_module("deltalake")
            import_module("pyarrow")
        except ImportError:
            raise DataValidationError("Install the pandas and delta extras for local Pandas Delta execution.") from None
        if write:
            return self.publisher.preflight(snapshot, dataset)
        path = dataset_path(snapshot, snapshot.require_binding(dataset))
        _check_log(path)
        return path

    def read(self, snapshot, dataset):
        import deltalake
        import pyarrow

        path = self.preflight(snapshot, dataset)
        binding = snapshot.bindings[dataset]
        definition = snapshot.definitions[dataset]
        table = deltalake.DeltaTable(path.as_uri(), version=binding.options.get("version"))
        _check_files(path, table.file_uris())
        arrow = table.to_pyarrow_table()
        types = {
            "int64": (pyarrow.int64(),),
            "float64": (pyarrow.float64(),),
            "boolean": (pyarrow.bool_(),),
            "string": (pyarrow.string(), pyarrow.large_string()),
        }
        for column in arrow.schema:
            declared = definition.schema.columns.get(column.name)
            expected = declared.type if declared else "string"
            if column.type not in types[expected]:
                raise DataValidationError("Delta physical types do not match the logical contract.")
        pandas = _pandas()
        mapping = {
            pyarrow.int64(): pandas.Int64Dtype(),
            pyarrow.float64(): pandas.Float64Dtype(),
            pyarrow.bool_(): pandas.BooleanDtype(),
            pyarrow.string(): pandas.StringDtype(),
            pyarrow.large_string(): pandas.StringDtype(),
        }
        return self.validate(arrow.to_pandas(types_mapper=mapping.get), definition)

    def publish(self, snapshot, dataset, frame):
        import deltalake
        import pyarrow

        definition = snapshot.definitions[dataset]
        owned = self.validate(frame, definition)
        types = {
            "int64": pyarrow.int64(),
            "float64": pyarrow.float64(),
            "boolean": pyarrow.bool_(),
            "string": pyarrow.string(),
        }
        schema = pyarrow.schema(
            [
                pyarrow.field(
                    name,
                    types[definition.schema.columns[name].type]
                    if name in definition.schema.columns
                    else types["string"],
                    nullable=definition.schema.columns[name].nullable if name in definition.schema.columns else True,
                )
                for name in owned.columns
            ]
        )
        table = pyarrow.Table.from_pandas(owned, schema=schema, preserve_index=False, safe=True)

        def prepare(stage):
            deltalake.write_deltalake(stage.as_uri(), table, mode="error")
            if deltalake.DeltaTable(stage.as_uri()).version() != 0:
                raise DataValidationError("The staged Delta table must contain exactly its initial version.")
            _check_prepared(stage)

        return self.publisher.publish(snapshot, dataset, prepare)


class SparkDeltaAdapter:
    def __init__(self, frames):
        self.frames = frames
        self.publisher = DirectoryPublisher()
        self._checked = False

    def preflight(self, snapshot, dataset, *, write=False):
        if not self._checked:
            try:
                import_module("delta.tables")
                session = self.frames.session
                extensions = session.conf.get("spark.sql.extensions", "").split(",")
                catalog = session.conf.get("spark.sql.catalog.spark_catalog", "")
                if (
                    "io.delta.sql.DeltaSparkSessionExtension" not in extensions
                    or catalog != "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                ):
                    raise ValueError("Delta session configuration is required.")
                jvm = session.sparkContext._jvm
                loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
                jvm.java.lang.Class.forName("io.delta.tables.DeltaTable", False, loader).getMethods()
            except Exception:
                raise DataValidationError(
                    "The explicit Spark session needs a compatible, configured Delta integration."
                ) from None
            self._checked = True
        if write:
            return self.publisher.preflight(snapshot, dataset)
        path = dataset_path(snapshot, snapshot.require_binding(dataset))
        _check_log(path)
        return path

    def read(self, snapshot, dataset):
        from delta.tables import DeltaTable

        path = self.preflight(snapshot, dataset)
        version = snapshot.bindings[dataset].options.get("version")
        session = self.frames.session
        if version is None:
            version = DeltaTable.forPath(session, str(path)).history(1).select("version").first()[0]
        frame = session.read.format("delta").option("versionAsOf", str(version)).load(str(path))
        _check_files(path, frame.inputFiles())
        return self.frames.validate(frame, snapshot.definitions[dataset])

    def publish(self, snapshot, dataset, frame):
        from delta.tables import DeltaTable

        self.preflight(snapshot, dataset, write=True)
        owned = self.frames.validate(frame, snapshot.definitions[dataset])

        def prepare(stage):
            try:
                owned.write.format("delta").mode("error").save(str(stage))
            except Exception:
                raise DataValidationError(
                    "Spark staging failed; inspect the owned stage before cleanup.", cleanup_pending=True
                ) from None
            current = DeltaTable.forPath(self.frames.session, str(stage)).history(1).select("version").first()[0]
            if current != 0:
                raise DataValidationError("The staged Delta table must contain exactly its initial version.")
            _check_prepared(stage)

        return self.publisher.publish(snapshot, dataset, prepare)
