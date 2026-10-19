"""Real local Spark I/O checks for the legacy catalog, without mocked writers."""

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from flint_core.core.catalog import DataCatalog
from flint_core.core.catalog.models import ColumnDefinition
from flint_core.core.exceptions import ColumnValidationError, UnsupportedBackendError
from flint_core.core.io import DataLoader, DataSaver

pytest.importorskip("pyspark")
from pyspark.errors import AnalysisException

from flint_core.spark_core.engine import SparkEngine


@pytest.fixture(params=["csv", "parquet", "json", "orc"])
def spark_catalog(tmp_path: Path, request: pytest.FixtureRequest) -> tuple[DataCatalog, Path]:
    catalog_path = tmp_path / "conf" / "catalog"
    catalog_path.mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "spark-local-test"\n', encoding="utf-8")
    dataset = {
        "engine": "spark",
        "format": request.param,
        "storage_path": "data/output",
        "columns": [{"name": "id", "type": "long"}, {"name": "value", "type": "string"}],
    }
    if request.param == "csv":
        dataset["options"] = {"header": True, "delimiter": "|"}
    (catalog_path / "datasets.yaml").write_text(yaml.safe_dump({"records": dataset}), encoding="utf-8")
    return DataCatalog(catalog_path=catalog_path, env="dev"), tmp_path / "data" / "output"


def snapshot(directory: Path) -> dict[str, bytes]:
    return {str(path.relative_to(directory)): path.read_bytes() for path in directory.rglob("*") if path.is_file()}


@pytest.mark.parametrize("mode", ["error", "ignore", "append", "overwrite"])
def test_real_spark_catalog_write_modes(spark_catalog: tuple[DataCatalog, Path], spark_session: Any, mode: str) -> None:
    catalog, target = spark_catalog
    saver = DataSaver(catalog)
    original = spark_session.createDataFrame([(1, "original", 99), (2, None, 98)], "id long, value string, extra long")
    incoming = spark_session.createDataFrame([(3, "new", 77)], original.schema)

    saver.save(original, "records", mode=mode, spark=spark_session)
    before = snapshot(target)
    assert before

    if mode == "error":
        with pytest.raises(AnalysisException, match="PATH_ALREADY_EXISTS"):
            saver.save(incoming, "records", mode=mode, spark=spark_session)
    else:
        saver.save(incoming, "records", mode=mode, spark=spark_session)

    if mode in ("error", "ignore"):
        assert snapshot(target) == before

    loaded = DataLoader(catalog).load("records", spark=spark_session)
    assert loaded.columns == ["id", "value"]
    assert loaded.schema["id"].dataType.typeName() == "long"
    expected = {
        "error": [(1, "original"), (2, None)],
        "ignore": [(1, "original"), (2, None)],
        "append": [(1, "original"), (2, None), (3, "new")],
        "overwrite": [(3, "new")],
    }
    assert sorted(tuple(row) for row in loaded.collect()) == expected[mode]


@pytest.mark.parametrize(
    "invalid_write", ["unknown_mode", "reserved_option", "conditional_overwrite", "missing_column"]
)
def test_real_spark_rejected_writes_preserve_files(
    spark_catalog: tuple[DataCatalog, Path], spark_session: Any, invalid_write: str
) -> None:
    catalog, target = spark_catalog
    saver = DataSaver(catalog)
    original = spark_session.createDataFrame([(1, "original")], "id long, value string")
    saver.save(original, "records", spark=spark_session)
    before = snapshot(target)
    assert before

    incoming = spark_session.createDataFrame([(2, "new")], original.schema)
    arguments: dict[str, Any] = {"mode": "overwrite", "spark": spark_session}
    if invalid_write == "unknown_mode":
        arguments["mode"] = "invalid"
        error, message = ValueError, "Write mode"
    elif invalid_write == "reserved_option":
        arguments["options"] = {"MoDe": "overwrite"}
        error, message = ValueError, "override"
    elif invalid_write == "conditional_overwrite":
        arguments["replace_where"] = "id = 1"
        error, message = UnsupportedBackendError, "replace_where.*delta"
    else:
        incoming = incoming.select("id")
        error, message = ColumnValidationError, "Missing expected catalog columns"

    with pytest.raises(error, match=message):
        saver.save(incoming, "records", **arguments)

    assert snapshot(target) == before
    assert [tuple(row) for row in DataLoader(catalog).load("records", spark=spark_session).collect()] == [
        (1, "original")
    ]


@pytest.mark.parametrize("data_format", ["csv", "parquet", "json", "orc"])
def test_real_spark_typed_nullable_round_trip(tmp_path: Path, spark_session: Any, data_format: str) -> None:
    columns = [
        ColumnDefinition("id", "long"),
        ColumnDefinition("active", "boolean"),
        ColumnDefinition("score", "double"),
        ColumnDefinition("amount", "decimal(10,2)"),
        ColumnDefinition("day", "date"),
        ColumnDefinition("instant", "timestamp"),
    ]
    expected = [
        (1, True, 1.5, Decimal("99.99"), date(2026, 10, 18), datetime(2026, 10, 18, 12)),
        (2, None, None, None, None, None),
    ]
    dataframe = spark_session.createDataFrame(
        expected, "id long, active boolean, score double, amount decimal(10,2), day date, instant timestamp"
    )
    metadata = {"options": {"header": True}} if data_format == "csv" else {}
    path = str(tmp_path / "typed")
    engine = SparkEngine()

    engine.save(dataframe, path, data_format, columns, metadata=metadata, spark=spark_session)
    loaded = engine.load(path, data_format, columns, metadata=metadata, spark=spark_session)

    assert loaded.schema == dataframe.schema
    assert sorted(tuple(row) for row in loaded.collect()) == expected
