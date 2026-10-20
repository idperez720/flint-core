"""Opt-in local Delta transaction checks using real Spark and delta-rs readers."""

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from flint_core.core.catalog import DataCatalog
from flint_core.core.exceptions import ColumnValidationError

pytest.importorskip("pyspark")
from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException, IllegalArgumentException, ParseException


@pytest.fixture
def delta_catalog(tmp_path: Path, spark_delta_session: Any) -> tuple[DataCatalog, Path, Any]:
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "spark-delta-test"\n', encoding="utf-8")
    manifest = tmp_path / "catalog.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "events": {
                    "engine": "spark",
                    "format": "delta",
                    "storage_path": "data/events",
                    "columns": [{"name": "id", "type": "long"}, {"name": "value", "type": "string"}],
                }
            }
        ),
        encoding="utf-8",
    )
    return DataCatalog(manifest, env="dev"), tmp_path / "data" / "events", spark_delta_session


def frame(session: Any, rows: list[tuple[int, str]]) -> Any:
    return session.createDataFrame(rows, "id long, value string")


def rows(catalog: DataCatalog, session: Any, **arguments: Any) -> list[tuple[int, str]]:
    return sorted(tuple(row) for row in catalog.load("events", spark=session, **arguments).collect())


def committed_log(target: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in (target / "_delta_log").glob("[0-9]*.json")}


def version(session: Any, target: Path) -> int:
    from delta.tables import DeltaTable

    return DeltaTable.forPath(session, str(target)).history(1).first()["version"]


@pytest.mark.parametrize("mode", ["error", "ignore", "append", "overwrite"])
def test_spark_delta_modes_on_absent_and_existing_table(
    delta_catalog: tuple[DataCatalog, Path, Any], mode: str
) -> None:
    catalog, target, session = delta_catalog
    catalog.save(frame(session, [(1, "first"), (2, "keep")]), "events", mode=mode, spark=session)
    assert version(session, target) == 0
    before = committed_log(target)

    if mode == "error":
        with pytest.raises(AnalysisException):
            catalog.save(frame(session, [(3, "new")]), "events", mode=mode, spark=session)
    else:
        catalog.save(frame(session, [(3, "new")]), "events", mode=mode, spark=session)

    expected = {
        "error": [(1, "first"), (2, "keep")],
        "ignore": [(1, "first"), (2, "keep")],
        "append": [(1, "first"), (2, "keep"), (3, "new")],
        "overwrite": [(3, "new")],
    }
    assert rows(catalog, session) == expected[mode]
    assert version(session, target) == (0 if mode in ("error", "ignore") else 1)
    if mode in ("error", "ignore"):
        assert committed_log(target) == before
    assert rows(catalog, session, version=0) == [(1, "first"), (2, "keep")]


def test_spark_delta_conditional_overwrite_preserves_other_rows(delta_catalog: tuple[DataCatalog, Path, Any]) -> None:
    catalog, target, session = delta_catalog
    catalog.save(frame(session, [(1, "first"), (2, "keep")]), "events", spark=session)

    catalog.save(frame(session, [(1, "updated")]), "events", mode="overwrite", replace_where="id = 1", spark=session)

    assert rows(catalog, session) == [(1, "updated"), (2, "keep")]
    assert version(session, target) == 1
    assert rows(catalog, session, version=0) == [(1, "first"), (2, "keep")]


@pytest.mark.parametrize(
    ("predicate", "input_id", "message"),
    [
        ("missing_column = 1", 1, "UNRESOLVED_COLUMN"),
        ("id =", 1, "DELTA_FAILED_RECOGNIZE_PREDICATE"),
        ("id = 1", 3, "(?i)replacewhere|replace_where|constraint"),
    ],
)
def test_spark_delta_invalid_predicate_preserves_committed_snapshot(
    delta_catalog: tuple[DataCatalog, Path, Any], predicate: str, input_id: int, message: str
) -> None:
    catalog, target, session = delta_catalog
    catalog.save(frame(session, [(1, "first"), (2, "keep")]), "events", spark=session)
    before = committed_log(target)

    with pytest.raises((AnalysisException, ParseException, Py4JJavaError), match=message):
        catalog.save(
            frame(session, [(input_id, "updated")]), "events", mode="overwrite", replace_where=predicate, spark=session
        )

    assert committed_log(target) == before
    assert version(session, target) == 0
    assert rows(catalog, session) == [(1, "first"), (2, "keep")]


def test_spark_delta_incompatible_schema_does_not_commit(delta_catalog: tuple[DataCatalog, Path, Any]) -> None:
    catalog, target, session = delta_catalog
    catalog.save(frame(session, [(1, "first")]), "events", spark=session)
    before = committed_log(target)
    incoming = session.createDataFrame([("not-an-integer", "new")], "id string, value string")

    with pytest.raises(AnalysisException, match="(?i)schema|merge"):
        catalog.save(incoming, "events", mode="append", spark=session)

    assert committed_log(target) == before
    assert version(session, target) == 0
    assert rows(catalog, session) == [(1, "first")]


@pytest.mark.parametrize("invalid_write", ["mode", "reserved_option", "missing_column"])
def test_spark_delta_invalid_controls_do_not_commit(
    delta_catalog: tuple[DataCatalog, Path, Any], invalid_write: str
) -> None:
    catalog, target, session = delta_catalog
    catalog.save(frame(session, [(1, "first")]), "events", spark=session)
    before = committed_log(target)
    incoming = frame(session, [(2, "new")])
    arguments: dict[str, Any] = {"mode": "overwrite", "spark": session}
    if invalid_write == "mode":
        arguments["mode"] = "invalid"
        error, message = ValueError, "Write mode"
    elif invalid_write == "reserved_option":
        arguments["options"] = {"replaceWhere": "id = 1"}
        error, message = ValueError, "override"
    else:
        incoming = incoming.select("id")
        error, message = ColumnValidationError, "Missing expected catalog columns"

    with pytest.raises(error, match=message):
        catalog.save(incoming, "events", **arguments)

    assert committed_log(target) == before
    assert version(session, target) == 0
    assert rows(catalog, session) == [(1, "first")]


@pytest.fixture
def delta_snapshots(delta_catalog: tuple[DataCatalog, Path, Any]) -> tuple[DataCatalog, Path, Any, str]:
    from delta.tables import DeltaTable

    catalog, target, session = delta_catalog
    catalog.save(frame(session, [(1, "first")]), "events", spark=session)
    timestamp = DeltaTable.forPath(session, str(target)).history(1).selectExpr("cast(timestamp as string)").first()[0]
    catalog.save(frame(session, [(2, "second")]), "events", mode="append", spark=session)
    return catalog, target, session, timestamp


@pytest.mark.parametrize("requested_version", [0, "0"])
def test_spark_delta_version_zero_is_historical(
    delta_snapshots: tuple[DataCatalog, Path, Any, str], requested_version: int | str
) -> None:
    catalog, target, session, _ = delta_snapshots
    before = committed_log(target)
    assert rows(catalog, session) == [(1, "first"), (2, "second")]
    assert rows(catalog, session, version=requested_version) == [(1, "first")]
    assert committed_log(target) == before


def test_spark_delta_timestamp_is_historical(delta_snapshots: tuple[DataCatalog, Path, Any, str]) -> None:
    catalog, target, session, timestamp = delta_snapshots
    before = committed_log(target)
    assert rows(catalog, session, as_of=timestamp) == [(1, "first")]
    assert committed_log(target) == before


@pytest.mark.parametrize("invalid_read", ["version", "timestamp", "conflict"])
def test_spark_delta_snapshot_errors_do_not_fall_back_to_latest(
    delta_snapshots: tuple[DataCatalog, Path, Any, str], invalid_read: str
) -> None:
    catalog, target, session, timestamp = delta_snapshots
    before = committed_log(target)
    arguments = {"version": 999} if invalid_read == "version" else {"as_of": "not-a-timestamp"}
    if invalid_read == "conflict":
        arguments = {"version": 0, "as_of": timestamp}

    with pytest.raises(
        (AnalysisException, IllegalArgumentException, ValueError), match="(?i)version|timestamp|time travel"
    ):
        rows(catalog, session, **arguments)

    assert committed_log(target) == before
    assert version(session, target) == 1
    assert rows(catalog, session) == [(1, "first"), (2, "second")]


@pytest.mark.parametrize("writer", ["spark", "pandas"])
def test_spark_delta_and_delta_rs_basic_interoperability(
    delta_catalog: tuple[DataCatalog, Path, Any], writer: str
) -> None:
    pandas = pytest.importorskip("pandas")
    deltalake = pytest.importorskip("deltalake")
    from flint_core.pandas_core.engine import PandasEngine

    catalog, target, session = delta_catalog
    if writer == "spark":
        catalog.save(frame(session, [(1, "first")]), "events", spark=session)
        actual = deltalake.DeltaTable(target).to_pandas()
        assert actual.to_dict("list") == {"id": [1], "value": ["first"]}
    else:
        PandasEngine().save(pandas.DataFrame({"id": [1], "value": ["first"]}), str(target), "delta", [])
        assert rows(catalog, session) == [(1, "first")]


def test_spark_delta_nullable_typed_round_trip(tmp_path: Path, spark_delta_session: Any) -> None:
    from flint_core.spark_core.engine import SparkEngine

    expected = [
        (1, True, 1.5, Decimal("99.99"), date(2026, 10, 18), datetime(2026, 10, 18, 12)),
        (2, None, None, None, None, None),
    ]
    dataframe = spark_delta_session.createDataFrame(
        expected, "id long, active boolean, score double, amount decimal(10,2), day date, instant timestamp"
    )
    path = str(tmp_path / "typed")
    engine = SparkEngine()
    engine.save(dataframe, path, "delta", [], spark=spark_delta_session)
    loaded = engine.load(path, "delta", [], spark=spark_delta_session)
    assert loaded.schema == dataframe.schema
    assert sorted(tuple(row) for row in loaded.collect()) == expected


def test_spark_delta_missing_read_does_not_create_table(delta_catalog: tuple[DataCatalog, Path, Any]) -> None:
    catalog, target, session = delta_catalog
    with pytest.raises(AnalysisException, match="(?i)not exist|not found"):
        rows(catalog, session)
    assert not target.exists()
