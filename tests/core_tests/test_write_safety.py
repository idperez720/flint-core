"""Regression tests for conditional writes that must fail before mutation."""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from flint_core.core.catalog import DataCatalog
from flint_core.core.exceptions import UnsupportedBackendError
from flint_core.core.io import DataSaver

pd = pytest.importorskip("pandas")
PandasEngine = pytest.importorskip("flint_core.pandas_core.engine").PandasEngine


@pytest.fixture
def catalog(tmp_path: Path) -> DataCatalog:
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "write-safety"\n', encoding="utf-8")
    catalog_path = tmp_path / "catalog.yml"
    catalog_path.write_text(
        yaml.safe_dump(
            {
                "customers": {
                    "engine": "pandas",
                    "format": "csv",
                    "storage_path": "data/customers.csv",
                },
                "spark_customers": {
                    "engine": "spark",
                    "format": "csv",
                    "storage_path": "data/customers.csv",
                },
            }
        ),
        encoding="utf-8",
    )
    target = tmp_path / "data" / "customers.csv"
    target.parent.mkdir()
    target.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    return DataCatalog(catalog_path=catalog_path)


@pytest.mark.parametrize("predicate", ["missing_column == 1", "id == 1"])
def test_conditional_csv_write_preserves_existing_data(catalog: DataCatalog, predicate: str) -> None:
    target = catalog.project_root / "data" / "customers.csv"
    original = target.read_bytes()

    with pytest.raises(UnsupportedBackendError, match="replace_where.*delta"):
        catalog.save(
            pd.DataFrame({"id": [1], "name": ["Updated"]}), "customers", mode="overwrite", replace_where=predicate
        )

    assert target.read_bytes() == original


@pytest.mark.parametrize("predicate", ["", " ", "\t\n", False, 0, []])
def test_invalid_predicates_cannot_turn_into_full_overwrites(catalog: DataCatalog, predicate: object) -> None:
    target = catalog.project_root / "data" / "customers.csv"
    original = target.read_bytes()

    with pytest.raises(ValueError, match="non-empty string"):
        DataSaver(catalog).save(pd.DataFrame({"id": [1]}), "customers", mode="overwrite", replace_where=predicate)

    assert target.read_bytes() == original


def test_unsupported_condition_is_rejected_before_engine_resolution(catalog: DataCatalog) -> None:
    with patch("flint_core.core.io.EngineRegistry.get_engine") as resolve_engine:
        with pytest.raises(UnsupportedBackendError, match="replace_where.*delta"):
            catalog.save(pd.DataFrame({"id": [1]}), "spark_customers", mode="overwrite", replace_where="id = 1")

    resolve_engine.assert_not_called()


@pytest.mark.parametrize(
    ("data_format", "target"),
    [
        ("csv", "data/customers.csv"),
        ("parquet", "s3://bucket/customers.parquet"),
        ("json", "gs://bucket/customers.json"),
        ("parquet", "abfss://container@account/customers.parquet"),
        ("sqlite", "customers"),
        ("iceberg", "warehouse.customers"),
    ],
)
def test_direct_engine_rejects_unsupported_conditional_writes(data_format: str, target: str) -> None:
    engine = PandasEngine()
    with patch.object(engine, "_resolve_format_handler") as resolve_handler:
        with pytest.raises(UnsupportedBackendError, match="replace_where.*delta"):
            engine.save(
                pd.DataFrame({"id": [1]}),
                target,
                data_format,
                [],
                mode="overwrite",
                metadata={"replace_where": "id == 1"},
            )

    resolve_handler.assert_not_called()


@pytest.mark.parametrize("mode", ["append", "error", "ignore", "unknown"])
def test_direct_engine_cannot_force_overwrite_for_delta(mode: str, tmp_path: Path) -> None:
    engine = PandasEngine()
    with patch.object(engine, "_resolve_format_handler") as resolve_handler:
        with pytest.raises(ValueError, match="mode"):
            engine.save(
                pd.DataFrame({"id": [1]}),
                str(tmp_path / "delta"),
                "delta",
                [],
                mode=mode,
                metadata={"replace_where": "id = 1"},
            )

    resolve_handler.assert_not_called()
    assert not (tmp_path / "delta").exists()


def test_rejected_conditional_write_does_not_create_directories(tmp_path: Path) -> None:
    target = tmp_path / "absent" / "customers.csv"

    with pytest.raises(UnsupportedBackendError, match="replace_where.*delta"):
        PandasEngine().save(
            pd.DataFrame({"id": [1]}),
            str(target),
            "csv",
            [],
            mode="overwrite",
            metadata={"replace_where": "id == 1"},
        )

    assert not target.parent.exists()


@pytest.fixture
def delta_catalog(tmp_path: Path) -> tuple[DataCatalog, Path]:
    deltalake = pytest.importorskip("deltalake")
    pyarrow = pytest.importorskip("pyarrow")
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "delta-safety"\n', encoding="utf-8")
    catalog_path = tmp_path / "catalog.yml"
    catalog_path.write_text(
        yaml.safe_dump(
            {
                "customers": {
                    "engine": "pandas",
                    "format": "delta",
                    "storage_path": "data/customers",
                }
            }
        ),
        encoding="utf-8",
    )
    target = tmp_path / "data" / "customers"
    deltalake.write_deltalake(
        str(target), pyarrow.Table.from_pandas(pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]}))
    )
    return DataCatalog(catalog_path=catalog_path), target


def test_native_delta_replacement_preserves_rows_outside_predicate(delta_catalog: tuple[DataCatalog, Path]) -> None:
    deltalake = pytest.importorskip("deltalake")
    catalog, target = delta_catalog

    catalog.save(
        pd.DataFrame({"id": [1], "name": ["Updated"]}),
        "customers",
        mode="overwrite",
        replace_where="id = 1",
    )

    actual = deltalake.DeltaTable(str(target)).to_pandas().sort_values("id").reset_index(drop=True)
    expected = pd.DataFrame({"id": [1, 2], "name": ["Updated", "Bob"]})
    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize(
    ("predicate", "input_id"),
    [("missing_column = 1", 1), ("id =", 1), ("id = 1", 3)],
)
def test_native_delta_failure_preserves_committed_version(
    delta_catalog: tuple[DataCatalog, Path], predicate: str, input_id: int
) -> None:
    deltalake = pytest.importorskip("deltalake")
    catalog, target = delta_catalog
    before = deltalake.DeltaTable(str(target))
    original = before.to_pandas().sort_values("id").reset_index(drop=True)
    version = before.version()

    with pytest.raises(deltalake.exceptions.DeltaError):
        catalog.save(
            pd.DataFrame({"id": [input_id], "name": ["Updated"]}),
            "customers",
            mode="overwrite",
            replace_where=predicate,
        )

    after = deltalake.DeltaTable(str(target))
    assert after.version() == version
    pd.testing.assert_frame_equal(after.to_pandas().sort_values("id").reset_index(drop=True), original)
