"""Behavioral regressions for explicit, consistent write modes."""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from flint_core.core.catalog import DataCatalog
from flint_core.core.exceptions import UnsupportedBackendError
from flint_core.core.pipeline import Node

pd = pytest.importorskip("pandas")
PandasEngine = pytest.importorskip("flint_core.pandas_core.engine").PandasEngine


@pytest.fixture
def mode_catalog(tmp_path: Path) -> DataCatalog:
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "write-modes"\n', encoding="utf-8")
    catalog_path = tmp_path / "catalog.yml"
    catalog_path.write_text(
        yaml.safe_dump(
            {
                "customers": {
                    "engine": "pandas",
                    "format": "csv",
                    "storage_path": "data/customers.csv",
                }
            }
        ),
        encoding="utf-8",
    )
    target = tmp_path / "data" / "customers.csv"
    target.parent.mkdir()
    target.write_text("id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
    return DataCatalog(catalog_path=catalog_path)


def test_csv_append_preserves_existing_rows_and_single_header(mode_catalog: DataCatalog) -> None:
    target = mode_catalog.project_root / "data" / "customers.csv"

    mode_catalog.save(pd.DataFrame({"id": [3], "name": ["Carol"]}), "customers", mode="append")
    mode_catalog.save(pd.DataFrame({"id": [4], "name": ["Dave"]}), "customers", mode="append")

    expected = pd.DataFrame({"id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Carol", "Dave"]})
    pd.testing.assert_frame_equal(pd.read_csv(target), expected)
    assert target.read_text(encoding="utf-8").count("id,name") == 1


def test_csv_append_handles_missing_final_newline(mode_catalog: DataCatalog) -> None:
    target = mode_catalog.project_root / "data" / "customers.csv"
    target.write_text("id,name\n1,Alice", encoding="utf-8")

    mode_catalog.save(pd.DataFrame({"id": [2], "name": ["Bob"]}), "customers", mode="append")

    assert pd.read_csv(target)["name"].tolist() == ["Alice", "Bob"]


def test_csv_append_rejects_incompatible_columns(mode_catalog: DataCatalog) -> None:
    target = mode_catalog.project_root / "data" / "customers.csv"
    original = target.read_bytes()

    with pytest.raises(ValueError, match="columns"):
        mode_catalog.save(pd.DataFrame({"name": ["Carol"], "id": [3]}), "customers", mode="append")

    assert target.read_bytes() == original


@pytest.mark.parametrize("data_format", ["parquet", "json", "orc"])
def test_unsupported_file_append_leaves_existing_bytes_unchanged(tmp_path: Path, data_format: str) -> None:
    target = tmp_path / f"customers.{data_format}"
    target.write_bytes(b"existing fixture")

    with pytest.raises(UnsupportedBackendError, match="append"):
        PandasEngine().save(pd.DataFrame({"id": [3]}), str(target), data_format, [], mode="append")

    assert target.read_bytes() == b"existing fixture"


@pytest.mark.parametrize("mode", ["unknown", "", None, False])
def test_invalid_mode_cannot_overwrite_existing_csv(tmp_path: Path, mode: object) -> None:
    target = tmp_path / "customers.csv"
    target.write_text("id\n1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="mode"):
        PandasEngine().save(pd.DataFrame({"id": [3]}), str(target), "csv", [], mode=mode)

    assert target.read_text(encoding="utf-8") == "id\n1\n"


@pytest.mark.parametrize("mode", ["error", "ignore", "append"])
def test_remote_files_reject_modes_without_remote_existence_support(mode: str) -> None:
    with patch("pandas.DataFrame.to_parquet") as writer:
        with pytest.raises(UnsupportedBackendError, match="mode"):
            PandasEngine().save(pd.DataFrame({"id": [3]}), "s3://bucket/customers.parquet", "parquet", [], mode=mode)
    writer.assert_not_called()


@pytest.mark.parametrize("option", ["mode", "if_exists", "append", "overwrite"])
def test_options_cannot_override_public_write_mode(tmp_path: Path, option: str) -> None:
    target = tmp_path / "customers.csv"
    target.write_text("id\n1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="options"):
        PandasEngine().save(
            pd.DataFrame({"id": [3]}),
            str(target),
            "csv",
            [],
            mode="overwrite",
            metadata={"options": {option: "append"}},
        )

    assert target.read_text(encoding="utf-8") == "id\n1\n"


@pytest.mark.parametrize("mode", ["overwrite", "append"])
def test_interrupted_file_write_preserves_destination(mode_catalog: DataCatalog, mode: str) -> None:
    target = mode_catalog.project_root / "data" / "customers.csv"
    original = target.read_bytes()

    def partial_write(handler, dataframe, path, options):
        Path(path).write_bytes(b"incomplete output")
        raise OSError("simulated full disk")

    with patch("flint_core.pandas_core.engine.CSVFormatHandler.write", new=partial_write):
        with pytest.raises(OSError, match="full disk"):
            mode_catalog.save(pd.DataFrame({"id": [3], "name": ["Carol"]}), "customers", mode=mode)

    assert target.read_bytes() == original
    assert list(target.parent.iterdir()) == [target]


@pytest.mark.parametrize("mode", ["error", "ignore"])
def test_creation_race_cannot_replace_another_writers_file(tmp_path: Path, mode: str) -> None:
    target = tmp_path / "customers.csv"

    def competing_write(handler, dataframe, path, options):
        Path(path).write_text("id\n3\n", encoding="utf-8")
        target.write_text("id\n99\n", encoding="utf-8")

    with patch("flint_core.pandas_core.engine.CSVFormatHandler.write", new=competing_write):
        if mode == "error":
            with pytest.raises(FileExistsError):
                PandasEngine().save(pd.DataFrame({"id": [3]}), str(target), "csv", [], mode=mode)
        else:
            PandasEngine().save(pd.DataFrame({"id": [3]}), str(target), "csv", [], mode=mode)

    assert target.read_text(encoding="utf-8") == "id\n99\n"
    assert list(tmp_path.iterdir()) == [target]


def test_node_default_never_overwrites_existing_output(mode_catalog: DataCatalog) -> None:
    target = mode_catalog.project_root / "data" / "customers.csv"
    original = target.read_bytes()
    node = Node("customers", lambda: pd.DataFrame({"id": [3], "name": ["Carol"]}), [], ["customers"])

    with pytest.raises(FileExistsError):
        node.run(catalog=mode_catalog)

    assert target.read_bytes() == original


def test_node_respects_explicit_output_mode(mode_catalog: DataCatalog) -> None:
    node = Node(
        "customers",
        lambda: pd.DataFrame({"id": [3], "name": ["Carol"]}),
        [],
        ["customers"],
        write_modes={"customers": "append"},
    )

    node.run(catalog=mode_catalog)

    actual = pd.read_csv(mode_catalog.project_root / "data" / "customers.csv")
    assert actual["id"].tolist() == [1, 2, 3]


@pytest.mark.parametrize("write_modes", [{"typo": "overwrite"}, {"customers": "unknown"}])
def test_node_rejects_invalid_output_policy(write_modes: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        Node("customers", lambda: None, [], ["customers"], write_modes=write_modes)


@pytest.mark.parametrize("mode", ["append", "overwrite", "error", "ignore"])
def test_delta_respects_public_mode(tmp_path: Path, mode: str) -> None:
    deltalake = pytest.importorskip("deltalake")
    pyarrow = pytest.importorskip("pyarrow")
    target = str(tmp_path / "delta")
    deltalake.write_deltalake(target, pyarrow.Table.from_pandas(pd.DataFrame({"id": [1, 2]})))

    if mode == "error":
        with pytest.raises((FileExistsError, deltalake.exceptions.DeltaError)):
            PandasEngine().save(pd.DataFrame({"id": [3]}), target, "delta", [], mode=mode)
    else:
        PandasEngine().save(pd.DataFrame({"id": [3]}), target, "delta", [], mode=mode)

    expected = {"append": [1, 2, 3], "overwrite": [3], "error": [1, 2], "ignore": [1, 2]}[mode]
    assert sorted(deltalake.DeltaTable(target).to_pandas()["id"].tolist()) == expected


@pytest.mark.parametrize("mode", ["append", "overwrite", "error"])
def test_sqlite_respects_public_mode(tmp_path: Path, mode: str) -> None:
    sqlalchemy = pytest.importorskip("sqlalchemy")
    url = f"sqlite:///{tmp_path / 'customers.db'}"
    connection = sqlalchemy.create_engine(url)
    pd.DataFrame({"id": [1, 2]}).to_sql("customers", connection, index=False)

    if mode == "error":
        with pytest.raises(ValueError, match="already exists"):
            PandasEngine().save(
                pd.DataFrame({"id": [3]}), "customers", "sqlite", [], mode=mode, metadata={"options": {"url": url}}
            )
    else:
        PandasEngine().save(
            pd.DataFrame({"id": [3]}), "customers", "sqlite", [], mode=mode, metadata={"options": {"url": url}}
        )

    expected = {"append": [1, 2, 3], "overwrite": [3], "error": [1, 2]}[mode]
    assert sorted(pd.read_sql_table("customers", connection)["id"].tolist()) == expected
    connection.dispose()


def test_csv_append_creates_missing_target(tmp_path: Path) -> None:
    target = tmp_path / "data" / "customers.csv"

    PandasEngine().save(pd.DataFrame({"id": [1]}), str(target), "csv", [], mode="append")
    PandasEngine().save(pd.DataFrame({"id": [2]}), str(target), "csv", [], mode="append")

    assert pd.read_csv(target)["id"].tolist() == [1, 2]


@pytest.mark.parametrize(
    "options",
    [{"encoding": "utf-16"}, {"header": False}, {"index": True}, {"compression": "gzip"}, {"columns": ["id"]}],
)
def test_unsupported_csv_append_options_preserve_target(mode_catalog: DataCatalog, options: dict) -> None:
    target = mode_catalog.project_root / "data" / "customers.csv"
    original = target.read_bytes()

    with pytest.raises(UnsupportedBackendError):
        mode_catalog.save(pd.DataFrame({"id": [3], "name": ["Carol"]}), "customers", mode="append", options=options)

    assert target.read_bytes() == original


def test_csv_append_preserves_custom_separator_and_quoted_fields(mode_catalog: DataCatalog) -> None:
    target = mode_catalog.project_root / "data" / "customers.csv"
    target.write_text('id;name\n1;"Alice; Smith"', encoding="utf-8")

    mode_catalog.save(
        pd.DataFrame({"id": [2], "name": ["Bob; Jones"]}), "customers", mode="append", options={"sep": ";"}
    )

    actual = pd.read_csv(target, sep=";")
    assert actual["name"].tolist() == ["Alice; Smith", "Bob; Jones"]


@pytest.mark.parametrize("data_format", ["parquet", "json", "orc"])
def test_local_file_modes_round_trip(tmp_path: Path, data_format: str) -> None:
    if data_format != "json":
        pytest.importorskip("pyarrow")
    target = str(tmp_path / f"customers.{data_format}")
    engine = PandasEngine()
    engine.save(pd.DataFrame({"id": [1, 2]}), target, data_format, [], mode="error")

    with pytest.raises(FileExistsError):
        engine.save(pd.DataFrame({"id": [3]}), target, data_format, [], mode="error")
    engine.save(pd.DataFrame({"id": [3]}), target, data_format, [], mode="ignore")
    assert engine.load(target, data_format, [])["id"].tolist() == [1, 2]

    engine.save(pd.DataFrame({"id": [3]}), target, data_format, [], mode="overwrite")
    assert engine.load(target, data_format, [])["id"].tolist() == [3]


@pytest.mark.parametrize(
    ("data_format", "mode"), [("iceberg", "overwrite"), ("snowflake", "error"), ("sqlite", "ignore")]
)
def test_unsupported_native_modes_fail_before_handler_io(data_format: str, mode: str) -> None:
    engine = PandasEngine()
    handler = engine._resolve_format_handler(data_format)
    with (
        patch.object(engine, "_resolve_format_handler", return_value=handler),
        patch.object(type(handler), "write") as writer,
    ):
        with pytest.raises(UnsupportedBackendError, match="mode"):
            engine.save(pd.DataFrame({"id": [3]}), "customers", data_format, [], mode=mode)
    writer.assert_not_called()


def test_node_keeps_modes_specific_to_each_output(mode_catalog: DataCatalog) -> None:
    target = mode_catalog.project_root / "catalog.yml"
    definitions = yaml.safe_load(target.read_text(encoding="utf-8"))
    definitions["archive"] = {"engine": "pandas", "format": "csv", "storage_path": "data/archive.csv"}
    target.write_text(yaml.safe_dump(definitions), encoding="utf-8")
    mode_catalog.reload_catalog(target)
    archive = mode_catalog.project_root / "data" / "archive.csv"
    archive.write_text("id\n99\n", encoding="utf-8")
    node = Node(
        "customers",
        lambda: {"customers": pd.DataFrame({"id": [3], "name": ["Carol"]}), "archive": pd.DataFrame({"id": [4]})},
        [],
        ["customers", "archive"],
        write_modes={"customers": "append", "archive": "overwrite"},
    )

    node.run(catalog=mode_catalog)

    assert pd.read_csv(mode_catalog.project_root / "data" / "customers.csv")["id"].tolist() == [1, 2, 3]
    assert pd.read_csv(archive)["id"].tolist() == [4]
