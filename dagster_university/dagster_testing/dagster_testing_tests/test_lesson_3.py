from pathlib import Path  # noqa: F401

import dagster as dg
import pytest
import yaml  # noqa: F401
from dagster._core.errors import DagsterTypeCheckDidNotPass  # noqa: F401
from dagster_testing.assets import lesson_3


@pytest.fixture()
def config_file():
    file_path = Path(__file__).absolute().parent / "data/test.csv"
    return lesson_3.FilepathConfig(path=file_path.as_posix())


@pytest.fixture()
def file_output():
    return [
        {
            "City": "New York",
            "Population": "8804190",
        },
        {
            "City": "Buffalo",
            "Population": "278349",
        },
        {
            "City": "Yonkers",
            "Population": "211569",
        },
    ]


@pytest.fixture()
def file_example_output():
    return [
        {
            "City": "Example 1",
            "Population": "4500000",
        },
        {
            "City": "Example 2",
            "Population": "3000000",
        },
        {
            "City": "Example 3",
            "Population": "1000000",
        },
    ]


@pytest.fixture()
def file_population():
    return 9294108


def test_state_population_file(file_output):
    assert lesson_3.state_population_file() == file_output


def test_total_population(file_output, file_population):
    assert lesson_3.total_population(file_output) == file_population


def test_func_wrong_type():
    assert lesson_3.func_wrong_type() == 2


def test_wrong_type_annotation_error():
    with pytest.raises(DagsterTypeCheckDidNotPass):
        assert lesson_3.func_wrong_type_annotation() == 2


def test_assets(file_output, file_population):
    _assets = [lesson_3.state_population_file, lesson_3.total_population]
    result = dg.materialize(_assets)
    assert result.success

    state_population = file_output
    assert result.output_for_node("state_population_file") == state_population
    assert result.output_for_node("total_population") == file_population


def test_state_population_file_config(config_file, file_example_output):
    assert lesson_3.state_population_file_config(config_file) == file_example_output


def test_assets_config(config_file, file_example_output):
    _assets = [lesson_3.state_population_file_config, lesson_3.total_population_config]
    result = dg.materialize(
        assets=_assets,
        run_config=dg.RunConfig({"state_population_file_config": config_file}),
    )
    assert result.success

    assert result.output_for_node("state_population_file_config") == file_example_output
    assert result.output_for_node("total_population_config") == 8500000


def test_assets_config_yaml(file_example_output):
    _assets = [lesson_3.state_population_file_config, lesson_3.total_population_config]
    result = dg.materialize(
        assets=_assets,
        run_config=yaml.safe_load(
            (Path(__file__).absolute().parent / "configs/lesson_3.yaml").open()
        ),
    )
    assert result.success

    assert result.output_for_node("state_population_file_config") == file_example_output
    assert result.output_for_node("total_population_config") == 8500000


def test_state_population_file_logging(file_output):
    context = dg.build_asset_context()
    result = lesson_3.state_population_file_logging(context)
    assert result == file_output


def test_assets_context():
    pass


def test_partition_asset_number():
    pass


def test_assets_partition():
    pass
