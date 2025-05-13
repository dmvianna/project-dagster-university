from unittest.mock import Mock, patch  # noqa: F401

import dagster as dg  # noqa: F401
import pytest
from dagster_testing.assets import lesson_4  # noqa: F401


@pytest.fixture
def example_response():
    return {
        "cities": [
            {
                "city_name": "New York",
                "city_population": 8804190,
            },
            {
                "city_name": "Buffalo",
                "city_population": 278349,
            },
        ],
    }


@pytest.fixture
def api_output():
    return [
        {
            "city": "New York",
            "population": 8804190,
        },
        {
            "city": "Buffalo",
            "population": 278349,
        },
    ]


@pytest.fixture
def fake_city():
    return {
        "city": "Fakestown",
        "population": 42,
    }


@patch("requests.get")
def test_state_population_api(mock_get, example_response):
    mock_response = Mock()
    mock_response.json.return_value = example_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = lesson_4.state_population_api()

    assert len(result) == 2
    assert result[0] == {
        "city": "New York",
        "population": 8804190,
    }
    mock_get.assert_called_once_with(lesson_4.API_URL, params={"state": "ny"})


def test_state_population_api_resource_mock():
    pass


def test_state_population_api_assets():
    pass


def test_state_population_api_assets_config():
    pass


def test_state_population_api_mocked_resource():
    pass


def test_state_population_api_assets_mocked_resource():
    pass
