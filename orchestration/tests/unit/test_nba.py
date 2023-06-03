import pytest
import pytest_mock
import datetime
import logging
import typing
from orchestration.tasks.nba import functions as nba_func
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

# Tests that static data is fetched successfully from NBA API, loaded into dataframes, and saved to S3.
def test_data_fetch_success(mocker: pytest_mock.MockerFixture) -> None:
    """
    Tests that static data is fetched successfully from NBA API, loaded into dataframes, and saved to S3.
    """
    # Mock the API calls to return sample data
    mocker.patch('nba_api.stats.static.players.get_players', return_value=[{'id': 1, 'full_name': 'Player 1'}, {'id': 2, 'full_name': 'Player 2'}])
    mocker.patch('nba_api.stats.static.teams.get_teams', return_value=[{'id': 1, 'full_name': 'Team 1'}, {'id': 2, 'full_name': 'Team 2'}])

    # Mock the S3Hook to prevent actual file writes
    mock_s3_load_file = mocker.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file', return_value=None)

    nba_func.static_data_fetch()

    # Assert that the dataframes were created and saved to parquet files
    assert pd.read_parquet('players.parquet').equals(pd.DataFrame([{'id': 1, 'full_name': 'Player 1'}, {'id': 2, 'full_name': 'Player 2'}]))
    assert pd.read_parquet('teams.parquet').equals(pd.DataFrame([{'id': 1, 'full_name': 'Team 1'}, {'id': 2, 'full_name': 'Team 2'}]))

    # Assert that the files were saved to S3
    mock_s3_load_file.assert_any_call(filename='players.parquet', key='bronze/nba/static/players.parquet', bucket_name='sports-lakehouse', replace=True, gzip=True)
    mock_s3_load_file.assert_any_call(filename='teams.parquet', key='bronze/nba/static/teams.parquet', bucket_name='sports-lakehouse', replace=True, gzip=True)

# Tests that the function handles NBA API being down or unavailable.
def test_data_fetch_failure(mocker: pytest_mock.MockerFixture) -> None:
    """
    Tests that the function handles NBA API being down or unavailable.
    """
    # Call the function and assert that it raises an exception
    with pytest.raises(Exception):
        # Mock the API calls to raise an exception
        with mocker.patch('nba_api.stats.static.players.get_players', side_effect=Exception('API error')):
            with mocker.patch('nba_api.stats.static.teams.get_teams', side_effect=Exception('API error')):
                nba_func.static_data_fetch()

# Tests that the function generates logging messages at appropriate times.
def test_logging_messages(caplog: pytest.LogCaptureFixture, mocker: pytest_mock.MockerFixture) -> None:
    """
    Tests that the function generates logging messages at appropriate times.
    """
    # Mock the API calls to return sample data
    with mocker.patch('nba_api.stats.static.players.get_players', return_value=[{'id': 1, 'full_name': 'Player 1'}, {'id': 2, 'full_name': 'Player 2'}]):
        with mocker.patch('nba_api.stats.static.teams.get_teams', return_value=[{'id': 1, 'full_name': 'Team 1'}, {'id': 2, 'full_name': 'Team 2'}]):
            # Mock the S3Hook to prevent actual file writes
            s3_hook_mock = mocker.Mock(spec=S3Hook)
            with mocker.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file', return_value=None):
                # Call the function
                nba_func.static_data_fetch()

    # Assert that the expected log messages were generated
    assert 'Fetching static data...' in caplog.text
    assert 'Static data fetched.' in caplog.text
    assert 'Loaded dataframes... beginning to write to Parquet.' in caplog.text
    assert 'Wrote players.parquet.' in caplog.text
    assert 'Wrote teams.parquet.' in caplog.text
    assert 'Finished fetching static data.' in caplog.text
    assert 'Initializing S3Hook...' in caplog.text
    assert 'Saved players.parquet to S3.' in caplog.text
    assert 'Saved teams.parquet to S3.' in caplog.text
    assert 'Finished task.' in caplog.text