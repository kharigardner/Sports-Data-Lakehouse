import pytest
import pytest_mock
import datetime
import logging
import typing
from orchestration.tasks.nfl import functions as nfl_func
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

#Tests the successful import and transformation of static NFL data from the API to parquet files and uploading them to S3.
def test_happy_path_data_import(mocker: pytest_mock.MockerFixture) -> None:
    mock_import_officials = mocker.patch('nfl_data_py.import_officials')
    mock_import_officials.return_value = pd.DataFrame({'name': ['John Doe'], 'position': ['Referee']})

    mock_import_ids = mocker.patch('nfl_data_py.import_ids')
    mock_import_ids.return_value = pd.DataFrame({'id': [1], 'name': ['John Doe']})

    mock_import_contracts = mocker.patch('nfl_data_py.import_contracts')
    mock_import_contracts.return_value = pd.DataFrame({'player_id': [1], 'team_id': [1], 'salary': [1000000]})

    mock_import_draft_picks = mocker.patch('nfl_data_py.import_draft_picks')
    mock_import_draft_picks.return_value = pd.DataFrame({'year': [2021], 'round': [1], 'pick': [1], 'team_id': [1]})

    mock_import_draft_values = mocker.patch('nfl_data_py.import_draft_values')
    mock_import_draft_values.return_value = pd.DataFrame({'year': [2021], 'round': [1], 'pick': [1], 'value': [1000]})

    mock_import_combine_data = mocker.patch('nfl_data_py.import_combine_data')
    mock_import_combine_data.return_value = pd.DataFrame({'player_id': [1], 'year': [2021], 'position': ['QB'], 'height_inches': [72]})

    mock_import_team_desc = mocker.patch('nfl_data_py.import_team_desc')
    mock_import_team_desc.return_value = pd.DataFrame({'team_id': [1], 'name': ['New York Giants'], 'city': ['New York']})

    mock_to_parquet = mocker.patch('pandas.DataFrame.to_parquet')

    with mocker.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file', return_value=None):
        nfl_func.static_data_fetch()

    assert mock_import_officials.called
    assert mock_import_ids.called
    assert mock_import_contracts.called
    assert mock_import_draft_picks.called
    assert mock_import_draft_values.called
    assert mock_import_combine_data.called
    assert mock_import_team_desc.called

    assert mock_to_parquet.call_count == 7

# Tests that all parquet files are successfully loaded to S3.
def test_happy_path_s3_load(mocker: pytest_mock.MockerFixture) -> None:
    
    mock_s3_load_file = mocker.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file', return_value=None)

    nfl_func.static_data_fetch()

    assert mock_s3_load_file.call_count == 7


# Tests that the function saves parquet files with correct column names and data types.
def test_parquet_file_data(mocker: pytest_mock.MockerFixture) -> None:
    mock_import_officials = mocker.patch('nfl_data_py.import_officials')
    mock_import_officials.return_value = pd.DataFrame({'name': ['John Doe'], 'position': ['Referee']})

    mock_import_ids = mocker.patch('nfl_data_py.import_ids')
    mock_import_ids.return_value = pd.DataFrame({'id': [1], 'name': ['John Doe']})

    mock_import_contracts = mocker.patch('nfl_data_py.import_contracts')
    mock_import_contracts.return_value = pd.DataFrame({'player_id': [1], 'team_id': [1], 'salary': [1000000]})

    mock_import_draft_picks = mocker.patch('nfl_data_py.import_draft_picks')
    mock_import_draft_picks.return_value = pd.DataFrame({'year': [2021], 'round': [1], 'pick': [1], 'team_id': [1]})

    mock_import_draft_values = mocker.patch('nfl_data_py.import_draft_values')
    mock_import_draft_values.return_value = pd.DataFrame({'year': [2021], 'round': [1], 'pick': [1], 'value': [1000]})

    mock_import_combine_data = mocker.patch('nfl_data_py.import_combine_data')
    mock_import_combine_data.return_value = pd.DataFrame({'player_id': [1], 'year': [2021], 'position': ['QB'], 'height_inches': [72]})

    mock_import_team_desc = mocker.patch('nfl_data_py.import_team_desc')
    mock_import_team_desc.return_value = pd.DataFrame({'team_id': [1], 'name': ['New York Giants'], 'city': ['New York']})

    mock_to_parquet = mocker.patch('pandas.DataFrame.to_parquet')

    with mocker.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file', return_value=None):
        nfl_func.static_data_fetch()

    assert mock_to_parquet.call_count == 7

    mock_to_parquet.assert_any_call('officials.parquet', index=False)
    mock_to_parquet.assert_any_call('mapping_ids.parquet', index=False)
    mock_to_parquet.assert_any_call('contracts.parquet', index=False)
    mock_to_parquet.assert_any_call('draft_picks.parquet', index=False)
    mock_to_parquet.assert_any_call('draft_values.parquet', index=False)
    mock_to_parquet.assert_any_call('combine_data.parquet', index=False)
    mock_to_parquet.assert_any_call('team_desc.parquet', index=False)

# Tests that data is fetched, saved to parquet files, and loaded to S3 bucket for a valid year.
def test_fetch_yearly_data_valid_year(mocker: pytest_mock.MockerFixture) -> None:
    year = 2021
    mock_to_parquet = mocker.patch('pandas.DataFrame.to_parquet')
    
    mock_s3_load_file = mocker.patch('airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file', return_value=None)
    
    nfl_func.fetch_yearly_data(year)
    
    mock_to_parquet.assert_any_call('weekly_data.parquet', index=False)
    mock_to_parquet.assert_any_call('snap_counts.parquet', index=False)
    mock_to_parquet.assert_any_call('qbr.parquet', index=False)
    mock_to_parquet.assert_any_call('depth_charts.parquet', index=False)
    mock_to_parquet.assert_any_call('injuries.parquet', index=False)
    mock_to_parquet.assert_any_call('win_totals.parquet', index=False)
    mock_to_parquet.assert_any_call('sc_lines.parquet', index=False)
    mock_s3_load_file.assert_any_call(filename='weekly_data.parquet', key=f'bronze/nfl/weekly_data/{year}.parquet', bucket_name='sports-lakehouse', replace=True, gzip=True)