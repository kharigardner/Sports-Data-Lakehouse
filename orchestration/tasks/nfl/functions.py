import datetime as dt
import pandas as pd
import nfl_data_py as nfl

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import typing
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def static_data_fetch() -> None:
    """
    Airflow task to fetch static data from the NFL data API.
    """

    officials = nfl.import_officials()
    mapping_ids = nfl.import_ids()
    contracts = nfl.import_contracts()
    draft_picks = nfl.import_draft_picks()
    draft_values = nfl.import_draft_values()
    combine_data = nfl.import_combine_data()
    team_desc = nfl.import_team_desc()

    officials.to_parquet('officials.parquet', index=False)
    mapping_ids.to_parquet('mapping_ids.parquet', index=False)
    contracts.to_parquet('contracts.parquet', index=False)
    draft_picks.to_parquet('draft_picks.parquet', index=False)
    draft_values.to_parquet('draft_values.parquet', index=False)
    combine_data.to_parquet('combine_data.parquet', index=False)
    team_desc.to_parquet('team_desc.parquet', index=False)

    s3_hook = S3Hook(aws_conn_id='aws_default')

    s3_hook.load_file(
        filename='officials.parquet',
        key='bronze/nfl/static/officials.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='mapping_ids.parquet',
        key='bronze/nfl/static/mapping_ids.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='contracts.parquet',
        key='bronze/nfl/static/contracts.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='draft_picks.parquet',
        key='bronze/nfl/static/draft_picks.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='draft_values.parquet',
        key='bronze/nfl/static/draft_values.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='combine_data.parquet',
        key='bronze/nfl/static/combine_data.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='team_desc.parquet',
        key='bronze/nfl/static/team_desc.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

def fetch_yearly_data(year: int):
    """Fetch data from the NFL data API for a given year."""
    weekly_data = nfl.import_weekly_data([year])
    snap_counts = nfl.import_snap_counts([year])
    qbr = nfl.import_qbr([year], frequency='weekly')
    depth_charts = nfl.import_depth_charts([year])
    injuries = nfl.import_injuries([year])
    win_totals = nfl.import_win_totals([year])
    sc_lines = nfl.import_sc_lines([year])

    weekly_data.to_parquet('weekly_data.parquet', index=False)
    snap_counts.to_parquet('snap_counts.parquet', index=False)
    qbr.to_parquet('qbr.parquet', index=False)
    depth_charts.to_parquet('depth_charts.parquet', index=False)
    injuries.to_parquet('injuries.parquet', index=False)
    win_totals.to_parquet('win_totals.parquet', index=False)
    sc_lines.to_parquet('sc_lines.parquet', index=False)

    s3_hook = S3Hook(aws_conn_id='aws_default')

    s3_hook.load_file(
        filename='weekly_data.parquet',
        key=f'bronze/nfl/weekly_data/{year}.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='snap_counts.parquet',
        key=f'bronze/nfl/snap_counts/{year}.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='qbr.parquet',
        key=f'bronze/nfl/qbr/{year}.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='depth_charts.parquet',
        key=f'bronze/nfl/depth_charts/{year}.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='injuries.parquet',
        key=f'bronze/nfl/injuries/{year}.parquet',
        bucket_name='sports-lakehouse',
        replace=True,   
        gzip=True
    )

    s3_hook.load_file(
        filename='win_totals.parquet',
        key=f'bronze/nfl/win_totals/{year}.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='sc_lines.parquet',
        key=f'bronze/nfl/sc_lines/{year}.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )