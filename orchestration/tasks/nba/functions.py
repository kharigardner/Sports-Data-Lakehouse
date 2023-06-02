import datetime as dt
import pandas as pd
from nba_api.stats.endpoints import *
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints._base import Endpoint

import logging
import typing

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def static_data_fetch() -> None:
    """
    Airflow task to fetch static data from the NBA API.
    Wrapped in the dag around a task decorator and another function.
    Here so we can test independently of an airflow instance.
    """
    logging.info('Fetching static data...')

    static_players = players.get_players()
    static_teams = teams.get_teams()

    logging.info('Static data fetched.')

    players_df = pd.DataFrame(static_players)
    teams_df = pd.DataFrame(static_teams)

    logging.warn('Loaded dataframes... beginning to write to Parquet.')

    players_df.to_parquet('players.parquet', index=False)
    
    logging.warn('Wrote players.parquet.')

    teams_df.to_parquet('teams.parquet', index=False)
    
    logging.warn('Wrote teams.parquet.')

    logging.info('Finished fetching static data.')

    logging.info('Initializing S3Hook...')

    s3_hook = S3Hook(aws_conn_id='aws_default')


    s3_hook.load_file(
        filename='players.parquet',
        key='bronze/nba/static/players.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )
    
    logging.info('Saved players.parquet to S3.')

    s3_hook.load_file(
        filename='teams.parquet',
        key='bronze/nba/static/teams.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    logging.info('Saved teams.parquet to S3.')

    logging.info('Finished task.')

def all_time_leaders() -> None:
    """
    Airflow task to fetch all time leaders from the NBA API.
    Wrapped in the dag around a task decorator and another function.
    Here so we can test independently of an airflow instance.
    """

    alltimeleadersgrids_data = alltimeleadersgrids.AllTimeLeadersGrids()

    ast_leaders = alltimeleadersgrids_data.ast_leaders.get_data_frame()
    blk_leaders = alltimeleadersgrids_data.blk_leaders.get_data_frame()
    dreb_leaders = alltimeleadersgrids_data.dreb_leaders.get_data_frame()
    fg3_a_leaders = alltimeleadersgrids_data.fg3_a_leaders.get_data_frame()
    fg3_m_leaders = alltimeleadersgrids_data.fg3_m_leaders.get_data_frame()
    fg3_pct_leaders = alltimeleadersgrids_data.fg3_pct_leaders.get_data_frame()
    fga_leaders = alltimeleadersgrids_data.fga_leaders.get_data_frame()
    fgm_leaders = alltimeleadersgrids_data.fgm_leaders.get_data_frame()
    fg_pct_leaders = alltimeleadersgrids_data.fg_pct_leaders.get_data_frame()
    fta_leaders = alltimeleadersgrids_data.fta_leaders.get_data_frame()
    ftm_leaders = alltimeleadersgrids_data.ftm_leaders.get_data_frame()
    ft_pct_leaders = alltimeleadersgrids_data.ft_pct_leaders.get_data_frame()
    g_p_leaders = alltimeleadersgrids_data.g_p_leaders.get_data_frame()
    oreb_leaders = alltimeleadersgrids_data.oreb_leaders.get_data_frame()
    pf_leaders = alltimeleadersgrids_data.pf_leaders.get_data_frame()
    pts_leaders = alltimeleadersgrids_data.pts_leaders.get_data_frame()
    reb_leaders = alltimeleadersgrids_data.reb_leaders.get_data_frame()
    stl_leaders = alltimeleadersgrids_data.stl_leaders.get_data_frame()
    tov_leaders = alltimeleadersgrids_data.tov_leaders.get_data_frame()
    
    global_assist_counter = assisttracker.AssistTracker()
    dfs = global_assist_counter.get_data_frames()
    global_assist_counter_df = dfs[0]


    global_assist_counter_df.to_parquet('global_assist_counter.parquet', index=False)
    ast_leaders.to_parquet('ast_leaders.parquet', index=False)
    blk_leaders.to_parquet('blk_leaders.parquet', index=False)
    dreb_leaders.to_parquet('dreb_leaders.parquet', index=False)
    fg3_a_leaders.to_parquet('fg3_a_leaders.parquet', index=False)
    fg3_m_leaders.to_parquet('fg3_m_leaders.parquet', index=False)
    fg3_pct_leaders.to_parquet('fg3_pct_leaders.parquet', index=False)
    fga_leaders.to_parquet('fga_leaders.parquet', index=False)
    fgm_leaders.to_parquet('fgm_leaders.parquet', index=False)
    fg_pct_leaders.to_parquet('fg_pct_leaders.parquet', index=False)
    fta_leaders.to_parquet('fta_leaders.parquet', index=False)
    ftm_leaders.to_parquet('ftm_leaders.parquet', index=False)
    ft_pct_leaders.to_parquet('ft_pct_leaders.parquet', index=False)
    g_p_leaders.to_parquet('g_p_leaders.parquet', index=False)
    oreb_leaders.to_parquet('oreb_leaders.parquet', index=False)
    pf_leaders.to_parquet('pf_leaders.parquet', index=False)
    pts_leaders.to_parquet('pts_leaders.parquet', index=False)
    reb_leaders.to_parquet('reb_leaders.parquet', index=False)
    stl_leaders.to_parquet('stl_leaders.parquet', index=False)
    tov_leaders.to_parquet('tov_leaders.parquet', index=False)

    s3_hook = S3Hook(aws_conn_id='aws_default')

    s3_hook.load_file(
        filename='global_assist_counter.parquet',
        key='bronze/nba/static/global_assist_counter.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='ast_leaders.parquet',
        key='bronze/nba/static/ast_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='blk_leaders.parquet',
        key='bronze/nba/static/blk_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='dreb_leaders.parquet',
        key='bronze/nba/static/dreb_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='fg3_a_leaders.parquet',
        key='bronze/nba/static/fg3_a_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='fg3_m_leaders.parquet',
        key='bronze/nba/static/fg3_m_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='fg3_pct_leaders.parquet',
        key='bronze/nba/static/fg3_pct_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='fga_leaders.parquet',
        key='bronze/nba/static/fga_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='fgm_leaders.parquet',
        key='bronze/nba/static/fgm_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='fg_pct_leaders.parquet',
        key='bronze/nba/static/fg_pct_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='fta_leaders.parquet',
        key='bronze/nba/static/fta_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='ftm_leaders.parquet',
        key='bronze/nba/static/ftm_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='ft_pct_leaders.parquet',
        key='bronze/nba/static/ft_pct_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='g_p_leaders.parquet',
        key='bronze/nba/static/g_p_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='oreb_leaders.parquet',
        key='bronze/nba/static/oreb_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='pf_leaders.parquet',
        key='bronze/nba/static/pf_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='pts_leaders.parquet',
        key='bronze/nba/static/pts_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='reb_leaders.parquet',
        key='bronze/nba/static/reb_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='stl_leaders.parquet',
        key='bronze/nba/static/stl_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

    s3_hook.load_file(
        filename='tov_leaders.parquet',
        key='bronze/nba/static/tov_leaders.parquet',
        bucket_name='sports-lakehouse',
        replace=True,
        gzip=True
    )

def get_games() -> typing.List[typing.Dict[str, pd.DataFrame]]:
    TEAM_IDS = {1610612737,1610612738,1610612739,1610612740,1610612741,1610612742,1610612743,1610612744,1610612745,1610612746,1610612747,1610612748,1610612749,1610612750,1610612751,1610612752,1610612753,1610612754,1610612755,1610612756,1610612757,1610612758,1610612759,1610612760,1610612761,1610612762,1610612763
    ,1610612764,1610612765, 1610612766}
    PROXY_INFO = 'http://groups-RESIDENTIAL:apify_proxy_X4cFIqR8cbXXz2hq2bTusiFAQctscK3bu1lI@proxy.apify.com:8000'

    teams_df = []

    for team_id in TEAM_IDS:
        games = leaguegamefinder.LeagueGameFinder(
            team_id_nullable=str(team_id),
            proxy=PROXY_INFO
        )

        games_dfs = games.get_data_frames()

        logging.info(f'Found {len(games_dfs)} game dataframes for {team_id}')

        # stack the dataframes
        logging.info(f'Stacking dataframes for {team_id}')
        
        games_df = pd.concat(games_dfs, ignore_index=True)

        teams_df.append({
            'team_id': team_id,
            'games': games_df
        })

        logging.info(f'Finished extracting data for {team_id}')

    return teams_df

def process_games(teams_df_list: typing.List[typing.Dict[str, pd.DataFrame]]) -> typing.List[typing.Any]:
    """
    Processes the games data for each team in the given list of teams dataframes and saves the resulting data to S3.

    Args:
        teams_df_list (List[Dict[str, pd.DataFrame]], optional): A list of dictionaries containing the team ID and its corresponding games dataframe. Defaults to get_games().

    Returns:
        None
    """

    game_ids = []

    for team_df in teams_df_list:
        
        logging.info(f'Processing {team_df["team_id"]}')

        typing.cast(pd.DataFrame, team_df['games']).to_parquet(
            f'{team_df["team_id"]}_games.parquet',
        )


        game_idz = team_df['games']['GAME_ID'].to_list()
        
        logging.info(f'Appending {len(game_idz)} game ids for {team_df["team_id"]} to pass to boxscore_getter task...')
        
        game_ids.extend(game_idz)

        s3_hook = S3Hook(aws_conn_id='aws_default')

        s3_hook.load_file(
            filename=f'{team_df["team_id"]}_games.parquet',
            key=f'bronze/nba/games/{team_df["team_id"]}_games.parquet',
            bucket_name='sports-lakehouse',
            replace=True,
            gzip=True
        )

        logging.info(f'Finished processing {team_df["team_id"]}')


    return game_ids


def boxscore_getter(endpoint: Endpoint) -> typing.List[pd.DataFrame]:
    try:
        return endpoint.get_data_frames()
    except Exception as e:
        try:
            return [endpoint.get_data_frame()]
        except AttributeError as e:
            try:
                data = endpoint.get_normalized_dict()
                if isinstance(data, list):
                    return [pd.DataFrame(data)]
                else:
                    return [pd.DataFrame(data['data'] if 'data' in data else data)]
            except Exception as e:
                raise e
            
                