import statsapi
from prefect import flow, task
import pandas as pd
from prefect.artifacts import create_table_artifact
from datetime import timedelta
from prefect_aws.s3 import S3Bucket

s3_bucket_block = S3Bucket.load("s3-results")

@task(result_storage_key="batting_stats", cache_expiration=timedelta(seconds=50))
def process_batting_stats(boxscore, schedule_game_id, team_type):
    batting_records = []
    
    batters = boxscore.get(f'{team_type}Batters', [])[1:]
    team_info = boxscore['teamInfo'][team_type]
    
    for batter in batters:
        if isinstance(batter, dict):
            # Convert string values to appropriate numeric types
            record = {
                'game_id': int(schedule_game_id),
                'game_date': boxscore['gameId'].split('/')[0:3],
                'team_id': int(team_info['id']),
                'team_name': str(team_info['teamName']),
                'player_id': int(batter.get('personId', 0)),
                'player_name': str(batter.get('name', '')),
                'position': str(batter.get('position', '')),
                'batting_order': str(batter.get('battingOrder', '')),
                'ab': int(batter.get('ab', 0)),
                'r': int(batter.get('r', 0)),
                'h': int(batter.get('h', 0)),
                'doubles': int(batter.get('doubles', 0)),
                'triples': int(batter.get('triples', 0)),
                'hr': int(batter.get('hr', 0)),
                'rbi': int(batter.get('rbi', 0)),
                'bb': int(batter.get('bb', 0)),
                'k': int(batter.get('k', 0)),
                'lob': int(batter.get('lob', 0)),
                'avg': float(batter.get('avg', 0.0)),
                'ops': float(batter.get('ops', 0.0)),
                'obp': float(batter.get('obp', 0.0)),
                'slg': float(batter.get('slg', 0.0))
            }
            batting_records.append(record)
    
    return batting_records

@task(result_storage_key="pitching_stats", cache_expiration=timedelta(seconds=50))
def process_pitching_stats(boxscore, schedule_game_id, team_type):
    pitching_records = []
    
    # Get pitchers array (skip the header row)
    pitchers = boxscore.get(f'{team_type}Pitchers', [])[1:]  # Skip first element which is header
    team_info = boxscore['teamInfo'][team_type]
    
    for pitcher in pitchers:
        if isinstance(pitcher, dict):  # Only process actual pitcher entries
            record = {
                'game_id': schedule_game_id,  # Use the schedule's game ID
                'game_date': boxscore['gameId'].split('/')[0:3],  # Extract date from boxscore ID
                'team_id': team_info['id'],
                'team_name': team_info['teamName'],
                'player_name': pitcher.get('name', ''),
                'player_id': pitcher.get('personId', ''),
                'note': pitcher.get('note', ''),
                'ip': pitcher.get('ip', ''),
                'h': pitcher.get('h', ''),
                'r': pitcher.get('r', ''),
                'er': pitcher.get('er', ''),
                'bb': pitcher.get('bb', ''),
                'k': pitcher.get('k', ''),
                'hr': pitcher.get('hr', ''),
                'era': pitcher.get('era', ''),
                'pitches': pitcher.get('p', ''),
                'strikes': pitcher.get('s', '')
            }
            pitching_records.append(record)
    
    return pitching_records

@task(result_storage_key="single_game_boxscore", cache_expiration=timedelta(seconds=5))
def process_single_game_boxscore(game_id=744798):
    # Get the boxscore data
    boxscore = statsapi.boxscore_data(game_id)
    
    # Initialize lists for batting and pitching data
    all_batting = []
    all_pitching = []
    
    # Process home and away teams
    for team_type in ['home', 'away']:
        all_batting.extend(process_batting_stats(boxscore, game_id, team_type))
        all_pitching.extend(process_pitching_stats(boxscore, game_id, team_type))
    
    # Convert to DataFrames
    batting_df = pd.DataFrame(all_batting)
    pitching_df = pd.DataFrame(all_pitching)

    # Save to CSV files
    batting_filename = f"batting_{game_id}.csv"
    pitching_filename = f"pitching_{game_id}.csv"
    
    batting_df.to_csv(batting_filename, index=False)
    pitching_df.to_csv(pitching_filename, index=False)

    batting_records = batting_df.to_dict('records')
    pitching_records = pitching_df.to_dict('records')

    create_table_artifact(
        key="single-game-boxscore-batting",
        table=batting_records,
        description= "# Batting df from single game boxscore!"
    )

    create_table_artifact(
        key="single-game-boxscore-pitching",
        table=pitching_records,
        description= "# Pitching df from single game boxscore!"
    )
    
    return batting_filename, pitching_filename, batting_df, pitching_df


@flow(log_prints=True, result_storage=s3_bucket_block)
def new_game_data():
    
    #Grab boxscore of new game
    batting_file, pitching_file, batting_df, pitching_df = process_single_game_boxscore()
    print(f"\nFiles created:")
    print(f"Batting stats: {batting_file}")
    print(f"Pitching stats: {pitching_file}")

    # Print summary statistics
    print(f"\nTotal batting records: {len(batting_df)}")
    print(f"Total pitching records: {len(pitching_df)}")


if __name__ == "__main__":
    # new_game_data_and_dbt_run.serve()
    new_game_data()