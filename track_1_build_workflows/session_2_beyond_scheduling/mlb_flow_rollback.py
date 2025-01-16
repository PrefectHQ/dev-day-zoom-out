from prefect import flow, task, runtime
from prefect.artifacts import create_markdown_artifact
from prefect.transactions import transaction
from datetime import datetime
import statsapi
import json
import pandas as pd
import os
import time

@task
def get_recent_games(team_id, start_date, end_date):
    # Get all games for the provided team and date range
    schedule = statsapi.schedule(team=team_id,start_date=start_date,end_date=end_date)
    for game in schedule:
        print(game['game_id'])
    return [game['game_id'] for game in schedule]

@task
def fetch_single_game_boxscore(game_id):
    boxscore = statsapi.boxscore_data(game_id)
    
    # Extract relevant data
    home_score = boxscore['home']['teamStats']['batting']['runs']
    away_score = boxscore['away']['teamStats']['batting']['runs']
    home_team = boxscore['teamInfo']['home']['teamName']
    away_team = boxscore['teamInfo']['away']['teamName']
    time_value = next(item['value'] for item in boxscore['gameBoxInfo'] if item['label'] == 'T')
    
    #Create a dictionary with the game data
    game_data = {
        'game_id': game_id,
        'home_team': home_team,
        'away_team': away_team,
        'home_score': home_score,
        'away_score': away_score,
        'score_differential': abs(home_score - away_score),
        'game_time': time_value,
    }
    
    print(game_data)
    return game_data

@task
def clean_time_value(game_data):
    #Remove any extra text like '(1:16 delay)'
    if '(' in game_data['game_time']:
        game_data['game_time'] = game_data['game_time'].split('(')[0]
    
    # Remove any non-digit, non-colon characters
    game_data['game_time'] = ''.join(char for char in game_data['game_time'] if char.isdigit() or char == ':')
    
    hours, minutes = map(int, game_data['game_time'].split(':'))
    game_data['game_time_in_minutes'] = hours * 60 + minutes
    
    print(game_data)
    return game_data

@task
def save_raw_data_to_file(game_data, file_name):
    #save raw data to the raw_data folder
    with open(file_name, "w") as outfile:
        json.dump(game_data, outfile, indent=4, sort_keys=True)
    
    print(file_name)
    return file_name
        
@save_raw_data_to_file.on_rollback
def del_file(txn):
    "Deletes file."
    os.unlink(txn.get("filepath"))
    

@task
def quality_test(file_path):
    "Checks contents of file."
    with open(file_path, "r") as f:
        data = json.load(f)
        
    if len(data) < 5:
        raise ValueError(f"Not enough data! There are only {len(data)} games in the file.")

@task
def analyze_games(game_data, start_date, end_date, team_id):
    df = pd.DataFrame(game_data)
    
    # Calculate average, median, max, and min differential
    avg_differential = float(df['score_differential'].mean())
    median_differential = float(df['score_differential'].median())
    max_differential = float(df['score_differential'].max())
    min_differential = float(df['score_differential'].min())

    
    # Calculate average, median, max, and min game time
    avg_game_time = float(df['game_time_in_minutes'].mean())
    median_game_time = float(df['game_time_in_minutes'].median())
    max_game_time = float(df['game_time_in_minutes'].max())
    min_game_time = float(df['game_time_in_minutes'].min())
    
    # Calculate correlation between game time and score differential
    correlation = float(df['game_time_in_minutes'].corr(df['score_differential']))
    
    game_analysis = {
        'search_start_date': start_date,
        'search_end_date': end_date,
        'chosen_team_id': team_id,
        'max_game_time': max_game_time,
        'min_game_time': min_game_time,
        'median_game_time': median_game_time,
        'average_game_time': avg_game_time,
        'max_differential': max_differential,
        'min_differential': min_differential,
        'median_differential': median_differential,
        'average_differential': avg_differential,
        'time_differential_correlation': correlation,
    }
    print(game_analysis)
    return game_analysis

@task
def save_analysis_to_file(game_analysis, file_name):
    with open(file_name, "w") as outfile:
        json.dump(game_analysis, outfile, indent=4, sort_keys=True)
        

@task
def game_analysis_artifact(game_analysis, game_data):
    df = pd.DataFrame(game_data)
    
    markdown_report=f""" # Game Analysis Report
## Search Parameters
Search Start Date: {game_analysis['search_start_date']}
Search End Date: {game_analysis['search_end_date']}
Chosen Team ID: {game_analysis['chosen_team_id']}

## Summary Statistics
Max game time: {game_analysis['max_game_time']:.2f}
Min game time: {game_analysis['min_game_time']:.2f}
Median game time: {game_analysis['median_game_time']:.2f}
Average game time: {game_analysis['average_game_time']:.2f}
Max differential: {game_analysis['max_differential']:.2f}
Min differential: {game_analysis['min_differential']:.2f}
Median differential: {game_analysis['median_differential']:.2f}
Average differential: {game_analysis['average_differential']:.2f}
Correlation between game time and score differential: {game_analysis['time_differential_correlation']:.2f}

## Raw Data
{df.to_markdown(index=False)}

"""
    create_markdown_artifact(
        key="game-analysis",
        markdown=markdown_report,
        description="Game analysis report"
    )


@flow
def mlb_flow_rollback(team_id, start_date, end_date):
    # Get recent games
    game_ids = get_recent_games(team_id, start_date, end_date)
    
    # Fetch boxscore for each game
    game_data = [fetch_single_game_boxscore(game_id) for game_id in game_ids]
    
    #Define file path for raw data
    today = datetime.now().strftime("%Y-%m-%d") #YYYY-MM-DD
    flow_run_name = runtime.flow_run.name
    raw_file_path = f"./raw_data/{today}-{team_id}-{flow_run_name}-boxscore.json"
    
    #Create transaction which will be used to rollback if the data quality test fails
    with transaction() as txn:
        txn.set("filepath", raw_file_path)
        # Save raw data to a file
        save_raw_data_to_file(game_data, raw_file_path)
        time.sleep(10) # sleeping to give you a chance to see the file
        quality_test(raw_file_path)
    
    # Clean the time value
    game_data = [clean_time_value(game_data) for game_data in game_data]
    
    # Analyze the results
    results = analyze_games(game_data, start_date, end_date, team_id)
    
    # Save the results to a file
    analysis_file_path = f"./boxscore_analysis/{today}-{team_id}-{flow_run_name}-game-analysis.json"
    save_analysis_to_file(results, analysis_file_path)
    
    # Save the results to an artifact
    game_analysis_artifact(results, game_data)
    
    
if __name__ == "__main__":
    #This flow will succeed
    mlb_flow_rollback(143, '06/01/2024', '06/30/2024')
    #This flow will fail the quality test
    mlb_flow_rollback(143, '06/01/2024', '06/02/2024')