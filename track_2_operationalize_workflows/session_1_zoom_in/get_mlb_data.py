# get_mlb_data.py

from prefect import flow, task
from datetime import datetime
import statsapi
import json
import pandas as pd
import os
from prefect.tasks import exponential_backoff
from typing import List, Dict
import asyncio
from prefect.futures import wait

# Importing Snowflake helper tasks
from snowflake_helper import (
    create_mlb_snowflake_tables,
    insert_game_scores_into_snowflake,
    insert_game_locations_into_snowflake,
)
from prefect._experimental.lineage import emit_lineage_event
from resources import MLB_API_SCHEDULE, MLB_API_SCORE, MLB_API_LOCATION, HIGHLIGHTED_TEAMS_S3_FILE
from prefect_aws.s3 import S3Bucket
import pandas as pd
import time

@task
async def get_highlighted_teams_data_from_s3() -> pd.DataFrame:
    s3_bucket_block = await S3Bucket.load("mlb-raw-data")
    s3_bucket_path = await s3_bucket_block.upload_from_path("2025-01-02-143-alluring-dragonfly-boxscore.json")
    downloaded_file_path = await s3_bucket_block.download_object_to_path(
        s3_bucket_path, "highlighted_team_data.json"
    )

    await emit_lineage_event(
        event_name="Get Highlighted Teams Data from S3",
        upstream_resources=[HIGHLIGHTED_TEAMS_S3_FILE],
        downstream_resources=None,
        direction_of_run_from_event="downstream",
    )

    df = pd.read_json("highlighted_teams_data.json")

    team_ids = df[0].to_list()

    await emit_lineage_event(
        event_name=f"Get Highlighted Teams Data from S3; {team_ids}",
        upstream_resources=[HIGHLIGHTED_TEAMS_S3_FILE],
        downstream_resources=None,
        direction_of_run_from_event="downstream",
    )

    return team_ids


@task
async def retrieve_recent_game_ids(
    team_ids: List[int], start_date: str, end_date: str
) -> List[str]:
    """
    Retrieve recent game IDs for the specified teams and date range.
    """
    all_game_ids = []
    for team_id in team_ids:
        # Assuming statsapi.schedule is synchronous; wrap it in a thread
        schedule = statsapi.schedule(
            team=team_id, start_date=start_date, end_date=end_date
        )
        await emit_lineage_event(
            event_name=f"Get Recent Games; Team ID: {team_id} Date Range: {start_date} {end_date}",
            upstream_resources=[MLB_API_SCHEDULE],
            downstream_resources=None,
            direction_of_run_from_event="downstream",
        )
        # Ensure game_id is string and present
        game_ids = [str(game["game_id"]) for game in schedule if "game_id" in game]
        print(f"Retrieved {len(game_ids)} games for team ID {team_id}.")
        all_game_ids.extend(game_ids)

    # Remove duplicates and ensure all are digits
    unique_game_ids = list({gid for gid in all_game_ids if gid.isdigit()})

    return unique_game_ids


@task(retries=5, retry_delay_seconds=exponential_backoff(backoff_factor=10))
async def fetch_game_score_data(game_id: str) -> Dict:
    boxscore = statsapi.boxscore_data(game_id)

    await emit_lineage_event(
        event_name=f"Fetch Game Score; Game ID: {game_id}",
        upstream_resources=[MLB_API_SCORE],
        downstream_resources=None,
        direction_of_run_from_event="downstream",
    )

    if not boxscore:
        print(f"No boxscore data found for game ID {game_id}.")
        return {}

    # Extract relevant data with correct key names
    home_score = (
        boxscore.get("home", {}).get("teamStats", {}).get("batting", {}).get("runs", 0)
    )
    away_score = (
        boxscore.get("away", {}).get("teamStats", {}).get("batting", {}).get("runs", 0)
    )
    home_team = boxscore.get("teamInfo", {}).get("home", {}).get("teamName", "Unknown")
    away_team = boxscore.get("teamInfo", {}).get("away", {}).get("teamName", "Unknown")
    home_team_id = (
        boxscore.get("teamInfo", {}).get("home", {}).get("id", 0)
    )  # Default to 0 if missing
    away_team_id = (
        boxscore.get("teamInfo", {}).get("away", {}).get("id", 0)
    )  # Default to 0 if missing
    time_value = next(
        (
            item.get("value", "Unknown")
            for item in boxscore.get("gameBoxInfo", [])
            if item.get("label") == "T"
        ),
        "Unknown",
    )

    # Create a dictionary with the game score data
    score_data = {
        "game_id": int(game_id),  # Ensure GAME_ID is integer
        "home_team_id": int(home_team_id) if home_team_id else 0,
        "home_team": home_team,
        "away_team_id": int(away_team_id) if away_team_id else 0,
        "away_team": away_team,
        "home_score": int(home_score),
        "away_score": int(away_score),
        "score_differential": abs(int(home_score) - int(away_score)),
        "game_time": time_value,
    }

    return score_data


@task(retries=5, retry_delay_seconds=exponential_backoff(backoff_factor=10))
async def fetch_game_location_data(game_id: str) -> Dict:
    """
    Fetch game location details for each game.
    """
    game = statsapi.get("game", params={"gamePk": game_id})

    await emit_lineage_event(
        event_name=f"Fetch Game Location; Game ID: {game_id}",
        upstream_resources=[MLB_API_LOCATION],
        downstream_resources=None,
        direction_of_run_from_event="downstream",
    )

    if not game:
        print(f"No location data found for game ID {game_id}.")
        return {}

    # Access venue data correctly
    game_data = game.get("gameData", {})
    venue = game_data.get("venue", {})
    location = venue.get("location", {})
    default_coordinates = location.get("defaultCoordinates", {})

    location_data = {
        "game_id": int(game_id),  # Ensure GAME_ID is integer
        "venue_id": int(venue.get("id", 0)) if venue.get("id") else 0,
        "venue_name": venue.get("name", "Unknown"),
        "venue_city": location.get("city", "Unknown"),
        "venue_state": location.get("state", "Unknown"),
        "venue_postal_code": location.get("postalCode", "Unknown"),
        "venue_country": location.get("country", "Unknown"),
        "venue_latitude": float(default_coordinates.get("latitude", 0.0)),
        "venue_longitude": float(default_coordinates.get("longitude", 0.0)),
        "venue_elevation": float(
            location.get("elevation", 0.0)
        ),  # Handle elevation with default
    }

    return location_data


@flow
async def fetch_and_store_mlb_raw_data(start_date: str, end_date: str, snowflake_block: str
):
    """
    Prefect flow to fetch game scores and locations for multiple teams, then insert them into Snowflake.
    """

    # Step 0: Get highlighted teams data to analyze
    team_ids = await get_highlighted_teams_data_from_s3()

    # Step 1: Set up Snowflake tables
    await create_mlb_snowflake_tables(block_name=snowflake_block)

    # Step 2: Get recent game IDs for all teams
    game_ids = await retrieve_recent_game_ids(team_ids, start_date, end_date)

    # Step 3: Fetch game scores
    scores = []
    tasks = []
    for game_id in game_ids:
        task_future = fetch_game_score_data.submit(game_id)
        tasks.append(task_future)
        print(f"Submitted score fetch for game ID {game_id}.")
    # Gather all results asynchronously
    for task in tasks:
        scores.append(task.result())
    # Filter out any empty dictionaries in case of missing data
    scores = [score for score in scores if score]

    # Step 4: Fetch game locations
    locations = []
    tasks = []
    for game_id in game_ids:
        task_future = fetch_game_location_data.submit(game_id)
        tasks.append(task_future)
        print(f"Submitted location fetch for game ID {game_id}.")
    # Gather all results asynchronously
    for task in tasks:
        locations.append(task.result())
    # Filter out any empty dictionaries in case of missing data
    locations = [location for location in locations if location]

    # Step 5: Insert data into Snowflake
    await insert_game_scores_into_snowflake(
        game_scores=scores, block_name=snowflake_block
    )
    await insert_game_locations_into_snowflake(
        game_locations=locations, block_name=snowflake_block
    )

    print("MLB Simple Flow Completed Successfully.")


if __name__ == "__main__":
    # Example usage
    START_DATE = "2022-02-01"
    END_DATE = "2024-11-30"
    SNOWFLAKE_BLOCK_NAME = "dev-day-staging"  # Replace with your actual block name

    asyncio.run(
        fetch_and_store_mlb_raw_data(
            start_date=START_DATE,
            end_date=END_DATE,
            snowflake_block=SNOWFLAKE_BLOCK_NAME,
        )
    )
