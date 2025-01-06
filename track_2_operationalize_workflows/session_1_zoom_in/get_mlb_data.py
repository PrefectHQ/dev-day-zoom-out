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
from snowflake_helper import setup_tables, insert_game_scores, insert_game_locations
from prefect._experimental.lineage import emit_lineage_event

@task
async def get_recent_games(team_ids: List[int], start_date: str, end_date: str) -> List[str]:
    """
    Retrieve recent game IDs for the specified teams and date range.
    """
    all_game_ids = []
    for team_id in team_ids:
        # Assuming statsapi.schedule is synchronous; wrap it in a thread
        schedule = await asyncio.to_thread(statsapi.schedule, team=team_id, start_date=start_date, end_date=end_date)
        # Ensure game_id is string and present
        game_ids = [str(game["game_id"]) for game in schedule if "game_id" in game]
        print(f"Retrieved {len(game_ids)} games for team ID {team_id}.")
        all_game_ids.extend(game_ids)
    # Remove duplicates and ensure all are digits
    unique_game_ids = list({gid for gid in all_game_ids if gid.isdigit()})
    return unique_game_ids

@task(retries=5, retry_delay_seconds=exponential_backoff(backoff_factor=10))
async def fetch_game_score(game_id: str) -> Dict:
    boxscore = await asyncio.to_thread(statsapi.boxscore_data, game_id)

    if not boxscore:
        print(f"No boxscore data found for game ID {game_id}.")
        return {}

    # Extract relevant data with correct key names
    home_score = boxscore.get("home", {}).get("teamStats", {}).get("batting", {}).get("runs", 0)
    away_score = boxscore.get("away", {}).get("teamStats", {}).get("batting", {}).get("runs", 0)
    home_team = boxscore.get("teamInfo", {}).get("home", {}).get("teamName", "Unknown")
    away_team = boxscore.get("teamInfo", {}).get("away", {}).get("teamName", "Unknown")
    home_team_id = boxscore.get("teamInfo", {}).get("home", {}).get("id", 0)  # Default to 0 if missing
    away_team_id = boxscore.get("teamInfo", {}).get("away", {}).get("id", 0)  # Default to 0 if missing
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

    # Emit lineage event after fetching game score
    await emit_lineage_event(
        event_name="fetch_game_score",
        upstream_resources=[
            {
                "prefect.resource.id": f"api://statsapi/boxscore/{game_id}",
                "prefect.resource.lineage-group": "mlb_data_pipeline",
                "prefect.resource.role": "api",
                "prefect.resource.name": f"Boxscore API for Game ID {game_id}",
            }
        ],
        downstream_resources=[
            {
                "prefect.resource.id": "snowflake://DEV_DAY/PUBLIC/GAME_SCORES",
                "prefect.resource.lineage-group": "mlb_data_pipeline",
                "prefect.resource.role": "table",
                "prefect.resource.name": "DEV_DAY.PUBLIC.GAME_SCORES",
            }
        ],
        direction_of_run_from_event="downstream",
    )

    return score_data

@task(retries=5, retry_delay_seconds=exponential_backoff(backoff_factor=10))
async def fetch_game_location(game_id: str) -> Dict:
    """
    Fetch game location details for each game.
    """
    game = await asyncio.to_thread(statsapi.get, "game", params={"gamePk": game_id})
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
        "venue_elevation": float(location.get("elevation", 0.0)),  # Handle elevation with default
    }

    # Emit lineage event after fetching game location
    await emit_lineage_event(
        event_name="fetch_game_location",
        upstream_resources=[
            {
                "prefect.resource.id": f"api://statsapi/game/{game_id}",
                "prefect.resource.lineage-group": "mlb_data_pipeline",
                "prefect.resource.role": "api",
                "prefect.resource.name": f"Game API for Game ID {game_id}",
            }
        ],
        downstream_resources=[
            {
                "prefect.resource.id": "snowflake://DEV_DAY/PUBLIC/GAME_LOCATIONS",
                "prefect.resource.lineage-group": "mlb_data_pipeline",
                "prefect.resource.role": "table",
                "prefect.resource.name": "DEV_DAY.PUBLIC.GAME_LOCATIONS",
            }
        ],
        direction_of_run_from_event="downstream",
    )

    return location_data

@flow
async def get_game_scores(game_ids: List[str]) -> List[Dict]:
    """
    Fetch scores for each game.
    """
    scores = []
    tasks = []
    for game_id in game_ids:
        task_future = fetch_game_score.submit(game_id)
        tasks.append(task_future)
        print(f"Submitted score fetch for game ID {game_id}.")
    # Gather all results asynchronously
    for task in tasks:
        scores.append(task.result())
    # Filter out any empty dictionaries in case of missing data
    scores = [score for score in scores if score]
    return scores

@flow
async def get_game_locations_flow(game_ids: List[str]) -> List[Dict]:
    """
    Fetch game location details for each game.
    """
    locations = []
    tasks = []
    for game_id in game_ids:
        task_future = fetch_game_location.submit(game_id)
        tasks.append(task_future)
        print(f"Submitted location fetch for game ID {game_id}.")
    # Gather all results asynchronously
    for task in tasks:
        locations.append(task.result())
    # Filter out any empty dictionaries in case of missing data
    locations = [location for location in locations if location]
    return locations

@flow
async def mlb_simple_flow(team_ids: List[int], start_date: str, end_date: str, snowflake_block: str):
    """
    Prefect flow to fetch game scores and locations for multiple teams, then insert them into Snowflake.
    """
    # Step 1: Set up Snowflake tables
    await setup_tables(block_name=snowflake_block)

    # Step 2: Get recent game IDs for all teams
    game_ids = await get_recent_games(team_ids, start_date, end_date)

    # Step 3: Fetch game scores
    scores = await get_game_scores(game_ids)

    # Step 4: Fetch game locations
    locations = await get_game_locations_flow(game_ids)

    # Step 5: Insert data into Snowflake
    await insert_game_scores(game_scores=scores, block_name=snowflake_block)
    await insert_game_locations(game_locations=locations, block_name=snowflake_block)

    print("MLB Simple Flow Completed Successfully.")

if __name__ == "__main__":
    # Example usage
    TEAM_IDS = [
        133,
        # 134,
        # 135,
        # 136,
        # 137,
        # 138,
        # 139,
        # 140,
        # 141,
        # 121,
    ]
    START_DATE = "2022-02-01"
    END_DATE = "2024-11-30"
    SNOWFLAKE_BLOCK_NAME = "dev-day-staging"  # Replace with your actual block name

    asyncio.run(
        mlb_simple_flow(
            team_ids=TEAM_IDS,
            start_date=START_DATE,
            end_date=END_DATE,
            snowflake_block=SNOWFLAKE_BLOCK_NAME
        )
    )