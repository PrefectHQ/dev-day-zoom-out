# snowflake_helper.py

from prefect import task
from prefect_snowflake import SnowflakeConnector
import logging
from typing import List, Dict
from prefect._experimental.lineage import emit_lineage_event

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task
def setup_tables(block_name: str):
    """Create Snowflake tables for game scores and locations."""
    try:
        with SnowflakeConnector.load(block_name) as connector:
            # Create GAME_SCORES table
            connector.execute("""
                CREATE TABLE IF NOT EXISTS DEV_DAY.PUBLIC.GAME_SCORES (
                    GAME_ID INTEGER,
                    HOME_TEAM_ID INTEGER,
                    HOME_TEAM VARCHAR,
                    AWAY_TEAM_ID INTEGER,
                    AWAY_TEAM VARCHAR,
                    HOME_SCORE INTEGER,
                    AWAY_SCORE INTEGER,
                    SCORE_DIFFERENTIAL INTEGER,
                    GAME_TIME VARCHAR
                );
            """)
            logger.info("Created table: GAME_SCORES")

            # Create GAME_LOCATIONS table
            connector.execute("""
                CREATE TABLE IF NOT EXISTS DEV_DAY.PUBLIC.GAME_LOCATIONS (
                    GAME_ID INTEGER,
                    VENUE_ID INTEGER,
                    VENUE_NAME VARCHAR,
                    VENUE_CITY VARCHAR,
                    VENUE_STATE VARCHAR,
                    VENUE_POSTAL_CODE VARCHAR,
                    VENUE_COUNTRY VARCHAR,
                    VENUE_LATITUDE FLOAT,
                    VENUE_LONGITUDE FLOAT,
                    VENUE_ELEVATION FLOAT
                );
            """)
            logger.info("Created table: GAME_LOCATIONS")
    except Exception as e:
        logger.error(f"Error setting up tables: {e}")
        raise e

@task
def insert_game_scores(game_scores: List[Dict], block_name: str):
    """Insert game scores data into Snowflake."""
    if not game_scores:
        logger.info("No game scores to insert.")
        return

    try:
        with SnowflakeConnector.load(block_name) as connector:
            connector.execute_many(
                """
                INSERT INTO DEV_DAY.PUBLIC.GAME_SCORES (
                    GAME_ID,
                    HOME_TEAM_ID,
                    HOME_TEAM,
                    AWAY_TEAM_ID,
                    AWAY_TEAM,
                    HOME_SCORE,
                    AWAY_SCORE,
                    SCORE_DIFFERENTIAL,
                    GAME_TIME
                )
                VALUES (
                    %(game_id)s,
                    %(home_team_id)s,
                    %(home_team)s,
                    %(away_team_id)s,
                    %(away_team)s,
                    %(home_score)s,
                    %(away_score)s,
                    %(score_differential)s,
                    %(game_time)s
                );
                """,
                game_scores,
            )
            logger.info(f"Inserted {len(game_scores)} game scores into Snowflake.")
    except Exception as e:
        logger.error(f"Failed to insert game scores: {e}")
        raise e

@task
def insert_game_locations(game_locations: List[Dict], block_name: str):
    """Insert game locations data into Snowflake."""
    if not game_locations:
        logger.info("No game locations to insert.")
        return

    try:
        with SnowflakeConnector.load(block_name) as connector:
            connector.execute_many(
                """
                INSERT INTO DEV_DAY.PUBLIC.GAME_LOCATIONS (
                    GAME_ID,
                    VENUE_ID,
                    VENUE_NAME,
                    VENUE_CITY,
                    VENUE_STATE,
                    VENUE_POSTAL_CODE,
                    VENUE_COUNTRY,
                    VENUE_LATITUDE,
                    VENUE_LONGITUDE,
                    VENUE_ELEVATION
                )
                VALUES (
                    %(game_id)s,
                    %(venue_id)s,
                    %(venue_name)s,
                    %(venue_city)s,
                    %(venue_state)s,
                    %(venue_postal_code)s,
                    %(venue_country)s,
                    %(venue_latitude)s,
                    %(venue_longitude)s,
                    %(venue_elevation)s
                );
                """,
                game_locations,
            )
            logger.info(f"Inserted {len(game_locations)} game locations into Snowflake.")
    except Exception as e:
        logger.error(f"Failed to insert game locations: {e}")
        raise e
