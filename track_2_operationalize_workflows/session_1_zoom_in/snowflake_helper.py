# snowflake_helper.py

from prefect import task
from prefect_snowflake import SnowflakeConnector
import logging
from typing import List, Dict
import asyncio
from prefect._experimental.lineage import emit_lineage_event

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task
async def setup_tables(block_name: str):
    """Create Snowflake tables for game scores and locations."""
    try:
        # Load the Snowflake connector asynchronously
        snowflake_connector = await SnowflakeConnector.load(block_name)

        # Define the SQL statements
        game_scores_table_sql = """
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
        """

        game_locations_table_sql = """
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
        """

        # Execute the SQL statements asynchronously
        snowflake_connector.execute(game_scores_table_sql)
        logger.info("Created table: GAME_SCORES")

        snowflake_connector.execute(game_locations_table_sql)
        logger.info("Created table: GAME_LOCATIONS")

        # Optionally, emit a lineage event after setting up tables
        # await emit_lineage_event(...)

    except Exception as e:
        logger.error(f"Error setting up tables: {e}")
        raise e


@task
async def insert_game_scores(game_scores: List[Dict], block_name: str):
    """Insert game scores data into Snowflake."""
    if not game_scores:
        logger.info("No game scores to insert.")
        return

    try:
        # Load the Snowflake connector asynchronously
        snowflake_connector = await SnowflakeConnector.load(block_name)

        # Define the SQL statement
        insert_sql = """
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
        """

        # Execute the insert asynchronously
        snowflake_connector.execute_many(insert_sql, game_scores)
        await emit_lineage_event(
            event_name="Upload Game Scores to Snowflake",
            upstream_resources=None,
            downstream_resources=[
                {
                    "prefect.resource.id": "snowflake://DEV_DAY/PUBLIC/GAME_SCORES",
                    "prefect.resource.lineage-group": "global",
                    "prefect.resource.role": "table",
                    "prefect.resource.name": "dev_day.public.game_scores",
                }
            ],
            direction_of_run_from_event="upstream",
        )

        logger.info(f"Inserted {len(game_scores)} game scores into Snowflake.")

    except Exception as e:
        logger.error(f"Failed to insert game scores: {e}")
        raise e


@task
async def insert_game_locations(game_locations: List[Dict], block_name: str):
    """Insert game locations data into Snowflake."""
    if not game_locations:
        logger.info("No game locations to insert.")
        return

    try:
        # Load the Snowflake connector asynchronously
        snowflake_connector = await SnowflakeConnector.load(block_name)

        # Define the SQL statement
        insert_sql = """
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
        """

        # Execute the insert asynchronously
        snowflake_connector.execute_many(insert_sql, game_locations)
        await emit_lineage_event(
            event_name="Upload Game Locations to Snowflake",
            upstream_resources=None,
            downstream_resources=[
                {
                    "prefect.resource.id": "snowflake://DEV_DAY/PUBLIC/GAME_LOCATIONS",
                    "prefect.resource.lineage-group": "global",
                    "prefect.resource.role": "table",
                    "prefect.resource.name": "dev_day.public.game_locations",
                }
            ],
            direction_of_run_from_event="upstream",
        )
        logger.info(f"Inserted {len(game_locations)} game locations into Snowflake.")

        # Optionally, emit a lineage event after inserting game locations
        # await emit_lineage_event(...)

    except Exception as e:
        logger.error(f"Failed to insert game locations: {e}")
        raise e
