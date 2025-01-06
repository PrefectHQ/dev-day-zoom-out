from prefect import flow, task
import httpx
import requests
from prefect_snowflake import SnowflakeCredentials

# https://api.open-meteo.com/v1/elevation?latitude=52.52&longitude=13.41

from prefect_snowflake.database import SnowflakeConnector
from prefect._experimental.lineage import emit_lineage_event
import asyncio
from resources import (
    OPEN_METEO_ELEVATION_API,
    SNOWFLAKE_GAME_LOCATIONS,
    SNOWFLAKE_ELEVATION_DATA,
)


@task
async def fetch_unique_city_locations():
    snowflake_connector = await SnowflakeConnector.load("dev-day-connector")

    with snowflake_connector as connector:
        locations = connector.fetch_many(
            f"SELECT DISTINCT venue_city, venue_latitude, venue_longitude FROM {connector.database}.PUBLIC.GAME_LOCATIONS;",
            size=100,
        )

        await emit_lineage_event(
            event_name=f"Get Game Locations to Snowflake; N Rows: {len(locations)}",
            upstream_resources=[SNOWFLAKE_GAME_LOCATIONS],
            downstream_resources=None,
            direction_of_run_from_event="downstream",
        )

    return locations


@task
async def fetch_elevation_for_coordinates(latitude: float, longitude: float) -> float:
    # for _, lat, long in locations:
    #  get_elevation(lat, long)

    response = requests.get(
        f"https://api.open-meteo.com/v1/elevation?latitude={latitude}&longitude={longitude}"
    )

    await emit_lineage_event(
        event_name=f"Fetch Elevation Data for Coordinates; Latitude: {latitude}, Longitude: {longitude}",
        upstream_resources=[OPEN_METEO_ELEVATION_API],
        downstream_resources=None,
        direction_of_run_from_event="downstream",
    )

    return response.json()["elevation"]


@task
async def create_and_insert_elevation_data(
    block_name: str, locations: list, elevation: list
) -> None:
    snowflake_connector = await SnowflakeConnector.load(block_name)
    snowflake_connector.execute(
        f"CREATE TABLE IF NOT EXISTS {snowflake_connector.database}.PUBLIC.ELEVATION_DATA (city varchar, lat float, lon float, elevation float);"
    )
    snowflake_connector.execute_many(
        "INSERT INTO elevation_data (city, lat, lon, elevation) VALUES (%(city)s, %(lat)s, %(lon)s, %(elevation)s);",
        seq_of_parameters=[
            {
                "city": location[0],
                "lat": location[1],
                "lon": location[2],
                "elevation": elev,
            }
            for location, elev in zip(locations, elevation)
        ],
    )

    await emit_lineage_event(
        event_name=f"Upload Elevation Data to Snowflake; N Rows: {len(locations)}",
        upstream_resources=None,
        downstream_resources=[SNOWFLAKE_ELEVATION_DATA],
        direction_of_run_from_event="upstream",
    )


@flow
async def process_and_store_elevation_data(snowflake_block_name: str):
    locations = await fetch_unique_city_locations()

    elevations = []
    for _, lat, long in locations:
        elevation = await fetch_elevation_for_coordinates(lat, long)

        print(elevation)
        elevations.append(elevation)

    await create_and_insert_elevation_data(snowflake_block_name, locations, elevations)


if __name__ == "__main__":
    SNOWFLAKE_BLOCK_NAME = "dev-day-staging"
    asyncio.run(process_and_store_elevation_data(SNOWFLAKE_BLOCK_NAME))
