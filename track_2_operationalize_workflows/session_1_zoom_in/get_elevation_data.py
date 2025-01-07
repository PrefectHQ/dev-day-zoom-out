from prefect import flow, task
import httpx
import requests
from prefect_snowflake import SnowflakeCredentials

# https://api.open-meteo.com/v1/elevation?latitude=52.52&longitude=13.41

from prefect_snowflake.database import SnowflakeConnector
from prefect._experimental.lineage import emit_lineage_event
import asyncio




@task
async def get_city_data():
    snowflake_connector = await SnowflakeConnector.load("dev-day-connector")

    with snowflake_connector as connector:
        locations = connector.fetch_many(
            "SELECT DISTINCT venue_city, venue_latitude, venue_longitude FROM DEV_DAY.PUBLIC.GAME_LOCATIONS;", size=100
        )


        await emit_lineage_event(
            event_name="Get Game Locations to Snowflake",
            upstream_resources=[
                {
                    "prefect.resource.id": "snowflake://DEV_DAY/PUBLIC/GAME_LOCATIONS",
                    "prefect.resource.lineage-group": "global",
                    "prefect.resource.role": "table",
                    "prefect.resource.name": "dev_day.public.game_locations",
                }
            ],
            downstream_resources=None,
            direction_of_run_from_event="downstream",
        )

    return locations


@task
async def get_elevation(latitude: float, longitude: float) -> float:

    # for _, lat, long in locations:
    #  get_elevation(lat, long)

    response = requests.get(
        f"https://api.open-meteo.com/v1/elevation?latitude={latitude}&longitude={longitude}"
    )

    await emit_lineage_event(
        event_name="get_elevation_data",
        upstream_resources=[
            {
                "prefect.resource.id": "api://api.open-meteo.com/v1/elevation",
                "prefect.resource.lineage-group": "global",
                "prefect.resource.role": "api",
                "prefect.resource.name": "api.open-meteo.elevation",
            }
        ],
        downstream_resources=None,
        direction_of_run_from_event="downstream",
    )

    return response.json()["elevation"]


@task
async def setup_table(block_name: str, locations: list, elevation: list) -> None:

    connector = await SnowflakeConnector.load(block_name)
    connector.execute(
            "CREATE TABLE IF NOT EXISTS elevation_data (city varchar, lat float, lon float, elevation float);"
        )
    connector.execute_many(
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
            event_name="Upload Elevation Data to Snowflake",
            upstream_resources=None,
            downstream_resources=[
                {
                    "prefect.resource.id": "snowflake://DEV_DAY/PUBLIC/ELEVATION_DATA",
                    "prefect.resource.lineage-group": "global",
                    "prefect.resource.role": "table",
                    "prefect.resource.name": "dev_day.public.elevation_data",
                }
            ],
            direction_of_run_from_event="upstream",
        )


@flow
async def main_elevation():

    locations = await get_city_data()

    elevations = []
    for _, lat, long in locations:
        elevation = await get_elevation(lat, long)

        print(elevation)
        elevations.append(elevation)

    await setup_table("dev-day-connector", locations, elevations)


if __name__ == "__main__":
    asyncio.run(main_elevation())
