MLB_API_SCHEDULE = {
    "prefect.resource.id": "api:statsapi.mlb.com/api/{ver}/schedule",
    "prefect.resource.lineage-group": "global",
    "prefect.resource.role": "api",
    "prefect.resource.name": "api.mlb.statsapi.mlb.schedule",
}


MLB_API_SCORE = {
    "prefect.resource.id": "api:statsapi.mlb.com/api/{ver}/game/{gamePk}/boxscore",
    "prefect.resource.lineage-group": "global",
    "prefect.resource.role": "api",
    "prefect.resource.name": "api.mlb.statsapi.mlb.game.gamePk.boxscore",
}

MLB_API_LOCATION = {
    "prefect.resource.id": "api:statsapi.mlb.com/api/{ver}/game/{gamePk}/feed/live",
    "prefect.resource.lineage-group": "global",
    "prefect.resource.role": "api",
    "prefect.resource.name": "api.mlb.statsapi.mlb.game.gamePk.feed.live",
}

SNOWFLAKE_GAME_SCORES = {
    "prefect.resource.id": "snowflake:DEV_DAY/PUBLIC/GAME_SCORES",
    "prefect.resource.lineage-group": "global",
    "prefect.resource.role": "table",
    "prefect.resource.name": "snowflake.dev_day.public.game_scores",
}

SNOWFLAKE_GAME_LOCATIONS = {
    "prefect.resource.id": "snowflake:DEV_DAY/PUBLIC/GAME_LOCATIONS",
    "prefect.resource.lineage-group": "global",
    "prefect.resource.role": "table",
    "prefect.resource.name": "snowflake.dev_day.public.game_locations",
}

OPEN_METEO_ELEVATION_API = {
    "prefect.resource.id": "api:api.open-meteo.com/v1/elevation",
    "prefect.resource.lineage-group": "global",
    "prefect.resource.role": "api",
    "prefect.resource.name": "api.elev.open-meteo.elevation",
}

SNOWFLAKE_ELEVATION_DATA = {
    "prefect.resource.id": "snowflake:DEV_DAY/PUBLIC/ELEVATION_DATA",
    "prefect.resource.lineage-group": "global",
    "prefect.resource.role": "table",
    "prefect.resource.name": "snowflake.dev_day.public.elevation_data",
}
