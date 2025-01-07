{{ config(materialized='table', alias='VENUE_ELEVATION') }}


select distinct game_locations.venue_id, game_locations.venue_name, game_scores.home_team_id, game_scores.home_team, game_locations.venue_city, game_locations.venue_state, game_locations.venue_postal_code, game_locations.venue_country, elevation_data.elevation
from {{ source('dev_day_track_2', 'game_locations') }}
join {{ source('dev_day_track_2', 'elevation_data') }}
on game_locations.venue_city = elevation_data.city
join {{ source('dev_day_track_2', 'game_scores') }}
on game_locations.game_id = game_scores.game_id
