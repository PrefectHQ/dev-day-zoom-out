{{ config(materialized='table', alias='PERFORMANCE_RELATIVE_ELEVATION') }}

select
  game_scores.game_id,
  game_scores.home_team_id,
  game_scores.home_team,
  game_scores.away_team_id,
  game_scores.away_team,
  game_scores.away_score - game_scores.home_score as away_team_lead,
  away_team_elevation.elevation - game_location_elevation.elevation as away_team_elevation_difference
from {{ source('dev_day_track_2', 'game_scores') }}
join {{ ref('venue_elevation') }} as game_location_elevation
on game_scores.home_team_id = game_location_elevation.home_team_id
join {{ ref('venue_elevation') }} as away_team_elevation
on game_scores.away_team_id = away_team_elevation.home_team_id
order by away_team_lead asc