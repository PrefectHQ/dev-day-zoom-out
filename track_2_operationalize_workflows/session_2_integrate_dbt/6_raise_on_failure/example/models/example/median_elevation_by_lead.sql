{{ config(materialized='view', alias='MEDIAN_ELEVATION_BY_LEAD') }}

select
    away_team_lead,
    median(away_team_elevation_difference) as median_elevation_difference
from {{ ref('performance_relative_elevation') }}
group by away_team_lead
order by away_team_lead