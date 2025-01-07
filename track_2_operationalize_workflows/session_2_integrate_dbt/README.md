# MLB Game Analysis Pipeline

## Overview
Now that all of our raw data in in Snowflake, we can use dbt to transform it into something our analysts can use to draw useful insights. We'll take advantage of some new integration capabilities between dbt and Prefect to simplify configuration management, error capture, project organization, alerting, and lineage tracking.

## Prerequisites
- Python 3.8+
- prefect
- dbt-core
- dbt-snowflake
- prefect-dbt-experimental
