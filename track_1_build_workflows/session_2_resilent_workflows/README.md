# Track 1, Session 2: Schedule workflows and make them resilient

## Overview
This project contains a series of example pipelines that demonstrate how to use Prefect implementing error handling, data quality checks, and automated retries. We'll also explore how to run a pipeline on a configurable schedule with a managed execution workpool. Each example is an iteration of a data pipeline that fetches and analyzes MLB game statistics using the MLB Stats API. 

## Topics we'll cover:
- Automatic retries and implementing custom error handling for API failures
- Prefect transactions, rollbacks, and data quality checks
- Storage of results in local JSON files, MotherDuck, and S3
- Running pipelines on a configurable schedule

## Prerequisites
Follow the top level README.md to install all proper prerequisites! You will also need to complete the prerequisite in the README.md of the track 1 folder.

## Project Contents
- `1_starting_flow`: A folder containing a basic pipeline that fetches and analyzes MLB game statistics for a specified team and date range.
- `2_retries`: A folder containing a series of pipelines with different retry strategies.
- `3_rollbacks`: A folder containing a pipeline with error handling and data quality checks.
- `4_deploy_and_schedule`: A folder containing a deployed version of the `mlb_flow.py` pipeline.

In each folder mentioned above you will find the following subfolders:
- `raw_data`: A folder for storing raw data from the MLB Stats API.
- `boxscore_analysis`: A folder for storing boxscore analysis results.
- `boxscore_parquet`: A folder for storing boxscore analysis results in parquet format.

