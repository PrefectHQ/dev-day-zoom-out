from prefect import flow
from pathlib import Path

def read_requirements(file_path="requirements.txt"):
    """Read and parse requirements.txt file"""
    requirements = Path(file_path).read_text().splitlines()
    # Filter out empty lines and comments
    return [req.strip() for req in requirements if req.strip() and not req.startswith('#')]

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/PrefectHQ/dev-day-zoom-out.git",
        entrypoint="track_1_build_workflows/session_2_resilent_workflows/4_deploy_and_schedule/mlb_flow_managed.py:mlb_flow",
    ).deploy(
        name="mlb-managed-flow",
        work_pool_name="managed-pool",
        parameters={"team_name": "phillies", "start_date": "06/01/2024", "end_date": "06/30/2024"},
        job_variables={"pip_packages": read_requirements()}
    )