from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from prefect_dbt.core.storage import GitRepository


@flow
def run_dbt():
    project = GitRepository(
        url="https://github.com/dbt-labs/jaffle_shop_duckdb.git",
    )
    project.pull_code()
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project.destination,
            profiles_dir=".",
        )
    )
    runner.invoke(["build"])


if __name__ == "__main__":
    run_dbt()
