from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings


@flow
def run_dbt():
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(project_dir="example", profiles_dir="."),
        raise_on_failure=False,
    )
    runner.invoke(["run"])


if __name__ == "__main__":
    run_dbt()
