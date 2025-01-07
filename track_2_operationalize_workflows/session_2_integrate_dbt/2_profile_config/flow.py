from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings


@flow
def run_dbt():
    runner = PrefectDbtRunner(
        ettings=PrefectDbtSettings(project_dir="example", profiles_dir=".")
    )
    runner.invoke(["run"])


if __name__ == "__main__":
    run_dbt()
