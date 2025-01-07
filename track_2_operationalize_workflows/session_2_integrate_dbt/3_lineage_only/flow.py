from prefect import flow
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings


@flow
def run_dbt():
    runner = PrefectDbtRunner(
        settings=PrefectDbtSettings(project_dir="example", profiles_dir=".")
    )
    runner.emit_lineage_events()


if __name__ == "__main__":
    run_dbt()
