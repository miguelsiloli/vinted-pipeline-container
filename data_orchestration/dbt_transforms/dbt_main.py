from pathlib import Path
from prefect import serve
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtDagOptions, DbtProfile, DbtProject

path = Path(__file__).parent
print(path)

my_dbt_flow = dbt_flow(
    project=DbtProject(
        name="vinted_pipeline",
        project_dir= path,
        profiles_dir= path,
    ),
    profile=DbtProfile(
        target="dev",
    ),
    dag_options=DbtDagOptions(
        run_test_after_model=True,
    ),
)

if __name__ == "__main__":
    vinted_tracking = my_dbt_flow.to_deployment(name="fact-tables",
            tags=["tracking", "dbt", "fact"],
            interval=60*60)
    serve(vinted_tracking,
          pause_on_shutdown=False)