from staging_workloads.catalog_flow import *
from staging_workloads.tracking_flow import *
from prefect import serve
from staging_workloads.utils import load_credentials
import os
from pathlib import Path
from prefect import serve, flow
from prefect_dbt_flow import dbt_flow
from prefect_dbt_flow.dbt import DbtDagOptions, DbtProfile, DbtProject

my_dbt_flow = dbt_flow(
    project=DbtProject(
        name="vinted_pipeline",
        project_dir=str(Path(__file__).parent) + "\dbt_transforms",
        profiles_dir=str(Path(__file__).parent) + "\dbt_transforms",
    ),
    profile=DbtProfile(
        target="dev",
    ),
    dag_options=DbtDagOptions(
        run_test_after_model=True,
    ),
)

@flow(name= "Trigger dbt transforms", 
      log_prints= True)
def dbt_transforms_flow():
    my_dbt_flow()


if __name__ == "__main__":
    load_credentials()
    # aws_rds_url = f"postgresql://postgres:4202@localhost:5432/vinted-ai" local string
    
    aws_rds_url = f"postgresql://{os.environ['user']}:{os.environ['password']}@{os.environ['host']}:{os.environ['port']}/{os.environ['database']}?sslmode=require"
    vinted_main = fetch_data_from_vinted.to_deployment(name="local-staging-main",
                        tags=["staging", "extraction", "api"],
                        parameters={"batch_size": 500,
                                    "nbrRows": 500,
                                    "item_ids": [221, 1242, 2320, 1811, 267, 1812, 98, 246, 287, 2964, 1806, 1815, 1809, 2476, 2469, 1221, 288],
                                    "server_conn_string": aws_rds_url,
                                    "interval": 60
                                    # [t shirts women, trainers, sweaters, books, hoodies and sweaters, 
                                    # zip hoodies, sunglasses, backpacks, caps, gorros, t-shirts men, knitted sweaters, polos, capas de telemovel, capas de telemovel, mulher acessorios tecnologicos, chapeus]
                                    },
                        interval=3600) # 1h interval
    
    vinted_tracking = tracking.to_deployment(name="local-staging-tracking",
            tags=["tracking", "api", "staging"],
            interval=60*15,
            parameters={"server_conn_string": aws_rds_url,
                        "interval": 200, # interval between chunks
                        "chunk_size": 10,
                        "sample_size": 60})

    dbt_fact = dbt_transforms_flow.to_deployment(name="fact-tables",
            tags=["tracking", "dbt", "fact"],
            interval=60*60)
    
    serve(vinted_main, vinted_tracking, dbt_fact,
          pause_on_shutdown=False)