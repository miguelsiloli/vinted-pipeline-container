from catalog_flow import *
from tracking_flow import *
from prefect import serve
from utils import load_credentials
import os

if __name__ == "__main__":
    load_credentials()
    # aws_rds_url = f"postgresql://postgres:4202@localhost:5432/vinted-ai" local string
    
    aws_rds_url = f"postgresql://{os.environ['user']}:{os.environ['password']}@{os.environ['host']}:{os.environ['port']}/{os.environ['database']}?sslmode=require"
    vinted_main = fetch_data_from_vinted.to_deployment(name="local-staging-main",
                        tags=["staging", "extraction", "api"],
                        parameters={"batch_size": 500,
                                    "nbrRows": 500,
                                    "item_ids": [221, 1242, 2320, 1811, 267, 1812, 98, 246, 287, 2964, 1806, 1815],
                                    "server_conn_string": aws_rds_url,
                                    "interval": 60
                                    # [t shirts women, trainers, sweaters, books, hoodies and sweaters, 
                                    # zip hoodies, sunglasses, backpacks, caps, gorros, t-shirts men, knitted sweaters]
                                    },
                        interval=3600) # 1h interval
    
    vinted_tracking = tracking.to_deployment(name="local-staging-tracking",
            tags=["tracking", "api", "staging"],
            interval=60*15,
            parameters={"server_conn_string": aws_rds_url,
                        "interval": 240, # interval between chunks
                        "chunk_size": 10,
                        "sample_size": 60})
    
    serve(vinted_main, vinted_tracking,
          pause_on_shutdown=False)