import os
import pandas as pd
from sqlalchemy import create_engine
import json

def load_credentials(path = "aws_rds_credentials.json"):
     with open(path, 'r') as file:
          config = json.load(file)

     # set up credentials
     for key in config.keys():
          os.environ[key] = config[key]

     return

load_credentials()

aws_rds_url = f"postgresql://{os.environ['user']}:{os.environ['password']}@{os.environ['host']}:{os.environ['port']}/{os.environ['database']}?sslmode=require"
engine = create_engine(aws_rds_url)

# make sure to include dbt schema: dbt_msilva
data = pd.read_sql("SELECT max(date) from dbt_msilva.users_fact", engine)
print(data)