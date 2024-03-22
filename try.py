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
# data = pd.read_sql("SELECT * from dbt_msilva.tracking_fact ORDER BY date DESC LIMIT 1000", engine)
data = pd.read_sql("SELECT * from tracking_staging ORDER BY date DESC LIMIT 1000", engine)
print(data[["date"]])

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import FeatureUnion
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics.pairwise import cosine_similarity

data = data[["catalog_id", "brand_title", "size_title", "status", "price_numeric", "description"]]
data = data.fillna("none")
data.replace("", "none", inplace=True)
data["concat_columns"] = data["brand_title"] + " " +  data["size_title"] + " " + data["status"]
print(data["concat_columns"])


vec = TfidfVectorizer(max_features=1000)
tfidf_matrix = vec.fit_transform(data["concat_columns"])
print(tfidf_matrix.shape)

from sklearn.metrics.pairwise import linear_kernel

# Apply preprocessing to the data
cosine_sim = linear_kernel(tfidf_matrix[0:5], tfidf_matrix).flatten()

titles = data["concat_columns"]
indices = pd.Series(data.index, index=data["concat_columns"])

def get_recommendations(title):
    idx = indices[title]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:31]
    movie_indices = [i[0] for i in sim_scores]
    return titles.iloc[movie_indices]

print(get_recommendations('Ralph Lauren').head(10))