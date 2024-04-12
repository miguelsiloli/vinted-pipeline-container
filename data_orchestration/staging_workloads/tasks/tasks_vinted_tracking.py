from prefect import task, flow
import pandas as pd
from .pyVinted.vinted import Vinted
from .utils import insert_on_conflict_nothing_tracking, insert_on_conflict_nothing_users_staging, insert_on_conflict_images_staging
import time
from datetime import datetime
from prefect.tasks import exponential_backoff
import json
#from prefect_great_expectations import run_checkpoint_validation

@task(name="Polling 'user_ids' from samples table.")
def load_data_from_postgres(engine, sample_size) -> pd.DataFrame:
    """
    """
    query = f'SELECT DISTINCT user_id FROM products_catalog TABLESAMPLE BERNOULLI(5) LIMIT {sample_size}'  
    user_ids = pd.read_sql_query(query, engine)
    return user_ids


@task(name="Batch API calls.",
      description= "Executes API calls in batches. Fails if dataframe is empty (all calls return None).",
      retries=3, 
      retry_delay_seconds=exponential_backoff(backoff_factor=7),
      retry_jitter_factor=2,
      log_prints= True)
def fetch_sample_data(data) -> list[pd.DataFrame]:
    """
    requests.exceptions.HTTPError 429: https://www.rfc-editor.org/rfc/rfc6585#page-3
    """
    # Specify your transformation logic here
    vinted = Vinted()
    _tracking_list = []
    _user_list = []
    for index, row in data.iterrows():
        _item, _user = vinted.items.search_item(user_id = row["user_id"])
        _tracking_list.append(_item)
        _user_list.append(_user)
        
        
    if _tracking_list == []:
        #prefect.engine.signals.SKIP()
        return []
    
    items = pd.concat(_tracking_list, 
                   axis=0, 
                   ignore_index=True)

    users = pd.concat(_user_list,
                    axis = 0, 
                    ignore_index= True)
    
    return [items, users]

@task(name="Drops and type asserts the columns fetched.")
def transform_items(df: pd.DataFrame, **kwargs) -> None:
    """
    """
    cols = ["id", "brand", "size", "catalog_id", "color1_id", "favourite_count", 
            "view_count", "created_at_ts", "original_price_numeric", "price_numeric", 
            "description", "package_size_id", "service_fee", "city", "country", "color1", 
            "status", "item_closing_action", "user_id"]
    
    # add size_id, disposal_conditions, package_size_id, active_bid_count
    
    # [res["items"][i]["user"] for i in range(len(res["items"]))]
    df = df[cols]

    df = df.rename(columns={'id': 'product_id', 
                            "brand": "brand_title", 
                            "size": "size_title", 
                            "created_at_ts": "created_at"})
    
    df["original_price_numeric"] = df["original_price_numeric"].astype(float)
    df["price_numeric"] = df["price_numeric"].astype(float)
    df["date"] = datetime.now().strftime("%Y-%m-%d")
    
    return df

@task(name = "Select images from product_id.")
def transform_images(df: pd.DataFrame) -> pd.DataFrame:
    photos = df["photos"] #.apply(json.loads)
    photos.to_json("photos.json")
    thumbnails = [photo[0]["thumbnails"][1]["url"] for photo in photos]
    data = pd.DataFrame({
        "product_id": df["id"], 
        "image": thumbnails
    })

    return data

@task(name="Selects users columns.")
def transform_users(df: pd.DataFrame, **kwargs) -> None:
    """
    """
    cols = ["id", "gender", "item_count", "given_item_count", "taken_item_count", "followers_count", "following_count", "positive_feedback_count", 
            "negative_feedback_count", "feedback_reputation", "feedback_count", "city_id", "city", "country_id", "country_title", "profile_url"]
    df = df[cols]

    df = df.rename(columns={'id': 'user_id'})

    df["date"] = datetime.now().strftime("%Y-%m-%d")
    return df

@task(name= "Export data to 'tracking_staging'.",
      description= "Export tracking data to staging table: 'tracking'",
      timeout_seconds = 180,
      retries= 2)
def export_items_to_postgres(df: pd.DataFrame, engine) -> None:
    """
    """

    table_name = 'tracking_staging'  # Specify the name of the table to export data to
    #engine = create_engine('postgresql://user:4202@localhost:5432/vinted-ai')
    df.to_sql(table_name, 
              engine, 
              if_exists = "append", 
              index = False, 
              method= insert_on_conflict_nothing_tracking,
              schema= "public")
    
@task(name= "Export data to 'users_staging'.",
      description= "Export tracking data to staging table: 'users_staging'",
      timeout_seconds = 180,
      retries= 2)
def export_users_to_postgres(df: pd.DataFrame, engine) -> None:
    """
    """

    table_name = 'users_staging'  # Specify the name of the table to export data to
    #engine = create_engine('postgresql://user:4202@localhost:5432/vinted-ai')
    df.to_sql(table_name, 
              engine, 
              if_exists = "append", 
              index = False, 
              method= insert_on_conflict_nothing_users_staging,
              schema= "public")
    
@task(name= "Export data to 'images_staging'.",
      description= "Export tracking data to staging table: 'images_staging'",
      timeout_seconds = 180,
      retries= 2)
def export_images_to_postgres(df: pd.DataFrame, engine) -> None:
    """
    """

    table_name = 'images_staging'  # Specify the name of the table to export data to
    #engine = create_engine('postgresql://user:4202@localhost:5432/vinted-ai')
    df.to_sql(table_name, 
              engine, 
              if_exists = "append", 
              index = False, 
              method= insert_on_conflict_images_staging,
              schema= "public")



def load_balancer(df: pd.DataFrame, engine, chunk_size, interval = 360) -> None:
    # total bandwidth = 50*1*24 = 1200
    for start in range(0, df.shape[0], chunk_size):
        tracking_subflow(df = df.iloc[start:start + chunk_size], 
                         name = f"Tracking subflow for: {str(start)}-{str(start + chunk_size)} of {str(df.shape[0])}",
                         engine = engine)
        time.sleep(interval)

@flow(flow_run_name= "Chunk: {name}", 
      log_prints= True)
def tracking_subflow(df, name, engine):
    items, users = fetch_sample_data(df)  
    images = transform_images(items)  
    items = transform_items(items)
    users = transform_users(users)
    export_items_to_postgres(items, engine)
    export_users_to_postgres(users, engine)
    export_images_to_postgres(images, engine)
