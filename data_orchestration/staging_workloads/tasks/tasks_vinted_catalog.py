from .pyVinted.vinted import Vinted
import pandas as pd
from typing import List
import datetime
from prefect import task, flow
from prefect.context import FlowRunContext
from .utils import insert_on_conflict_nothing
from operator import itemgetter
import re
from prefect.artifacts import create_table_artifact
from prefect.tasks import exponential_backoff

@task(name="Load from api", 
      log_prints= True,
      retries=3, 
      retry_delay_seconds=exponential_backoff(backoff_factor=6),
      retry_jitter_factor=2)
def load_data_from_api(vinted, nbrRows: int, batch_size: int, item: str) -> pd.DataFrame:
    """
    Loads data from the Vinted API based on specified parameters.

    Args:
        nbrRows (int): Number of rows to fetch from the API.
        batch_size (int): Batch size for API requests.
        item (List): List of items to include in the API request.

    Returns:
        pd.DataFrame: DataFrame containing data fetched from the Vinted API.
    """
    df = vinted.items.search_catalog(url = f"https://www.vinted.pt/catalog/items?catalog_ids[]={item}&order=newest_first")
    # cant process latin characters
    df["catalog_id"] = item
    
    return (df)

@flow(name="Subflow for catalog.", 
      flow_run_name= "Subflow for catalog {item}",
      log_prints= True,
      timeout_seconds = 600)
def catalog_subflow(item, nbrRows, batch_size, vinted, engine):
    df = load_data_from_api(vinted = vinted,
                            nbrRows = nbrRows,
                            batch_size = batch_size,
                            item = item)
    df = transform(data = df)
    #df = parse_size_title(data = df)
    #create_artifacts(data = df)
    export_data_to_postgres(data = df, 
                            engine = engine)     # upload first to products due to FK referencing
    return

@task(name="Drop columns and rename.")
def transform(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the input DataFrame by dropping columns, renaming columns, and converting data types.

    Args:
        data (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    date = datetime.datetime.now()
    data["user_id"] = data.user.apply(pd.Series)["id"]
    data["color"] = data.photo.apply(pd.Series)["dominant_color"]
    data["date"] = date
    # Specify your transformation logic here
    data["price"] = data["price"].astype(float)

    try:
        data = data.drop(["is_for_swap", "user", "photo", "is_favourite", "discount", "badge", "conversion", "service_fee", 
                "total_item_price_rounded", "icon_badges", "is_visible", "search_tracking_params", "favourite_count",
                "total_item_price", "content_source", "menu_options"],
                axis = 1)
    except:
        data = data.drop(["user", "photo", "is_favourite", "discount", "badge", "conversion", "service_fee", 
                "total_item_price_rounded", "icon_badges", "is_visible", "search_tracking_params", "favourite_count",
                "total_item_price", "content_source", "menu_options"],
                axis = 1)
        
    data = data.rename(columns={'id': 'product_id'})
    data["flow_name"] = FlowRunContext.get().flow_run.dict().get('name')
    data['date'] = pd.to_datetime(data['date']).dt.strftime('%Y-%m-%d %H:%M')
    return (data)

@task(name="Parse size_title into unique sizes (S, M, XL, XXL).")
def parse_size_title(data: pd.DataFrame) -> pd.DataFrame:
    """
    Parses the 'size_title' column into unique sizes (S, M, XL, XXL).

    Args:
        data (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with 'size_title' column parsed into unique sizes.
    """
    # Specify your transformation logic here
    def check_string(string_to_check):
        # check size_title and parse sizes into unique sizes
        # M / 36 = M
        if re.match(r'^[a-zA-Z]', string_to_check):
            return (''.join(re.findall(r'[a-zA-Z]', string_to_check)))

        else:
            # first letter starts with a number
            return(string_to_check)
        
    data["size_title"] = data["size_title"].apply(check_string)

    return (data)

@task(name="Transform metadata.")
def transform_metadata(data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms metadata by adding missing values and total rows information.

    Args:
        data (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Transformed DataFrame with metadata information.
    """

    flow_meta = FlowRunContext.get().flow_run.dict()
    flow_meta.update({"missing_values": data.isna().sum().sum(),
                      "total_rows": len(data.index)})
    flow_meta = itemgetter(*["name", "parameters", "missing_values", "total_rows"])(flow_meta)
    data = pd.DataFrame.from_dict(flow_meta).T
    data.columns = ["name", "missing_values", "total_rows"]

    return (data)

@task(name = "Create artifacts.")
def create_artifacts(data: pd.DataFrame) -> None:
    """
    Creates artifacts by generating summary statistics.

    Args:
        data (pd.DataFrame): Input DataFrame.

    Returns:
        None
    """
    create_table_artifact(key = "output-describe",
                          table = data.describe().reset_index().to_dict(orient='records'),
                          description= "output describe pandas")


@task(name="Export sample to pg")
def export_metadata_to_postgres(data: pd.DataFrame, engine) -> None:
    """
    Exports metadata to a PostgreSQL table.

    Args:
        data (pd.DataFrame): Input DataFrame containing metadata.
        engine: The SQLAlchemy engine for the PostgreSQL database.

    Returns:
        None
    """
    #schema_name = 'public'  # Specify the name of the schema to export data to
    table_name = 'flow_metadata'  # Specify the name of the table to export data to
    data.to_sql(table_name, 
                engine, 
                if_exists = "append", 
                index = False, 
                method = insert_on_conflict_nothing)
    
    return

@task(name="Export data to pg", log_prints=True)
def export_data_to_postgres(data: pd.DataFrame, engine) -> None:
    """
    Exports a pandas DataFrame to a specified PostgreSQL table using the provided database engine.

    Args:
        data (pd.DataFrame): The DataFrame to be exported to PostgreSQL.
        engine: The SQLAlchemy engine for the PostgreSQL database.

    Returns:
        None
    """
    #schema_name = 'public'  # Specify the name of the schema to export data to
    table_name = 'products_catalog'  # Specify the name of the table to export data to
    #engine = create_engine('postgresql://user:4202@localhost:5432/vinted-ai')
    data.to_sql(table_name, 
                engine, 
                if_exists = "append", 
                index = False, 
                method = insert_on_conflict_nothing)

    return