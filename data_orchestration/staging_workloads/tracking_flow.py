from tasks.tasks_vinted_tracking import *
from prefect import flow
from sqlalchemy import create_engine

@flow(name = "Tracking", log_prints= True)
def tracking(server_conn_string, interval, chunk_size, sample_size):
    engine = create_engine(server_conn_string)
    data = load_data_from_postgres(engine, sample_size)
    load_balancer(data, engine, chunk_size = chunk_size, interval= interval)
    return


if __name__ == "__main__":
    # Replace these values with your actual database connection details
    """
    tracking.serve(name="vinted-tracking",
            tags=["tracking"],
            pause_on_shutdown=False,
            interval=60*60*24)"""