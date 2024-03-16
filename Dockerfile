FROM python:3.10.6-slim-buster

# Set the working directory in the container
WORKDIR /vinted-orchestration

# Copy the current directory contents into the container at /vinted-orchestration
COPY . /vinted-orchestration

# Install dependencies
RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 \
    && pip install --no-cache-dir -r requirements.txt \
    && prefect server start
# Make port 80 available to the world outside this container prefect port
EXPOSE 4200

# Run the Python application
ENTRYPOINT ["python", "data_orchestration/staging_workloads/main.py"]
