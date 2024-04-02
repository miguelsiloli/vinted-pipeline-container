<h3 style="text-align: center;">Vinted Analytics Pipeline</h3>


# Project Overview

## Intro

According to a report by ThredUp, a leading online resale platform, the resale market is projected to reach $64 billion by 2024, highlighting the growing appeal of pre-owned fashion among consumers. (ThredUp's 2021 Resale Report)

Main market drivers are:
- Changing consumer patterns
- Economic opportunities and affordability
- Circular economy and environmental concerns
- Online platforms such as Vinted, Depop, Poshmar and ThredUp

![Local Image](assets/project_schema.png)

##### Limitations

- Vinted only provides the 
- Vinted API endpoints doesn't provide a direct way to get the brands, sizes, colours, gender, and status variables
- Vinted API requests are rate limited

## Roadmap

### Todo Main Features

- [x] Create an API to connect to Vinted Endpoints 
- [x] Create a basic data pipeline with prefect and postgres
- [ ] Documentation
- [x] Refactor tracking flow to schedule multiple subflows and use persistent results
- [ ] Cluster assessment of products in dashboard
- [ ] Use text tokens to predict prices (remove brand names)
- [ ] Cluster brand names by similarity (yeezy/adidas, nike/jordan, etc)

### Backlog

- [x] Work on the dashboard
- [ ] Develop a pipeline to track saleability
- [x] Develop a pipeline to track brand, catalog_id
- [x] Create dim tables for brands, sizes and colours
- [x] Normalize the database
- [x] Find a way to overcome API rate limits (currently implemented Exponential Backoff on requests)

## Dashboard

Currently just a few descriptive simple graphs. No callback functions.

### Looker Studio

[Daily Snapshot Report](https://lookerstudio.google.com/s/rPPHeQ0OlOk) of the Vinted Pipeline status.

![Local Image](assets/looker_studio.png)






