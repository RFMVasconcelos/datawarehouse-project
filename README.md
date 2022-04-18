# 2-stage ETL of songs & events data from s3 to Amazon Redshift Cloud DataWarehouse

This repository contains the necessary scripts to create a Redshift DataWarehouse for a music streaming app (Sparkify), with a performant architecture, as well as the scripts to extract data from 2 sets of files and load it to staging and final db tables on Redshift.

## Context & Architecture 

In order to enable performant queries to be done to the database, a star-schema composed of 5 tables is desireable: 1 fact table - `songplays` - and 4 dimension tables - `users`, `songs`, `artists`, `time`. 

The startup has put up systems that collect 2 types of data:

- Songs data stored in: `s3://udacity-dend/song_data`
- Events data stored in: `s3://udacity-dend/log_data` in the format show in `s3://udacity-dend/log_json_path.json`

To extract the data from s3 and load it into our star schema, we make use of 2 staging tables, as seen in the image below.

![image](https://user-images.githubusercontent.com/27001378/163799534-54811d81-9cfc-41a2-9c9f-b8e2332f88dd.png)

Through the scripts developed in this repo, data is extracted from 2 groups of files and loaded into the 5 tables of the DB. 

With the data cleanly placed into the 5 tables, the analytics team can now easily create dashboards which focus on the different areas of the business:

1. Acquiring and retaining users
2. Increase the songs catalog
3. Acquire more artists
4. Engage users daily

which correspond to 4 independent tables, while minimizing JOINS. The time table is used when time granularity is needed in the query. 

Advanced queries & usecases like recommending songs to a user, based on listening history can be performed easily with the `songplays` table.

## Running Python Scripts

In order to create the tables run:

```
python create-tables.py
```

To load the data from the songs and activity logs run:

```
python etl.py
```

## Directory Structure

Main scripts:
* dwh.cfg - config file (adapted to IaC)
* create_tables.py - script to create tables on Redshift cluster
* etl.py - script to (1) extract data from s3, (2) load it to staging tables, (3) Transform and Load data into final tables for OLAP
* sql_queries.py - script with all SQL queries
* Readme.md

Helper scripts & output:
* create_cluster.ipynb - notebook to spin-up the resources: IAM, Redshift, VPC; necessary for the project using IaC
* test.ipynb - notebook for testing
* draw_db.ipynb - notebook to draw db diagram
* db_diagram.png
