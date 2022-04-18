# CURRENTLY A COPY OF ---- Sparkify DB creation + ETL Pipeline

This repository contains the necessary scripts to create a PostgreSQL DB for a music streaming app (Sparkify), with a performant architecture, as well as the scripts to extract data from 2 sets of files and load it to the DB.

## 0. Context & Architecture 

In order to enable performant queries to be done to the database, a star-schema composed of 5 tables was used: 1 fact table - `songplays` - and 4 dimension tables - `users`, `songs`, `artists`, `time`. 

![image](https://user-images.githubusercontent.com/27001378/160474147-43485e7f-4dd3-4708-a7fc-b47d4f4ef833.png)

The startup has put up systems that collect 2 types of data:

- Songs 
- Activity Logs

Through the scripts developed in this repo, data is extracted from 2 groups of files and loaded into the 5 tables of the DB. 

With the data cleanly placed into the 5 tables, the analytics team can now easily create dashboards which focus on the different areas of the business:

1. Acquiring and retaining users
2. Increase the songs catalog
3. Acquire more artists
4. Engage users daily

which correspond to 4 independent tables, while minimizing JOINS. The time table is used when time granularity is needed in the query. 

Advanced queries & usecases like recommending songs to a user, based on listening history can be performed easily with the `songplays` table.

## 1. Running Python Scripts

In order to create the tables run:

```
python create-tables.py
```

To load the data from the songs and activity logs run:

```
python etl.py
```

## 2. Directory Structure

* dwh.cfg - config file
* create_cluster.ipynb - Jupyter notebook to spin-up the resources: IAM, Redshift, VPC; necessary for the project using IaC
* create_tables.py - script to create tables on Redshift cluster
* etl.py - script to (1) extract data from s3, (2) load it to staging tables, (3) Transform and Load data into final tables for OLAP
* sql_queries.py - helper script with all SQL queries
* test.ipynb - Jupyter notebook for testing
* Readme.md
