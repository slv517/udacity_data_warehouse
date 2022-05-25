<h1>DATA WAREHOUSE PROJECT</h1>

<h2>PROJECT DESCRIPTION</h2>

Sparkify is a music streaming start-up that would like to move their processes and data into the Cloud. The data currently resides in S3, in a directory of JSOn logs on user activity on the app, as well as a directory wity JSON metadata on the songs in their app.

In this project, I build an ETL pipeline for a database hosted on Redshift. I will load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.


<h2>PROJECT DATASETS</h2>

I will be working with two datasets that reside in S3. The S3 links for each:

- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data


<h2>DATABASE SCHEMA</h2>

1. Fact Table

Songplays: records in event data associated with song plays i.e. records with page NextSong
- songplay_id, 
- start_time, 
- user_id, 
- level, 
- song_id, 
- artist_id, 
- session_id, 
- location, 
- user_agent

2. Dimension Tables

Users: users in the app
- user_id, 
- first_name, 
- last_name, 
- gender, 
- level

Songs: songs in music database
- song_id, 
- title, 
- artist_id, 
- year, 
- duration

Artists: artists in music database
- artist_id, 
- name, 
- location, 
- lattitude, 
- longitude

Time: timestamps of records in songplays broken down into specific units
- start_time, 
- hour, 
- day, 
- week, 
- month, 
- year, 
- weekday


<h2>PROJECT TEMPLATE</h2>

To get started with the project, go to the workspace on the next page, where you'll find the project template. You can work on your project and submit your work through this workspace.

Alternatively, you can download the template files in the Resources tab in the classroom and work on this project on your local computer.

The project template includes four files:

- create_table.py is where you'll create your fact and dimension tables for the star schema in Redshift.
- etl.py is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
- sql_queries.py is where you'll define you SQL statements, which will be imported into the two other files above.
- README.md is where you'll provide discussion on your process and decisions for this ETL pipeline.

