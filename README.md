Sure, here's the project with your name substituted in for Sidharth:

---

# Data Engineering Project-2 | Building Spotify ETL using Python and Airflow

Create an Extract Transform Load pipeline using Python and automate it with Airflow.

![Spotify ETL](https://miro.medium.com/max/749/1*dm8hVrPTPMenyRY4uJiBIA@2x.png)

Image by Author

In this blog post, I will explain how to create a simple ETL (Extract, Transform, Load) pipeline using Python and automate the process through Apache Airflow.

# Problem Statement:

We need to use Spotify’s API to read the data and perform some basic transformations and Data Quality checks. Finally, we will load the retrieved data into a PostgreSQL DB and then automate the entire process through Airflow. **Est. Time:** [4–7 Hours]

# Tech Stack / Skills Used:

1. Python
2. APIs
3. Docker
4. Airflow
5. PostgreSQL

# Prerequisite:

1. Knowledge of APIs
2. Understanding of Docker and Docker-Compose
3. Intermediate Python and SQL
4. A basic understanding of Airflow [this](https://www.youtube.com/watch?v=AHMm1wfGuHE&t=705s) will help

# Learning Outcomes:

1. Understand how to interact with API to retrieve data
2. Handling DataFrame in pandas
3. Setting up Airflow and PostgreSQL through Docker-Compose
4. Learning to create DAGs in Airflow

# Introduction:

This is a beginner-friendly project to get started with building a simple pipeline and automating it through Airflow. First, we will focus on entirely building the pipeline and then extend the project by combining it with Airflow.

# Building ETL Pipeline:

**Dataset:** In this project, we are using Spotify’s API so please go ahead and create an account for yourself. After creating the account head to this [page](https://developer.spotify.com/console/get-recently-played/?limit=&after=&before=). Now you will be able to see a "Get Token" icon. Click that and select "user recently played" and click "Get Token".

![Get Token](https://miro.medium.com/max/690/1*4UKYwl00ALuF9PQj-TTJyA.png)

Image by Author

You can see your token like this.

![Token](https://miro.medium.com/max/749/1*CPZYseTyKH-CruoJpyNl-w.png)

Image by Author

Now, this is the procedure to get the token. You may need to generate this often as it expires after some time.

## Extract.py

We are using this token to extract the data from Spotify. We are creating a function `return_dataframe()`. The below Python code explains how we extract API data and convert it to a DataFrame.

## Transform.py

Here we are exporting the Extract file to get the data.

**def Data_Quality(load_df):** Used to check for the empty DataFrame, enforce unique constraints, checking for null values. Since these data might ruin our database it's important we enforce these Data Quality checks.

**def Transform_df(load_df):** Now we are writing some logic according to our requirement. Here we wanted to know our favorite artist, so we are grouping the songs listened to by the artist. Note: This step is not required; you can implement it or any other logic if you wish, but make sure you enforce the primary constraint.

## Load.py

In the load step, we are using SQLAlchemy and SQLite to load our data into a database and save the file in our project directory.

Finally, we have completed our ETL pipeline successfully. The structure of the project folder should look like this (inside the project folder we have 3 files).

```
E:\DE\PROJECTS\SPOTIFY_ETL\SPOTIFY_ETL  
│   Extract.py  
│   Load.py  
│   my_played_tracks.sqlite  
│   spotify_etl.py  
│   Transform.py  
└───
```

After running the **Load.py** you could see a .sqlite file saved to the project folder. To check the data inside the file head [here](https://inloop.github.io/sqlite-viewer/) and drop your file.

![SQLite Viewer](https://miro.medium.com/max/749/1*OpGD1spYMVIulWVCKPttlw.png)

Image by Author

Now we will automate this process using Airflow.

# Automating through Airflow

For those who have made it this far, I appreciate your efforts 👏 but from here it gets a little tricky. Hence, I am mentioning some important points below.

1. We have completed an ETL, and this itself is a mini project, hence save the work.
2. Now we are going to extend this with Airflow using Docker.
3. Why Docker? We are using Docker since it’s easier to install and maintain and it's OS independent.
4. How to set up Airflow using Docker? Follow the guide provided in this [blog](https://medium.com/@garc1a0scar/how-to-start-with-apache-airflow-in-docker-windows-902674ad1bbe).
5. You need to change the Yaml file alone from the above guidelines; please refer [here](https://github.com/sidharth1805/Spotify_etl/blob/main/docker-compose.yml).
6. After setting up the Airflow, place your DAGs inside the dags folder.
7. After the Docker is up, you could see 4 services running.

![Airflow Services](https://miro.medium.com/max/749/1*txaw4D2bowisN98SbG6PZQ.png)

Image by Author

Your Airflow folder should look like the below structure.

```
C:\USERS\JAINISH\DOCKER\AIRFLOW  
│   docker-compose.yml  
├───dags  
│   │   YOUR_DAGS.py  
├───logs  
├───plugins  
└───scripts
```

Now that we have set up Airflow, we can view the Airflow UI by visiting the [8080 port](http://localhost:8080/). The username and password would be `airflow`.

It’s time to create the required DAG for our project. But before jumping into DAG, let us understand what a DAG is. DAG stands for Directed Acyclic Graph, which is a set of tasks defined in the order of execution.

![DAG](https://miro.medium.com/max/749/1*cImMkJ3NRWWLmw2o4mH9NQ.png)

Image by Author

So inside our DAG, we need to create tasks to get our job done. To keep it simple, I will use two tasks i.e. one to create a Postgres table and another to load the data to the table. Our DAG will look like this.

![DAG Tasks](https://miro.medium.com/max/679/1*hYbRd0gKRffQZn1Xipt-BA.png)

## spotify_etl.py

In this Python file, we will write the logic to extract data from the API, do quality checks, and transform data.

1. **yesterday = today — datetime.timedelta(days=1)** → Defines the number of days you want data for. Change as you wish; since our job is the daily load, I have set it to 1.
2. **def spotify_etl()** → Core function which returns the DataFrame to the DAG Python file.
3. This file needs to be placed inside the dags folder.

## spotify_final_dag.py

This is the most important section you need to pay attention to. First, learn the basics about Airflow DAGs [here](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html). It might take around 15 mins, or you can search for it on YouTube. After the basics, please follow the below guideline.

1. **from airflow.operators.python_operator import PythonOperator** → We are using the Python operator to perform Python functions such as inserting DataFrame to the table.
2. **from airflow.providers.postgres.operators.postgres import PostgresOperator** → We are using the Postgres operator to create tables in our Postgres database.
3. **from airflow.hooks.base_hook import BaseHook** → A hook is an abstraction of a specific API that allows Airflow to interact with an external system. Hooks are built into many operators, but they can also be used directly in DAG code. We are using a hook here to connect to the Postgres database from our Python function.
4. **from spotify_etl import spotify_etl** → Importing `spotify_etl` function from `spotify_etl.py`.

## Code Explanation:

Setting up the default arguments and interval time. We can change the interval time and start date according to our needs.

![Airflow Code](https://miro.medium.com/max/749/1*YoZQLQWbgXUn4WOo_Z1o7Q.png)

Understanding Postgres connection and task.

1. **conn = BaseHook.get_connection('Your Connection ID')** → Connects to your Postgres DB.
2. **df.to_sql('Your Table Name', engine, if_exists='replace')** → Loads the DataFrame to the table.
3. **create_table >> run_etl** → Defining the flow of the task.

![Airflow Task](https://miro.medium.com/max/749/1*WjsH1W213_nOkQv8pgrAvA.png)

Setting up the Postgres Connection on Airflow UI:
Head to the Airflow UI and click "Connections".


Add your credentials and connection as below.


Now that we have configured everything, head back to the Airflow UI home page and run the DAG. You should be able to see the DAG running successfully. It takes around 30 mins to get the initial setup ready; you can then visualize the data inside your database.
