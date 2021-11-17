# peterparker

Welcome to this project. The name peterparker is inspired by the random user airflow created during a [quickstart instalation tutorial](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).
This project attempts to build a simple data warehouse out of [this dataset](https://www.kaggle.com/edgartanaka1/tmdb-movies-and-series).


## Pre pipeline
1. The pipeline assumes that the **source dataset is stored in AWS S3** somehow.
   - In this simulation, [the dataset](https://www.kaggle.com/edgartanaka1/tmdb-movies-and-series) is downloaded manually to a local machine.
   - Then uploaded to s3 using [upload_to_s3.py](https://github.com/dindapw/peterparker/blob/main/peterparker/upload_to_s3.py) script.
2. The pipeline is deployed in AWS EC2, and scheduled in this [Airflow](http://3.26.36.18:8080/).
   - Credential for S3 and PostgreSQL is saved in configuration file inside the EC2 server.
   - Environment variables is an option to set credentials but it's quite annoying to use back and forth between local machine venv and server.
   - Tutorial on how to setup free EC2 to host airflow can be read [here](https://medium.com/@dindapw/install-airlfow-2-0-on-awss-free-tier-ec2-8ab4b70d8d)
3. The choosen database for the data warehouse is PostgreSQL.
4. The pipeline uses two separate script [replicate_movies.py](https://github.com/dindapw/peterparker/blob/main/peterparker/replicate_movies.py) and [replicate_series.py](https://github.com/dindapw/peterparker/blob/main/peterparker/replicate_series.py), because of a few things:
   - The json struncture for movies and series is a bit different.
   - Making things generic is not always a good thing, sometimes it can be too complicated of a solution for simple problem.
   - The classic, not enough time to do it "better".


## Pipeline
1. Open folder
2. Read a file from folder.
3. Convert file content to json.
4. Transform and split json into different entity based on the table structure that has been created.
5. Upsert data to table.
6. If the process 3 thru 5 is succesful, then file will be moved to folder archive/.
7. If the process 3 thru 5 throws an error, then file will be moved to folder error/.
8. Repeat process 1 thru 6 until there is no more file to read.


## Data structure
Here's a list of the dimension tables, and its structure
<details>
<summary> table "series" </summary>
<p>

```sql
create table series
(
    id                   int,
    backdrop_path        text,
    created_by           int[],
    episode_run_time     int[],
    genres               int[],
    homepage             text,
    in_production        boolean,
    languages            text[],
    first_air_date       date,
    last_air_date        date,
    last_episode_to_air  json,
    name                 text,
    next_episode_to_air  json,
    networks             int[],
    number_of_episodes   int,
    number_of_seasons    int,
    origin_country       text[],
    original_language    text,
    original_name        text,
    overview             text,
    popularity           numeric(6, 3),
    poster_path          text,
    production_companies int[],
    seasons              json,
    status               text,
    type                 text,
    vote_average         numeric(4, 2),
    vote_count           int,
    date_effective       timestamp
);
```
</p>
</details>

<details>
<summary> table "creator" </summary>
<p>

```sql
create table creator
(
    id             int,
    credit_id      text,
    name           text,
    gender         int,
    profile_path   text,
    date_effective timestamp
);
```
</p>
</details>

<details>
<summary> table "genre" </summary>
<p>

```sql
create table genre
(
    id             int,
    name           text,
    date_effective timestamp
);

```
</p>
</details>

<details>
<summary> table "language" </summary>
<p>

```sql
create table language
(
    code           text,
    name           text,
    date_effective timestamp
);

```
</p>
</details>

<details>
<summary> table "network" </summary>
<p>

```sql
create table network
(
    id             int,
    name           text,
    logo_path      text,
    origin_country text,
    date_effective timestamp
);

```
</p>
</details>


<details>
<summary> table "country" </summary>
<p>

```sql
create table country
(
    code           text,
    name           text,
    date_effective timestamp
);

```
</p>
</details>


<details>
<summary> table "production_company" </summary>
<p>

```sql
create table production_company
(
    id             int,
    logo_path      text,
    name           text,
    origin_country text,
    date_effective timestamp
);

```
</p>
</details>


