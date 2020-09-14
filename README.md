# Data Engineering NanoDegree

## Author
Deivid Robim [Linkedin](https://www.linkedin.com/in/deivid-robim-200b3330/)

### Capstone Project: How COVID-19 impected U.S Automobile Accidents?

A automobile telematics startup, Global Telematics, has grown their traffic data and want to move their processes and data onto the cloud.
Their data resides in S3, in a directory of CSV files representing daily automobile accidents in U.S, as well as a directory with daily U.S COVID-19 data at county level.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their Analytics team to continue finding insights.
This time, the Analytics Team wants to ingest COVID-19 data to understand how the lockdown in U.S impected the automobile accidents.

### Project Structure
```
Data-Pipelines-with-Airflow
│   README.md                    # Project description
│   requirements.txt             # Python dependencies
│   docker-compose.yml           # Docker Containers Configuration
└───airflow                      # Airflow home
|   |
│   └───dags                          # Airflow DAGs location
│   |   │ create_raw_datalake_dag.py  # DAG definition
│   |   │ us_accidents_etl_dag.py  # DAG definition
|   |   |
|   └───plugins
│       │
|       └───helpers
|       |   | sql_creates.py  # Sql queries to create objects
|       |   | sql_load.py     # Sql queries to load data into sql tables
|       |
|       └───operators
|       |   | check_s3_file_count.py # CheckS3FileCount
|       |   | create_s3_bucket.py    # CreateS3BucketOperator
|       |   | upload_files_to_s3.py  # UploadFilesToS3Operator
|       |   | data_quality.py    # DataQualityOperator
|       |   | load_table.py      # LoadTableOperator
|       |   | s3_to_redshift.py  # S3ToRedshiftOperator
|___images
|   | dag_graph_view.png # DAG Graph View
|   | trigger_dag.png # DAG Tree View
|___src
|   | create_resources.py # Script to create resources required
|   | delete_resources.py # Script to delete resources created
|   | split_data.py       # Script to split large datasets
|   | dwh.cfg             # Configuration file
```

### Requirements for running locally
* Install [Python3](https://www.python.org/downloads/)
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* [AWS](https://aws.amazon.com/) Account

### Project Goal
The idea is to create a data lake and a DataWarehouse on AWS, enabling users to analyze accidents data and extract insights.
The main goal of this project is to build an end-to-end data pipeline which is capable to work with big volumes of data.

### Technologies
We are going to store the raw data on Amazon S3, which is is an object storage service that offers industry-leading scalability, availability and durability.

Considering the current data size, we are going to use Amazon Redshift to ingest the data from S3 and perform the ETL process, denormalizing the datasets to create FACTS and DIMENSION tables.

Finally, to orchestrate everything, we are going to build a data pipeline using Apache Airflow.
Airflow provides an intuitive UI where we can track the progress and bottlenecks of our pipelines.

### Datasets

We are going to work with 3 datasets:


- [U.S Accidents](https://www.kaggle.com/sobhanmoosavi/us-accidents)
    - This is a countrywide traffic accident dataset, which covers 49 states of the United States.
- [U.S Cities: Demographics](https://public.opendatasoft.com/explore/dataset/us-cities-demographics)
    - This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.
- [U.S COVID-19](https://www.kaggle.com/imdevskp/corona-virus-report?select=usa_county_wise.csv)
    - This dataset contains information about COVID-19 cases in US at county level.

### Explore Data Quality
Please refer to the comprehensive Data Profiling for each dataset.

- [US Accidents](data-profiling/us-accidents.html)
- [US Cities: Demographics](data-profiling/us-cities-demographics)
- [US COVID-19](data-profiling/covid_19_usa)

### Fact Table
```
• songplays - records in log data associated with song plays i.e. records with page NextSong
  table schema: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```
### Dimension Tables
```
• users - users in the app
  table schema: user_id, first_name, last_name, gender, level

• songs - songs in music database
  table schema: song_id, title, artist_id, year, duration

• artists - artists in music database
  table schema: artist_id, name, location, latitude, longitude

• time - timestamps of records in songplays broken down into specific units
  table schema: start_time, hour, day, week, month, year, weekday
```
### Instructions for running locally

#### Clone repository to local machine
```
git clone https://github.com/drobim-data-engineering/Capstone-Project-Data-Engineering-Nanodegree.git
```

#### Change directory to local repository
```
cd Capstone-Project-Data-Engineering-Nanodegree
```

#### Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements (this can take couple of minutes)
```

#### Split the data
Download and place the data in the desired directory.
The script below goes through the directory input by the user and splits the data.
```
cd src/
python split_data.py # Split the data into chunks
```

#### Start Airflow container

Everything is configured in the docker-compose.yml file.
```
docker-compose up
```
#### Edit dwh.cfg file

This file holds the configuration variables used on the scripts to create and configure the AWS resources.
These are the variables the user needs to set up before running the `create_resources.py` script.

```
AWS_ACCESS_KEY = <ENTER AWS ACCESS KEY>   # paste your user Access Key
AWS_SECRET_ACCESS_KEY = <ENTER AWS SECRET KEY>  # paste your user Secret Key
REGION = <ENTER THE AWS REGION> # paste the AWS Region to create resources
VPC_ID = <ENTER VPC ID>  # paste the VPC_ID you want to create the resources (If blank the first VPC on user's AWS account is considered)
```
<b>REMEMBER:</b> Never share your <b>AWS ACCESS KEY & SECRET KEY</b> on scripts.

This is just an experiment to get familiarized with AWS SDK for Python.

#### Run script
```
cd src/
python create_resources.py # Entry point to kick-off a series of processes from creating resources on AWS to creating custom connections on Airflow.
```
The execution of this script incur <b>REAL MONEY</b> costs so be aware of that.

#### Start the DAG
Visit the Airflow UI and start the DAG by switching it state from OFF to ON.

Refresh the page and click on the "trigger dag" button.

![trigger_dag](images/trigger_dag.png)

Finally, click on "create_raw_datalake_dag" and then on "Graph View" to view the current DAG state.
This pipeline creates the S3 bucket for our raw data lake and uploads the files from local machine. Wait until the pipeline has successfully completed (it should take around 15-20 minutes).

![dag_state](images/dag_graph_view.png)

#### Delete Resources
Please make sure to run the script below once the process is completed.

```
cd src/
python -m delete_resources.py # Entry point to kick-off a series of processes to delete resources resources on AWS and connections on Airflow.
```

## Addressing Other Scenarios
1. The data was increased by 100x

- <b>Data Lake</b>
    - The optimized data lake would not require significant changes since we have a flexible schema and S3 is meant for storing big data.
- <b>ETL Process</b>
    - Regarding the ETL job, it would require moving to a EMR Cluster on AWS running Apache Spark, which is optimize for Big Data Processing.
- <b>Orchestration</b>
    - The Airflow container is currently running on a single container on our local machine. In a production environment, Airflow would be running on a cluster of machines likely coordinated by Kubernetes.

2. The pipelines would be run on a daily basis by 7 am every day.
    - We can schedule our Airflow pipelines so that they follow this pattern.
    - Airflow will store useful statistics regarding job status and we can easily spot faults in the pipeline.

3. The database needed to be accessed by 100+ people.
    - Amazon Redshift should be able to handle 100+ people querying the Datawarehouse. The user can also increase the number of instances at any time to satisfy high demand.
