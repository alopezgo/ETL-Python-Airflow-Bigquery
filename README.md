# ETL process with python, Bigquery API and Airflow dags
This is a repository about data enginering with wide examples for extract transform and load data in an empreuner context, specificly data from streaming audio for a media enterprise.

# Audio data transmission (streaming), some terms to categorize them:
The streaming content is mainly categorized in live and ondemand.
But also you will find other terms to refer live content like new and old logic, been new when the auditors consume live content in the period delimited by a program or specific period of time, and old when auditors where consuming live content before the program or period start. 

# Used tools and programming languages
You will find etl projects written in Python 3
Bigquery and CloudSQL (GCP) API consumption
Script structure to be used in Airflow, a process scheduler, that defines DAGS to run periodically.
More information about Airflow dag at https://airflow.apache.org/docs/apache-airflow/1.10.10/concepts.html#:~:text=Core%20Ideas-,DAGs,reflects%20their%20relationships%20and%20dependencies.

