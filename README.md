# ETL process with python, Bigquery API and Airflow
This is a repository about data enginering with wide examples for extract transform and load data in an empreuner context, specificly data from streaming audio for a media enterprise.

# Audio data transmission (streaming), some terms to categorize them:
- The streaming content is mainly categorized in live and ondemand.
- But also you will find other terms to refer live content like new and old logic: new when the auditors consume live content in the period delimited by a program or specific period of time, and old when auditors where consuming live content before the program or period start.
- live content may refer to specific period of time, oficial if period is equal to radio program and ipsos if period is hour closed. For example: Amor moderno oficial goes from 10:15 am to 11:30, but Amor moderno Ipsos goes from 10:00 to 12:00.
- ondemand content could correspond to a program radioshow or a podcast, they are identified by id (show_id and media_id, show_id for readioshow/podcast and media_id for episode/media)
- live content currency auditors is categorized by vips, vip 0 is all auditors which were listening live content, vip1 is all auditrores which at least where listening for a minute. There's vip0, vip1, vip5, vip20 and vip40 metrics.
- The data is analyzed across population variables like genre, age, and location

# Used tools and programming languages
- You will find etl projects written in Python 3
- Bigquery and CloudSQL (GCP) API consumption
- Script structure to be used in Airflow, a process scheduler, that defines DAGS to run periodically.
- More information about Airflow dag at https://airflow.apache.org/docs/apache-airflow/1.10.10/concepts.html#:~:text=Core%20Ideas-,DAGs,reflects%20their%20relationships%20and%20dependencies.

