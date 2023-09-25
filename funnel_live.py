import itertools
import os
from datetime import date, datetime, timedelta

import pandas as pd
import pytz
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

cl = pytz.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'
v_agg = ["MONTH", "DAY", "HOUR"]
v_vip = [0, 1, 5, 20, 40]
var_emisor = [False, True]
content_live = {
    "601415b58308405b0d11c82a": "horizonte.cl",
    "5c915497c6fd7c085b29169d": "13cradio.cl",
    "5c8d6406f98fbf269f57c82c": "playfm.cl",
    "5c915724519bce27671c4d15": "sonarfm.cl",
    "5c915613519bce27671c4caa": "tele13radio.cl"
}
stream_vip = {
    1: "45afe3fdbe92500d3b72588549237cb3",
    5: "b3583f38d358a82ecb3b6783664f1305",
    20: "9e32ed53938d70972f115574cecc25d6",
    40: "e8bcb3395e6c37328e8cbb6424ebc376"
}

headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
table_id = "conexion-datos-rdf.funnel_vip.funnel_vip_update"

#region Funciones Auxiliares
def convertir_fechas(fechas:list):
    fecha_inicio_cl = cl.localize(datetime.strptime(fechas[0], "%Y-%m-%d"))
    fecha_inicio_utc = fecha_inicio_cl.astimezone(utc).strftime(fmt)

    fecha_fin_cl = cl.localize(datetime.strptime(fechas[1], "%Y-%m-%d")) + timedelta(seconds=-1, days=1)
    fecha_fin_utc = fecha_fin_cl.astimezone(utc).strftime(fmt)

    return [fecha_inicio_utc, fecha_fin_utc]

def generar_req(agg:str, fechas:list, vip:int) -> dict:
    date_range = convertir_fechas(fechas)

    req = {}
    req["name"] = "full_by_time"
    req["dimension"] = ["content_live", "content_id"]
    req["filter"] = [
        {"name": "date", "op": [">=","<="], "value":date_range},
        {"name": "content_type", "value": ["aod"], "logic_op": "and not", "order": 1}
    ]

    if vip > 0:
        req["filter"].append({"name": "group", "value": [stream_vip[vip]], "logic_op": "and", "order": 2})

    req["calendar"] = {"type": "all"}
    req["time"] = 0
    req["trunc"] = [agg]
    
    return req

def formatear_df(df_0:pd.DataFrame, vip:int, agg:str) -> pd.DataFrame:
    df = df_0[df_0["content_id"].isin(list(content_live.keys()))].copy()

    df["date_time"] = pd.to_datetime(df["date_time"]).dt.tz_localize(utc).dt.tz_convert(cl).dt.tz_localize(None)
    df["fecha"] = df["date_time"].dt.date
    
    if agg == "HOUR":
        df["hora_inicio"] = df["date_time"].dt.time.astype(str)
        df["hora_termino"] = (df["date_time"] + timedelta(hours=1, seconds=-1)).dt.time.astype(str)
    else:
        df["hora_inicio"] = "00:00:00"
        df["hora_termino"] = "23:59:59"
    
    dicc_replace = {
        "Horizonte": "horizonte.cl",
        "Oasis FM": "oasisfm.cl",
        "13c Radio": "13cradio.cl",
        "Play FM": "playfm.cl",
        "Sonar": "sonarfm.cl",
        "Tele 13 Radio": "tele13radio.cl"
    }

    df["soporte"] = df["content_live"].replace(dicc_replace)
    df["vip"] = "v" + str(vip)

    periodo = ""
    if agg == "MONTH": periodo = "mensual"
    elif agg == "DAY": periodo = "diario"
    elif agg == "HOUR": periodo = "hora"
    df["periodo"] = periodo

    df["live_id"] = df["content_live"].str.lower().str[:3].replace({"tel": "t13"})
    df["id"] = df["live_id"] + "_" + df["date_time"].dt.strftime("%y%m%d%H") + "_" + periodo[0] + "_v{0}".format(str(vip))

    return df[["id", "fecha", "hora_inicio", "hora_termino", "periodo", "soporte", "vip",
                        "start", "stream", "device", "minutes", "avg_minutes"]].convert_dtypes()

def subir_bq(df:pd.DataFrame):
    client = bigquery.Client.from_service_account_json(_google_key)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("fecha", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("hora_inicio", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("hora_termino", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("periodo", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("soporte", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vip", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_TRUNCATE"
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.

#endregion

#region Funciones principales

def descargar_data():
    replace_days = int(Variable.get("dias_remplazo"))

    ts = date.today()
    end_time = (ts + timedelta(days=-1)).strftime("%Y-%m-%d")
    start_time = (ts + timedelta(days=-replace_days)).strftime("%Y-%m-%d")
    fechas = [start_time, end_time]

    df_total = pd.DataFrame(columns=["id", "fecha", "hora_inicio", "hora_termino", "periodo", "soporte", "vip",
                    "start", "stream", "device", "minutes", "avg_minutes"])

    for agg, vip in list(itertools.product(v_agg, v_vip)):
        fechas_con = fechas.copy()
        if agg == "MONTH":
            fechas_con[0] = datetime.strptime(fechas[0], "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")

        req = generar_req(agg, fechas_con, vip)
        response = requests.post(endpoint, headers = headers, json = req)
        df = formatear_df(pd.DataFrame(response.json()["data"]), vip, agg)

        df_total = pd.concat([df_total, df])

    subir_bq(df_total)

def actualizar_tabla():
    client = bigquery.Client.from_service_account_json(_google_key)
    update_job = client.query(
        """
        MERGE `conexion-datos-rdf.funnel_vip.funnel_vip` t_final
        USING `conexion-datos-rdf.funnel_vip.funnel_vip_update` t_update
        ON t_final.id = t_update.id
        WHEN MATCHED THEN
        UPDATE SET
            hora_inicio = t_update.hora_inicio,
            hora_termino = t_update.hora_termino,
            start = t_update.start,
            stream = t_update.stream,
            device = t_update.device,
            minutes = t_update.minutes,
            avg_minutes = t_update.avg_minutes
        WHEN NOT MATCHED THEN
        INSERT (id, fecha, hora_inicio, hora_termino, periodo, soporte, vip, start, stream, device, minutes, avg_minutes)
        VALUES (id, fecha, hora_inicio, hora_termino, periodo, soporte, vip, start, stream, device, minutes, avg_minutes)
        """)
        
    update_job.result()

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['audiencias@rdfmedia.cl'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function
}

with DAG(
    "funnel_vip_live",
    default_args= args,
    description= "Actualización semanal de funnel live",
    schedule_interval= "30 8 * * *",
    catchup=False,
    start_date= datetime(2022, 1, 3),
    tags=["audio", "platform", "semanal", "funnel"],
) as dag:

    t1 = PythonOperator(
        task_id="descargar_data",
        python_callable=descargar_data
    )

    t2 = PythonOperator(
        task_id="actualizar_tabla",
        python_callable=actualizar_tabla
    )

    t1 >> t2 