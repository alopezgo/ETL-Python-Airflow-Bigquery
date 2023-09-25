""" Audiencias y Consumo - Detalle Live-OD Comercial

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos del detalle de consumo de audio digital de Emisor Podcasting, y ser
programadas para la extracción periódica en la plataforma Apache Airflow.

"""

# Importación de librerías
import itertools
from datetime import date, datetime, timedelta

import pandas as pd
import pendulum
import pytz
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# Configuración de BigQuery. La ruta corresponde a la dirección del archivo
# de credenciales dentro del servidor de Airflow. Además se configura dirección
# de base de IPs
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
cl = pytz.timezone("America/Santiago")
local_tz = pendulum.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'

headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

#region Funciones Auxiliares

def fechas_consulta(agg:str):
    """Generación automática del rango de fechas de consulta

    Args:
        agg (str): agregación de periodos

    Returns:
        list[str]: rango de fechas en formato de consulta (UTC)
    """

    replace_days = int(Variable.get("dias_remplazo"))

    ts = date.today()

    # Cálculo fecha de inicio de consulta
    fecha_inicio = ts + timedelta(days=-replace_days)
    fecha_inicio = datetime(
        fecha_inicio.year, fecha_inicio.month, fecha_inicio.day)
    # Si se utiliza agregación mensual se remplaza el día por el inicio del mes
    if (agg == "MONTH"): fecha_inicio = fecha_inicio.replace(day=1)
    fecha_inicio_cl = cl.localize(fecha_inicio)
    fecha_inicio_utc = fecha_inicio_cl.astimezone(utc).strftime(fmt)

    # Cálculo de fecha de fin de consulta
    fecha_fin = ts
    fecha_fin = datetime(fecha_fin.year, fecha_fin.month,
                         fecha_fin.day) + timedelta(seconds=-1)
    fecha_fin_cl = cl.localize(fecha_fin)
    fecha_fin_utc = fecha_fin_cl.astimezone(utc).strftime(fmt)

    return [fecha_inicio_utc, fecha_fin_utc]

def generar_req(date_range:list, agg:str, media:str, vip:int):
    """Generación automática del payload para consulta a API de Mediastream

    Args:
        agg (str): agregación de tiempo
        fechas (list): rango de fechas a consultar (UTC)
        vip (int): grupo de stream vip

    Returns:
        dict: payload con el formato para consulta
    """
    
    stream_vip = {
        0: None,
        1: "45afe3fdbe92500d3b72588549237cb3",
        5: "b3583f38d358a82ecb3b6783664f1305",
        20: "9e32ed53938d70972f115574cecc25d6",
        40: "e8bcb3395e6c37328e8cbb6424ebc376"
    }

    req = {}
    req["name"] = "full_by_time"
    req["dimension"] = ["content_live", "show_id", "show"]

    if media == "episode":
        req["dimension"] = req["dimension"] + ["content_id", "season_id", "season", "episode_id", "episode"]

    req["filter"] = [
        {"name": "date", "op": [">=","<="], "value":date_range},
        {"name": "content_type", "value": ["alive"], "logic_op": "and not", "order": 9}
    ]

    if vip:
        req["filter"].append({"name": "group", "value": [stream_vip[vip]], "logic_op": "and", "order": 3})

    req["calendar"] = {"type": "all"}
    req["time"] = "0"
    req["trunc"] = [agg]
    
    return req

def descargar_data(vip: int):
    medias = ["show", "episode"]
    aggs = ["MONTH", "DAY", "HOUR"]

    df_total = pd.DataFrame(columns=['content_id', 'content_type', 'content_live', 'episode_id', 'episode', 
            'season_id', 'season', 'show_id', 'show', 'date_time', 'duration', 'origen', 'vip',
            'start', 'stream', 'device', 'minutes', 'avg_minutes'])

    for media, agg in list(itertools.product(medias, aggs)):
        fechas = fechas_consulta(agg)
        print((media, vip, agg, fechas))

        req = generar_req(fechas, agg, media, vip)
        response = requests.post(endpoint, headers = headers, json = req)
                    
        df = pd.DataFrame(response.json()["data"])
        df["date_time"] = pd.to_datetime(df["date_time"]).dt.tz_localize(utc).dt.tz_convert(cl).dt.tz_localize(None)
        df["sessions"] = round(df["minutes"] / df["avg_minutes"])
        cols = ["show_id", "show", "date_time", "content_type"]

        if media == "episode":
            cols = cols + ["content_id", "season_id", "season", "episode_id", "episode"]
        df_2 = df.groupby(by=cols).sum().reset_index().copy()

        df_2["avg_minutes"] = df_2["minutes"] / df_2["sessions"]
        df_2.drop(["sessions"], axis=1, inplace=True)

        df_2["origen"] = "full_{0}_{1}".format(media, agg.lower())
        df_2["vip"] = "v{0}".format(vip)

        df_total = pd.concat([df_total, df_2])

    df_total = df_total.astype(dtype= {
        "content_id": "string", "content_type": "string", "content_live": "string", "episode_id": "string", 
        "episode": "string", "season_id": "string", "season": "string", "show_id": "string", "show": "string",
        "date_time": "datetime64[ns]", "duration": "Float64", "origen": "string", "vip": "string", 
        "start": "Int64", "stream": "Int64", "device": "Int64", "minutes": "Float64", "avg_minutes": "Float64"})

    return df_total

def actualizar_tabla(df):
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pd.DataFrame): DataFrame con formato
    """
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)
    table_id = "conexion-datos-rdf.raw.liveod_comercial_v2"

    # Configuración del trabajo. Debido a los tipos de datos involucrados, se debe 
    # declarar el esquema manualmente.
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("content_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("content_live", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("episode_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("episode", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("season_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("season", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("show_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("show", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("date_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("duration", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("origen", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vip", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("start", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("stream", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("device", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("minutes", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("avg_minutes", bigquery.enums.SqlTypeNames.FLOAT),
        ]
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
    return job.result()  # Wait for the job to complete.

#endregion

#region Funciones Principales

def del_curr():
    replace_days = int(Variable.get("dias_remplazo"))

    client = bigquery.Client.from_service_account_json(_google_key)

    ts = date.today()
    fecha_inicio = (ts + timedelta(days=-replace_days)).strftime("%Y-%m-%d")
    fecha_inicio_m = (ts + timedelta(days=-replace_days)).replace(day=1).strftime("%Y-%m-%d")
    
    update_job = client.query(
        """
        DELETE FROM `conexion-datos-rdf.raw.liveod_comercial_v2` 
        WHERE date_time >= "{0}"
        """.format(fecha_inicio))

    print(update_job.result())

    update_job = client.query(
        """
        DELETE FROM `conexion-datos-rdf.raw.liveod_comercial_v2` 
        WHERE date_time >= "{0}"
        AND CONTAINS_SUBSTR(origen, "month")
        """.format(fecha_inicio_m))

    print(update_job.result())

def descargar_data_v0():
    df = descargar_data(0)
    
    actualizar_tabla(df)

def descargar_data_v1():
    df = descargar_data(1)
    
    actualizar_tabla(df)

def descargar_data_v5():
    df = descargar_data(5)
    
    actualizar_tabla(df)

def descargar_data_v20():
    df = descargar_data(20)
    
    actualizar_tabla(df)

def descargar_data_v40():
    df = descargar_data(40)
    
    actualizar_tabla(df)


def flag_off():
    Variable.update(key="correo_od_comercial", value=False)


def flag_on():
    Variable.update(key="correo_od_comercial", value=True)

#endregion

# Argumentos por defecto de Airflow. Se configura un tiempo de espera de 5 
# minutos entre intentos de ejecución
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

# Configuración del DAG de Airflow. Se configuran tiempos de ejecución e inicio del mismo
with DAG(
    "liveod_comercial",
    default_args= args,
    description= "Actualización semanal de datos live/ondemand con fines comerciales",
    schedule_interval= "30 4 * * *",
    catchup=False,
    start_date= datetime(2022, 1, 3, tzinfo=local_tz),
    tags=["audio", "platform", "semanal", "on demand"],
) as dag:

    # Se definen los pasos del dag según funciones anteriores
    limpiar = PythonOperator(
        task_id="limpiar_tabla",
        python_callable=del_curr
    )
    
    v0 = PythonOperator(
        task_id="descargar_v0",
        python_callable=descargar_data_v0
    )
    
    v1 = PythonOperator(
        task_id="descargar_v1",
        python_callable=descargar_data_v1
    )
    
    v5 = PythonOperator(
        task_id="descargar_v5",
        python_callable=descargar_data_v5
    )
    
    v20 = PythonOperator(
        task_id="descargar_v20",
        python_callable=descargar_data_v20
    )
    
    v40 = PythonOperator(
        task_id="descargar_v40",
        python_callable=descargar_data_v40
    )

    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off
    )

    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on
    )

    # Configuración de la ejecución del DAG.
    marca_reset >> limpiar >> [v0, v1, v5, v20, v40] >> marca_ok
