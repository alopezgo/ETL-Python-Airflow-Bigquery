""" Audiencias y Consumo - Detalle Live-OD Emisor

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos del detalle de consumo de audio digital de Emisor Podcasting, y ser
programadas para la extracción periódica en la plataforma Apache Airflow.

"""

# Importación de librerías
import itertools
import os
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


stream_vip = {
    0: None,
    1: "45afe3fdbe92500d3b72588549237cb3",
    5: "b3583f38d358a82ecb3b6783664f1305",
    20: "9e32ed53938d70972f115574cecc25d6",
    40: "e8bcb3395e6c37328e8cbb6424ebc376"
}

#headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
#endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

# region Funciones Auxiliares


def sesion_platform():
    """Inicio de sesión de Mediastream Platform. A fin de extraer los datos, las
    consultas a la API requieren de un token de usuario.

    Returns:
        (dict, str): headers y endpoint para consulta a API
    """
    # Se mantiene el siguiente código como respaldo
    '''
    login_platform = {"username": "jlizana@rdfmedia.cl",
                      "password": "jlizana123", "withJWT": "true", "login": "Login"}
    session = requests.session()
    r = session.post("https://platform.mediastre.am/login",
                     data=login_platform)
    r = session.get('https://platform.mediastre.am/analytics/now')
    token = r.cookies.get_dict()["jwt"]
    headers = {"X-API-Token": token}
    '''

    headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
    endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

    return headers, endpoint


def fechas_consulta(agg: str):
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


def generar_req(date_range: list, agg: str, media: str, vip: int):
    """Generación automática del payload para consulta a API de Mediastream

    Args:
        agg (str): agregación de tiempo
        fechas (list): rango de fechas a consultar (UTC)
        vip (int): grupo de stream vip

    Returns:
        dict: payload con el formato para consulta
    """

    req = {}
    req["name"] = "full_by_time"
    req["dimension"] = ["content_live", "show_id", "show"]

    if media == "episode":
        req["dimension"] = req["dimension"] + ["content_id",
                                               "season_id", "season", "episode_id", "episode"]

    req["filter"] = [
        {"name": "date", "op": [">=", "<="], "value":date_range},
        {"name": "group", "value": [
            "b3583f38d358a82ecb3b6784664f1308"], "logic_op": "and", "order": 1},
        {"name": "group", "value": [
            "084012cca93d1e0c2ef6e0d162389a44"], "logic_op": "and", "order": 2},
        {"name": "content_type", "value": [
            "alive"], "logic_op": "and not", "order": 3}
    ]

    if vip:
        req["filter"].append(
            {"name": "group", "value": [stream_vip[vip]], "logic_op": "and", "order": 3})

    req["calendar"] = {"type": "all"}
    req["time"] = "0"
    req["trunc"] = [agg]

    return req


def upload_to_bq(df):
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pd.DataFrame): DataFrame con formato
    """
    
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    table_id = "conexion-datos-rdf.raw.liveod_emisor"

    # Configuración del trabajo. Debido a los tipos de datos involucrados, se debe 
    # declarar el esquema manualmente.
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                "content_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "content_live", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "episode_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "episode", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "season_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("season", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "show_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("show", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "date_time", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField(
                "duration", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("origen", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vip", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("start", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField(
                "stream", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField(
                "device", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("minutes", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField(
                "avg_minutes", bigquery.enums.SqlTypeNames.FLOAT),
        ]
    )

    # Make an API request.
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    return job.result()  # Wait for the job to complete.

# endregion

# region Funciones Principales


def del_curr():
    replace_days = int(Variable.get("dias_remplazo"))

    client = bigquery.Client.from_service_account_json(_google_key)

    ts = date.today()
    fecha_inicio = (ts + timedelta(days=-replace_days)).strftime("%Y-%m-%d")
    fecha_inicio_m = (ts + timedelta(days=-replace_days)
                      ).replace(day=1).strftime("%Y-%m-%d")

    update_job = client.query(
        """
        DELETE FROM `conexion-datos-rdf.raw.liveod_emisor` 
        WHERE date_time >= "{0}"
        """.format(fecha_inicio))

    print(update_job.result())

    update_job = client.query(
        """
        DELETE FROM `conexion-datos-rdf.raw.liveod_emisor` 
        WHERE date_time >= "{0}"
        AND CONTAINS_SUBSTR(origen, "month")
        """.format(fecha_inicio_m))

    print(update_job.result())


def descargar_data():
    headers, endpoint = sesion_platform()

    medias = ["show", "episode"]
    aggs = ["MONTH", "DAY", "HOUR"]
    vips = [0, 1, 5, 20, 40]

    df_total = pd.DataFrame(columns=['content_id', 'content_type', 'content_live', 'episode_id', 'episode',
                                     'season_id', 'season', 'show_id', 'show', 'date_time', 'duration', 'origen', 'vip',
                                     'start', 'stream', 'device', 'minutes', 'avg_minutes'])

    for media, vip, agg in list(itertools.product(medias, vips, aggs)):
        fechas = fechas_consulta(agg)
        print((media, vip, agg, fechas))

        req = generar_req(fechas, agg, media, vip)
        response = requests.post(endpoint, headers=headers, json=req)

        df = pd.DataFrame(response.json()["data"])
        df["date_time"] = pd.to_datetime(df["date_time"]).dt.tz_localize(
            utc).dt.tz_convert(cl).dt.tz_localize(None)
        df["sessions"] = round(df["minutes"] / df["avg_minutes"])
        cols = ["show_id", "show", "date_time", "content_type"]

        if media == "episode":
            cols = cols + ["content_id", "season_id",
                           "season", "episode_id", "episode"]
        df_2 = df.groupby(by=cols).sum().reset_index().copy()

        df_2["avg_minutes"] = df_2["minutes"] / df_2["sessions"]
        df_2.drop(["sessions"], axis=1, inplace=True)

        df_2["origen"] = "emisor_{0}_{1}".format(media, agg.lower())
        df_2["vip"] = vip

        df_total = pd.concat([df_total, df_2])

    df_total = df_total.astype(dtype={
        "content_id": "string", "content_type": "string", "content_live": "string", "episode_id": "string",
        "episode": "string", "season_id": "string", "season": "string", "show_id": "string", "show": "string",
        "date_time": "datetime64[ns]", "duration": "Float64", "origen": "string", "vip": "Int64",
        "start": "Int64", "stream": "Int64", "device": "Int64", "minutes": "Float64", "avg_minutes": "Float64"})

    upload_to_bq(df_total)


def actualizar_tabla_final():
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    table_id = "conexion-datos-rdf.live_od.data_editorial"

    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Ejecución de consutla para unir datos descargados con datos del funnel
    sql = """
        SELECT
            "emisorpodcasting.cl" AS soporte,
            "Ondemand" AS content_type,
            SUBSTRING(liveod_ope.origen,
                8,
                15) AS origen,
            liveod_ope.date_time AS date_time,
            dicc_shows.first_emision as first_emision,
            dicc_medios.date_created AS date_created,
            dicc_shows.title AS show,
            liveod_ope.season AS season,
            liveod_ope.episode AS episode,
            dicc_medios.duration AS duration,
            dicc_shows.type as type,
            dicc_shows.genres AS genres,
            dicc_shows.producers AS producers,
            dicc_shows.distributors as distributors,
            liveod_ope.vip AS vip,
            liveod_ope.start AS start,
            liveod_ope.stream AS stream,
            liveod_ope.device AS device,
            liveod_ope.minutes AS minutes,
            liveod_ope.avg_minutes AS avg_minutes
        FROM
            `conexion-datos-rdf.raw.liveod_emisor` AS liveod_ope
        LEFT JOIN
            `conexion-datos-rdf.dicc_medios.dicc_shows` AS dicc_shows
        ON
            liveod_ope.show_id = dicc_shows._id
        LEFT JOIN
            `conexion-datos-rdf.dicc_medios.dicc_medios` AS dicc_medios
        ON
            liveod_ope.content_id = dicc_medios.media_id
        WHERE
            liveod_ope.show_id IS NOT NULL

        UNION ALL

        SELECT
            "emisorpodcasting.cl" AS soporte,
            "Ondemand" AS content_type,
            CONCAT("soporte_", REPLACE(REPLACE(REPLACE(funnel_ope.periodo, "mensual", "month"), "diario", "day"), "hora", "hour")) AS origen,
            TIMESTAMP(CONCAT(funnel_ope.fecha, " ", funnel_ope.hora_inicio)) AS date_time,
            null as first_emision,
            NULL AS date_created,
            NULL AS show,
            NULL AS season,
            NULL AS episode,
            NULL AS duration,
            null as type,
            NULL AS genres,
            NULL AS producers,
            null as distributors,
            CAST(SUBSTRING(funnel_ope.vip,2,2)AS int) AS vip,
            funnel_ope.start AS start,
            funnel_ope.stream AS stream,
            funnel_ope.device AS device,
            funnel_ope.minutes AS minutes,
            funnel_ope.avg_minutes AS avg_minutes
        FROM
            `conexion-datos-rdf.funnel_vip.funnel_vip_emisorOPE` AS funnel_ope
    """

    # Start the query, passing in the extra configuration.
    # Make an API request.
    query_job = client.query(sql, job_config=job_config)
    print(query_job.result())  # Wait for the job to complete.


def flag_off():
    Variable.update(key="correo_od_emisor", value=False)


def flag_on():
    Variable.update(key="correo_od_emisor", value=True)

# endregion


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
    "liveod_emisor",
    default_args=args,
    description="Actualización semanal de datos live/ondemand con fines editoriales",
    schedule_interval='30 5 * * *',
    catchup=False,
    start_date=datetime(2022, 1, 3, tzinfo=local_tz),
    tags=["audio", "platform", "semanal", "on demand"],
) as dag:

    # Se definen los pasos del dag según funciones anteriores
    t1 = PythonOperator(
        task_id="borrar_data",
        python_callable=del_curr
    )

    t2 = PythonOperator(
        task_id="descargar_data",
        python_callable=descargar_data
    )

    t3 = PythonOperator(
        task_id="actualizar_tabla_final",
        python_callable=actualizar_tabla_final
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
    marca_reset >> t1 >> t2 >> t3 >> marca_ok
