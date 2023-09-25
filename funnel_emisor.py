""" Audiencias y Consumo - Funnel Live-OD Emisor

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos del detalle de consumo de audio digital por playback, y ser
programadas para la extracción periódica en la plataforma Apache Airflow.

"""

# Importación de librerías
import itertools
from datetime import date, datetime, timedelta

import pandas as pd
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
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'

# Diccionarios útiles
# Diccionarios de agregaciones y vips
v_agg = ["MONTH", "DAY", "HOUR"]
var_emisor = [False, True]
v_vip = [0, 1, 5, 20, 40]
stream_vip = {
    1: "45afe3fdbe92500d3b72588549237cb3",
    5: "b3583f38d358a82ecb3b6783664f1305",
    20: "9e32ed53938d70972f115574cecc25d6",
    40: "e8bcb3395e6c37328e8cbb6424ebc376"
}

# Tipos de contenido a extraer por marca
content_live = {
    "601415b58308405b0d11c82a": "horizonte.cl",
    "5c915497c6fd7c085b29169d": "oasisfm.cl",
    "5c915497c6fd7c085b29169d": "13cradio.cl",
    "5c8d6406f98fbf269f57c82c": "playfm.cl",
    "5c915724519bce27671c4d15": "sonarfm.cl",
    "5c915613519bce27671c4caa": "tele13radio.cl"
}

# region Funciones Auxiliares


def sesion_platform():
    """Inicio de sesión de Mediastream Platform. A fin de extraer los datos, las
    consultas a la API requieren de un token de usuario.

    Returns:
        (dict, str): headers y endpoint para consulta a API
    """
    login_platform = {"username": "jlizana@rdfmedia.cl",
                      "password": "jlizana123", "withJWT": "true", "login": "Login"}
    session = requests.session()
    r = session.post("https://platform.mediastre.am/login",
                     data=login_platform)
    r = session.get('https://platform.mediastre.am/analytics/now')
    token = r.cookies.get_dict()["jwt"]
    headers = {"X-API-Token": token}
    endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

    return headers, endpoint


def convertir_fechas(fechas: list) -> list:
    """Conversión de rango de fechas a formato UTC

    Args:
        fechas (list): rango de fechas original (CL)

    Returns:
        list[str]: rango de fechas (UTC)
    """
    # Conversión fecha de inicio
    fecha_inicio_cl = cl.localize(datetime.strptime(fechas[0], "%Y-%m-%d"))
    fecha_inicio_utc = fecha_inicio_cl.astimezone(utc).strftime(fmt)

    # Conversión fecha de término. Se considera la hora al término del día
    fecha_fin_cl = cl.localize(datetime.strptime(
        fechas[1], "%Y-%m-%d")) + timedelta(seconds=-1, days=1)
    fecha_fin_utc = fecha_fin_cl.astimezone(utc).strftime(fmt)

    return [fecha_inicio_utc, fecha_fin_utc]


def generar_req(agg: str, fechas: list, vip: int) -> dict:
    """Generación automática del payload para consulta a API de Mediastream

    Args:
        agg (str): agregación de tiempo
        fechas (list): rango de fechas a consultar (UTC)
        vip (int): grupo de stream vip

    Returns:
        dict: payload con el formato para consulta
    """
    # El payload corresponde a un diccionario json con elementos obligatorios
    req = {}
    # Nombre de la consulta
    req["name"] = "full_by_time"
    # Dimensiones, en este caso sólo se consulta el contenido live/OD
    req["dimension"] = ["content_live"]

    # Filtros. Se debe incluír siempre el filtro de fechas, que se convierten previamente.
    # Se añade filtro del grupo de marca. 
    date_range = convertir_fechas(fechas)
    req["filter"] = [
        {"name": "date", "op": [">=", "<="], "value":date_range},
        {"name": "group", "value": [
            "b3583f38d358a82ecb3b6784664f1308"], "logic_op": "and", "order": 1},
        {"name": "group", "value": [
            "084012cca93d1e0c2ef6e0d162389a44"], "logic_op": "and", "order": 2},
        {"name": "content_type", "value": [
            "alive"], "logic_op": "and not", "order": 3}
    ]

    # Se añade filtro de acuerdo al stream vip a consultar
    if vip > 0:
        req["filter"].append(
            {"name": "group", "value": [stream_vip[vip]], "logic_op": "and", "order": 2})

    # Se señalan opciones por defecto y agrupación a consultar
    req["calendar"] = {"type": "all"}
    req["time"] = 0
    req["trunc"] = [agg]

    return req


def formatear_df(df_0: pd.DataFrame, vip: int, agg: str) -> pd.DataFrame:
    """Función de formateo de respuesta desde API Mediastream a tabla en BigQuery

    Args:
        df_0 (pd.DataFrame): Dataframe sin formato de la respuesta desde API Mediastream
        vip (int): grupo de stream vip consultado
        agg (str): agregación temporal consultada

    Returns:
        pd.DataFrame: datos con formato para subir a BigQuery
    """
    # Se trabaja sobre una copia del DataFrame original y se hace formateo de tipos de datos
    df = df_0.copy()
    df["date_time"] = pd.to_datetime(df["date_time"]).dt.tz_localize(
        utc).dt.tz_convert(cl).dt.tz_localize(None)

    # Cálculo de sesiones totales para agregación de variables y corrección
    df["sessions"] = round(df["minutes"] / df["avg_minutes"])
    df = df.groupby(by=["date_time"]).sum().reset_index().copy()
    df["avg_minutes"] = df["minutes"] / df["sessions"]

    # Extracción de fecha y horas de bloque según tipo de agregación
    df["fecha"] = df["date_time"].dt.date
    if agg == "HOUR":
        df["hora_inicio"] = df["date_time"].dt.time.astype(str)
        df["hora_termino"] = (
            df["date_time"] + timedelta(hours=1, seconds=-1)).dt.time.astype(str)
    else:
        df["hora_inicio"] = "00:00:00"
        df["hora_termino"] = "23:59:59"

    # Se añaden columnas de soporte, vip y tipo de bloque
    df["soporte"] = "emisorpodcasting.cl"
    df["vip"] = "v" + str(vip)
    periodo = ""
    if agg == "MONTH":
        periodo = "mensual"
    elif agg == "DAY":
        periodo = "diario"
    elif agg == "HOUR":
        periodo = "hora"
    df["periodo"] = periodo

    # Se genera un id de fila para comprobar columnas únicas. Este incluirá soporte, fecha, 
    # agregación, vip y tipo de contenido y tendrá el siguiente formato de ejemplo: 
    # emi_21071600_h_v40_live
    df["id"] = "emi_" + df["date_time"].dt.strftime(
        "%y%m%d%H") + "_" + periodo[0] + "_v{0}".format(str(vip))

    return df[["id", "fecha", "hora_inicio", "hora_termino", "periodo", "soporte", "vip",
               "start", "stream", "device", "minutes", "avg_minutes"]].convert_dtypes()


def subir_bq(df: pd.DataFrame):
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pd.DataFrame): DataFrame con formato
    """
    
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)
    table_id = "conexion-datos-rdf.funnel_vip.funnel_vip_emisorOPE_update"

    # Configuración del trabajo. Debido a los tipos de datos involucrados, se debe 
    # declarar el esquema manualmente.
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("fecha", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField(
                "hora_inicio", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "hora_termino", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "periodo", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "soporte", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vip", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_TRUNCATE"
    )

    # Se carga DataFrame a BigQuery y se espera el resultado
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

# endregion

# region Funciones principales


def descargar_data():
    """Descarga de datos desde BigQuery a partir de una consulta
    """

    # Inicio de sesión de BigQuery con archivo de credenciales
    replace_days = int(Variable.get("dias_remplazo"))

    # Se extrae la cantidad de días a remplazar de las variables de Airflow
    headers, endpoint = sesion_platform()
    ts = date.today()
    end_time = (ts + timedelta(days=-1)).strftime("%Y-%m-%d")
    start_time = (ts + timedelta(days=-replace_days)).strftime("%Y-%m-%d")
    fechas = [start_time, end_time]

    # Se crea el DataFrame final con las columnas finales
    df_total = pd.DataFrame(columns=["id", "fecha", "hora_inicio", "hora_termino", "periodo", "soporte", "vip",
                                     "start", "stream", "device", "minutes", "avg_minutes"])

    # Por cada combinación de agregación y vip...
    for agg, vip in list(itertools.product(v_agg, v_vip)):
        # Se corrige la fecha de inicio si corresponde a agregación mensual
        fechas_con = fechas.copy()
        if agg == "MONTH":
            fechas_con[0] = datetime.strptime(
                fechas[0], "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")

        # Ejecución de la consulta a la API Mediastream
        req = generar_req(agg, fechas_con, vip)
        response = requests.post(endpoint, headers=headers, json=req)
        # Formato de datos
        df = formatear_df(pd.DataFrame(response.json()["data"]), vip, agg)

        # Finalmente, se adjunta a tabla final
        df_total = pd.concat([df_total, df])

    subir_bq(df_total)


def actualizar_tabla():
    """Proceso de actualización de tabla con datos nuevos
    """
    
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)
    
    # Ejecución de función merge para actualizar datos en función de los cargados a la tabla auxiliar
    update_job = client.query(
        """
        MERGE `conexion-datos-rdf.funnel_vip.funnel_vip_emisorOPE` t_final
        USING `conexion-datos-rdf.funnel_vip.funnel_vip_emisorOPE_update` t_update
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
    "funnel_vip_emisor",
    default_args=args,
    description="Actualización semanal de funnel Emisor",
    schedule_interval="30 8 * * *",
    catchup=False,
    start_date=datetime(2022, 1, 3),
    tags=["audio", "platform", "semanal", "funnel"],
) as dag:

    # Se definen los pasos del dag según funciones anteriores
    t1 = PythonOperator(
        task_id="descargar_data",
        python_callable=descargar_data
    )

    t2 = PythonOperator(
        task_id="actualizar_tabla",
        python_callable=actualizar_tabla
    )

    # Configuración de la ejecución del DAG.
    t1 >> t2 
