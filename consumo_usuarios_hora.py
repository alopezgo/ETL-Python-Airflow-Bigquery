""" Audiencias y Consumo - Audio Digital

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos del detalle de consumo de audio digital por playback, y ser
programadas para la extracción periódica en la plataforma Apache Airflow.

"""

# Importación de librerías
from datetime import date, datetime, timedelta

import pandas as pd
import pendulum
import pytz
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# Configuración de BigQuery. La ruta corresponde a la dirección del archivo
# de credenciales dentro del servidor de Airflow. Además se configura dirección
# de base de IPs
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
bq_table = "conexion-datos-rdf.consumo_usuarios.consumo"

# Datos de autenticación para API Mediastream
headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
cl = pytz.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'
agregaciones = ["MONTH", "DAY"]
local_tz = pendulum.timezone("America/Santiago")

# region Funciones Auxiliares


def convertir_fechas(fechas: list, agg: str) -> list:
    """Conversión de rango de fechas a formato UTC según agregación

    Args:
        fechas (list): rango de fechas original (CL)
        agg (str): agregación temporal

    Returns:
        list: rango de fechas (UTC)
    """
    # Conversión fecha de inicio. Se remplaza por inicio de mes para agregación mensual
    fecha_inicio = datetime.strptime(fechas[0], "%Y-%m-%d %H:%M:%S")
    if agg == "MONTH":
        fecha_inicio = fecha_inicio.replace(day=1)
    fecha_inicio_cl = cl.localize(fecha_inicio)
    fecha_inicio_utc = fecha_inicio_cl.astimezone(utc).strftime(fmt)

    # Conversión fecha de término. Se considera la hora al término del día
    fecha_fin_cl = cl.localize(
        datetime.strptime(fechas[1], "%Y-%m-%d %H:%M:%S"))
    fecha_fin_utc = fecha_fin_cl.astimezone(utc).strftime(fmt)

    return [fecha_inicio_utc, fecha_fin_utc]


def generar_req(fechas: list, agg: str) -> dict:
    """Generación automática del payload para consulta a API de Mediastream

    Args:
        fechas (list): rango de fechas a consultar (UTC)
        agg (str): agregación de tiempo

    Returns:
        dict: payload con el formato para consulta
    """
    
    # El payload corresponde a un diccionario json con elementos obligatorios
    req = {}
    # Nombre de la consulta
    req["name"] = "full_by_time"
    # Dimensiones. Se consultarán por distintos niveles de desagregación
    req["dimension"] = ["user_id", "content_live", "country_name",
                        "content_id", "referrer", "device", "device_category"]

    # Filtros. Se debe incluír siempre el filtro de fechas, que se convierten previamente.
    # Se añade filtro del grupo de marca
    fechas = convertir_fechas(fechas, agg)
    req["filter"] = [{"name": "date", "op": [">=", "<="], "value": fechas}]

    # Se señalan opciones por defecto y agregación a consultar
    req["calendar"] = [{"type": "all"}]
    req["time"] = 0
    req["trunc"] = [agg]
    return req


def response_to_dataframe(response: requests.Response, agg: str) -> pd.DataFrame:
    """Función de formateo de respuesta desde API Mediastream a tabla en BigQuery

    Args:
        response (requests.Response): Respuesta desde API Mediastream
        agg (str): agregación temporal consultada

    Returns:
        pandas.DataFrame: datos con formato para subir a BigQuery
    """
    # Se crea un DataFrame vacío con las columnas finales
    df_formato = pd.DataFrame(columns=['start', 'stream', 'device', 'minutes', 'avg_minutes', 'content_type',
                                       'date_time', 'user_id', 'referrer', 'duration', 'device_category',
                                       'device_1', 'country_name', 'content_live', 'content_id'])

    # Se crea un DataFrame por defecto con los datos provenientes desde la API
    df = pd.concat([df_formato, pd.DataFrame(
        response.json()["data"])])

    # Se añade una columna con el tipo de agregación convertido
    periodo = ""
    if agg == "MONTH":
        periodo = "mensual"
    elif agg == "DAY":
        periodo = "diario"
    elif agg == "HOUR":
        periodo = "hora"
    df["periodo"] = periodo

    # Se convierten los tipos de datos de texto a número según corresponda
    df["device"] = df["device"].astype(int)
    df["avg_minutes"] = df["avg_minutes"].astype(float)
    df["minutes"] = df["minutes"].astype(float)
    df["duration"] = df["duration"].astype(float)
    
    # Se convierten columnas con datos de hora
    df["date_time"] = pd.to_datetime(df["date_time"]).dt.tz_localize(
        utc).dt.tz_convert(cl).dt.tz_localize(None)
    df["so"] = df["device_1"].astype(str)

    # Se filtran las filas que no posean todos los detalles requeridos
    df = df[df["user_id"].notnull()]
    
    df.dtypes
    df_final = df[["date_time", "periodo", "user_id", "content_id", "referrer", "content_type", "content_live",
                   "country_name", "device_category", "so", "start", "stream", "device", "minutes", "avg_minutes"]].convert_dtypes()

    return df_final

# endregion

# region Funciones para Ejecutar


def descargar_data(agg):
    """Descarga de datos desde API de Mediastream y conversión a formato adecuado

    Args:
        soporte (str): marca a consultar

    Returns:
        pandas.DataFrame: datos con formato
    """
    
    # Según la cantidad de días a remplazar se genera el rango de fechas a consultar
    ts = datetime.now()
    end_time = ts.strftime("%Y-%m-%d %H:%M:%S")
    start_time = ts.strftime("%Y-%m-%d") + " 00:00:00"
    fechas = [start_time, end_time]

    # Se genera el payload para consultar a API de Mediastream y se ejecuta
    req_day = generar_req(fechas, agg)
    print(req_day)

    resp = requests.post(endpoint, headers=headers, json=req_day)
    
    # Se convierte respuesta al formato correspondiente
    df_day = response_to_dataframe(resp, agg)

    return df_day


def upload_to_bq(df_total):
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pandas.DataFrame): DataFrame con formato
    """
    
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    # Configuración del trabajo. Debido a los tipos de datos involucrados, se debe 
    # declarar el esquema manualmente.
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                "date_time", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField(
                "periodo", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "user_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "content_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "referrer", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "content_live", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "country_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "device_category", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("so", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("start", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField(
                "stream", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField(
                "device", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("minutes", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField(
                "avg_minutes", bigquery.enums.SqlTypeNames.FLOAT),
        ],
        # write_disposition="WRITE_TRUNCATE",
    )

    # Se carga DataFrame a BigQuery y se espera el resultado
    job = client.load_table_from_dataframe(
        df_total, bq_table, job_config=job_config)
    job.result()

# endregion

# region Funciones Principales


def del_curr():
    """Eliminación de datos para remplazo. Debido al método de consulta a la 
    API y la imposibilidad de realizar actualizaciones incrementales, se 
    eliminan los datos de los últimos días y se remplazan con los datos actualizados
    """

    # Inicio de sesión de BigQuery con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    # Se determina la fecha de inicio de la consulta para eliminar según agregación
    ts = date.today()
    fecha_inicio = ts.strftime("%Y-%m-%d")
    fecha_inicio_m = ts.replace(day=1).strftime("%Y-%m-%d")

    # Data diaria/hora
    update_job = client.query(
        """
        DELETE FROM `{0}` 
        WHERE date_time >= "{1}"
        """.format(bq_table, fecha_inicio))

    print(update_job.result())

    # Data Mensual
    update_job_m = client.query(
        """
        DELETE FROM `{0}` 
        WHERE date_time >= "{1}"
        AND periodo = "mensual"
        """.format(bq_table, fecha_inicio_m))

    print(update_job_m.result())


# Para cada agregación se realiza la extracción, transformación y carga de los datos 
# según las funciones anteriores: se obtienen los datos y se convierten, para luego
# realizar la carga a BigQuery
def actualizar_mes():
    agg = "MONTH"

    df = descargar_data(agg)

    upload_to_bq(df)


def actualizar_dia():
    agg = "DAY"

    df = descargar_data(agg)

    upload_to_bq(df)

# endregion


# Argumentos por defecto de Airflow. Se configura un tiempo de espera de 5 
# minutos entre intentos de ejecución
args = {
    'owner': 'audiencias',
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
    "consumo_usuarios_hora",
    default_args=args,
    description="Actualización diaria de los datos de consumo de audio digital.",
    start_date=datetime(2022, 9, 20, tzinfo=local_tz),
    tags=["audio", "platform", "diario", "consumo", "CIA"],
    
# Se definen los pasos del dag según funciones anteriores
) as dag:
    clear = PythonOperator(
        task_id="limpiar_reciente",
        python_callable=del_curr
    )

    dia = PythonOperator(
        task_id="actualizar_dia",
        python_callable=actualizar_dia
    )

    mes = PythonOperator(
        task_id="actualizar_mes",
        python_callable=actualizar_mes
    )

    # Configuración de la ejecución del DAG.
    clear >> [dia, mes]
