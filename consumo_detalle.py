""" Audiencias y Consumo - Detalle de Consumo

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos del detalle de consumo de audio digital por playback, y ser
programadas para la extracción periódica en la plataforma Apache Airflow.

"""

# Importación de librerías
import os
from datetime import date, datetime, time, timedelta

import maxminddb
import pandas as pd
import pendulum
import pytz
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from user_agents import parse as agent_parse

# Configuración de BigQuery. La ruta corresponde a la dirección del archivo
# de credenciales dentro del servidor de Airflow. Además se configura dirección
# de base de IPs
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
bq_table = "conexion-datos-rdf.consumo.consumo_detalle"
ip_database_path = "/home/airflow/airflow/dags/dbip.mmdb"

# Datos de autenticación para API Mediastream
headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/export"

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
cl = pytz.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'

# region Funciones Auxiliares

def gen_qdates(start_date: datetime, end_date: datetime, freq: str = "12H") -> list:
    """Generación de la lista de los rangos de días a descargar desde la API de Mediastream

    Args:
        start_date (datetime): Fecha de inicio
        end_date (datetime): Fecha de término
        freq (str, optional): Frecuencia de cada rango. Por defecto en "12H".

    Returns:
        list: lista de rangos de fechas
    """
    
    # Se genera un rango de fechas entre ambas fechas con la frecuencia ingresada
    pers = pd.date_range(start_date, end_date, freq=freq).tolist()

    final_list = []

    # Por cada datetime en el rango hasta el penúltimo...
    for i in range(len(pers) - 1):
        # Se genera un rango de fechas entre una fecha y la siguiente en la lista
        start_date_utc = pers[i].strftime(fmt)
        end_date_utc = pers[i+1].strftime(fmt)

        # Se añade a la lista final
        final_list.append([start_date_utc, end_date_utc])

    # Si la fecha de término no alcanzó a generar un rango completo, se agrega manualmente
    if final_list[-1][-1] != end_date.strftime(fmt): 
        start_date_utc = pers[i+1].strftime(fmt)
        end_date_utc = end_date.strftime(fmt)

        final_list.append([start_date_utc, end_date_utc])

    return final_list


def gen_req(fechas: list) ->dict:
    """Generación automática del payload para consulta a API de Mediastream

    Args:
        fechas (list): rango de fechas a consultar

    Returns:
        dict: payload con el formato para consulta
    """

    # Debido a la simplicidad del payload para esta consulta, se retorna inmediatamente
    return {
        "name": "full_by_time_date",
        "dimension": [
            "user_id", "user_agent", "request_ip", "session",
            "content_id", "content_live", "isp", "property"
        ],
        "filter": [
            {"name": "date", "op": [">=", "<="], "value": fechas}
        ],
        "trunc": ["MINUTE"]
    }

# endregion

# region Funciones para Ejecutar


def download_data(req: dict, total_attempts: int = 3) -> pd.DataFrame:
    """Descarga de datos desde API de Mediastream y conversión a formato adecuado

    Args:
        req (dict): payload con el formato para consulta
        total_attempts (int, optional): Intentos de descarga de los datos. Por defecto en 3.

    Returns:
        pd.DataFrame: datos con formato para subir a BigQuery
    """
    # Mientras no se supere la cantidad de intentos...
    attempts = 0
    while attempts < total_attempts:
        # Se intenta descargar los datos desde API de Mediastream
        try:
            # Se obtiene la respuesta al requerimiento a la API de descarga
            resp = requests.post(endpoint, headers=headers, json=req)
            # Se convierte respuesta a DataFrame
            df = pd.read_csv(resp.json()["data"][0])

            # Se convierten columnas de datetime al tipo de dato correspondiente
            df["start_date"] = pd.to_datetime(
                df["start_date"]).dt.tz_localize(None)
            df["end_date"] = pd.to_datetime(
                df["end_date"]).dt.tz_localize(None)

            # Se devuelve Dataframe con formato
            return df[["start_date", "end_date", "user_id", "user_agent", "request_ip", "session", "minutes",
                       "content_id", "content_type", "content_live", "property", "isp"]], resp.json()["processedMB"]
        # En caso de error, se descuenta un intento y se imprime el mensaje
        except Exception as e:
            print(e, resp.json())
            attempts += 1

    # En caso de agotarse los intentos, se declara el fallo
    raise Exception("Cantidad de intentos superados para descarga de datos")

def append_device_data(df: pd.DataFrame) -> pd.DataFrame:
    """Obtención de los datos del dispositivo según user_agent

    Args:
        df (pandas.DataFrame): DataFrame con datos, requiere columna "user_agent"

    Returns:
        pandas.DataFrame: DataFrame con datos de dispositivos
    """

    # Por cada user_agent único en el DataFrame...
    ua_details = []
    for agent in list(df["user_agent"].unique()):
        # Se genera un diccionario con un identificador
        ua_det = {}
        ua_det["user_agent"] = agent

        # Se obtienen los detalles del user_agent
        user_agent = agent_parse(agent)

        # Se asigna un tipo de dispositivo según corresponda
        if user_agent.is_pc:
            ua_det["device_type"] = "Desktop"
        elif user_agent.is_mobile:
            ua_det["device_type"] = "Phone"
        elif user_agent.is_tablet:
            ua_det["device_type"] = "Tablet"
        else:
            ua_det["device_type"] = "Other"

        # Se obtiene el sistema del dispositivo
        ua_det["system"] = user_agent.os.family

        # Se añade detalle a listado final
        ua_details.append(ua_det)

    # Se convierte listado de detalles únicos a DataFrame y se une al DataFrame original
    df_devices = pd.DataFrame(ua_details)
    df_total_dev = pd.merge(left=df, right=df_devices,
                            how="left", left_on="user_agent", right_on="user_agent")

    return df_total_dev


def append_location_data(df: pd.DataFrame) -> pd.DataFrame:
    """Obtención de los datos de ubicación según ip

    Args:
        df (pd.DataFrame): DataFrame con datos, requiere columna "request_ip"

    Returns:
        pd.DataFrame: DataFrame con datos de ubicación
    """

    # Se carga base de datos de direcciones IP
    ip_database = maxminddb.open_database(ip_database_path)

    # Por cada request_ip único en el DataFrame...
    ip_details = []
    for ip in list(df["request_ip"].unique()):
        # Se genera un diccionario con un identificador
        ip_det = {}
        ip_det["request_ip"] = ip

        # Se obtienen los detalles del user_agent
        try:
            rec = ip_database.get(ip)

            ip_det["continent_code"] = rec["continent"]["code"]
            try: ip_det["continent_name"] = rec["continent"]["names"]["es"]
            except: ip_det["continent_name"] = rec["continent"]["names"]["en"]

            ip_det["country_code"] = rec["country"]["iso_code"]
            try: ip_det["country_name"] = rec["country"]["names"]["es"]
            except: ip_det["country_name"] = rec["country"]["names"]["en"]

            if "subdivisions" in rec:
                try: ip_det["region_code"] = rec["subdivisions"][0]["iso_code"]
                except: pass
                try: ip_det["region_name"] = rec["subdivisions"][0]["names"]["es"]
                except: ip_det["region_name"] = rec["subdivisions"][0]["names"]["en"]

            try: ip_det["city_code"] = rec["city"]["geoname_id"]
            except: continue
            try: ip_det["city_name"] = rec["city"]["names"]["es"]
            except: ip_det["city_name"] = rec["city"]["names"]["en"]

            ip_det["city_lat"] = rec["location"]["latitude"]
            ip_det["city_lon"] = rec["location"]["longitude"]
            ip_det["time_zone"] = rec["location"]["time_zone"]

            try: ip_det["asn"] = rec["traits"]["autonomous_system_number"]
            except: pass
            
            ip_det["connection_type"] = rec["traits"]["connection_type"]
            ip_det["user_type"] = rec["traits"]["user_type"]

            # Se añade detalle a listado final
            ip_details.append(ip_det)
        # Si la detección de la IP falla, se informa del error
        except Exception as e:
            print("{0}: {1}".format(ip, e))
            continue

    # Se convierte listado de detalles únicos a DataFrame y se une al DataFrame original
    df_location = pd.DataFrame(ip_details)
    df_total_ip = pd.merge(left=df, right=df_location,
                           how="left", left_on="request_ip", right_on="request_ip")

    return df_total_ip


def upload_bq(df: pd.DataFrame):
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pandas.DataFrame): DataFrame con formato
    """

    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    # Configuración del trabajo. Debido a los tipos de datos involucrados, se debe 
    # declarar el esquema manualmente. Se añaden opciones de clusterización y 
    # particionamiento de los datos
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("start_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("end_date", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("user_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("user_agent", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("request_ip", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("session", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("minutes", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("content_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("content_live", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("property", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("device_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("system", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("continent_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("continent_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("country_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("country_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("region_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("region_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("city_code", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("city_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("city_lat", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("city_lon", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("time_zone", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("asn", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("isp", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("connection_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("user_type", bigquery.enums.SqlTypeNames.STRING)
        ],
        clustering_fields=["user_id", "content_id", "property"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="start_date",
        )
    )

    # Se carga DataFrame a BigQuery y se espera el resultado
    job = client.load_table_from_dataframe(df, bq_table, job_config=job_config)
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

    # Se extrae la cantidad de días a remplazar de las variables de Airflow
    replace_days = int(Variable.get("dias_remplazo"))

    # Se determina la fecha de inicio de la consulta para eliminar
    ts_date = datetime.combine(date.today(), time(0, 0)).astimezone(utc)
    start_time = (ts_date + timedelta(days=-replace_days)).replace(tzinfo=None)

    # Se ejecuta consulta
    update_job = client.query(
        """
        DELETE FROM `{0}` 
        WHERE start_date >= "{1}"
        """.format(bq_table, start_time))

    print(update_job.result())


def descargar_consumo():
    """Descarga de datos desde API de Mediastream y conversión a formato adecuado
    """
    
    # Se extrae la cantidad de días a remplazar de las variables de Airflow
    replace_days = int(Variable.get("dias_remplazo"))

    # Se determina la fecha de inicio de la consulta para eliminar
    ts = datetime.now().astimezone(utc)
    ts_date = datetime.combine(date.today(), time(0, 0)).astimezone(utc)
    start_time = (ts_date + timedelta(days=-replace_days)).replace(tzinfo=None)
    end_time = ts.replace(tzinfo=None)

    # Extracción de consultas según marca y creación de DataFrame final
    qdates = gen_qdates(start_time, end_time)

    df_total = pd.DataFrame()
    total_mb = 0
    print("{0} - {1}".format(qdates[0][0], qdates[-1][1]))

    # Para cada periodo en el rango de fechas...
    for dates in qdates:
        print(dates)

        # Se genera el payload de la consulta y se ejecuta
        req = gen_req(dates)
        df, processed_mb = download_data(req)

        total_mb += processed_mb

        # Se añaden detalles de dispositivo y ubicación
        df_2 = append_device_data(df)
        df_3 = append_location_data(df_2)

        # Se sube a BigQuery
        upload_bq(df_3)

    print(total_mb)


# Actualización de variables de Airflow para seguimiento de ejecución
def flag_off():
    Variable.update(key="correo_registrados_cia", value=False)
    Variable.update(key="correo_detalle_cia", value=False)


def flag_on():
    Variable.update(key="correo_registrados_cia", value=True)
    Variable.update(key="correo_detalle_cia", value=True)

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
}

# Configuración del DAG de Airflow. Se configuran tiempos de ejecución e inicio del mismo
with DAG(
    "consumo_detalle",
    default_args=args,
    description="Actualización diaria del detalle de consumo de audio digital",
    schedule_interval="0 5 * * *",
    catchup=False,
    start_date=datetime(
        2022, 4, 21,
        tzinfo=pendulum.timezone("America/Santiago")
    ),
    tags=["audio", "platform", "diario", "live", "on demand"],

# Se definen los pasos del dag según funciones anteriores
) as dag:
    del_repl = PythonOperator(task_id="eliminar_remplazo",
                              python_callable=del_curr)

    dl_data = PythonOperator(task_id="descargar_consumo",
                             python_callable=descargar_consumo)

    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off
    )

    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on
    )

    # Configuración de la ejecución del DAG.
    marca_reset >> del_repl >> dl_data >> marca_ok
