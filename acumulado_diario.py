""" Audiencias y Consumo - Acumulado Diario

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos de audiencias mensuales acumulados a cada día del mes, y ser
programadas para la extracción periódica en la plataforma Apache Airflow.

"""

# Importación de librerías
import itertools
from datetime import datetime, timedelta, date

import pandas as pd
import pendulum
import pytz
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# Configuración de BigQuery. La ruta corresponde a la dirección del archivo
# de credenciales dentro del servidor de Airflow.
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
bq_table = "conexion-datos-rdf.audio_digital.acumulado_diario"

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
cl = pytz.timezone("America/Santiago")
local_tz = pendulum.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S'
fmt_d = '%Y-%m-%d'

# Diccionarios útiles
fechas_soportes = {
    "oasisfm.cl": [date(2019,8,1), date(2022,10,31)],
    "13cradio.cl": [date(2022,11,1), date.max],
    "playfm.cl": [date(2019,8,1), date.max],
    "sonarfm.cl": [date(2019,8,1), date.max],
    "tele13radio.cl": [date(2019,8,1), date.max],
    "horizonte.cl": [date(2019,8,1), date.max]
}

# Tipos de contenido a extraer por marca
content_types = {
    "horizonte.cl": [None],
    "oasisfm.cl": [None],
    "13cradio.cl": [None],
    "playfm.cl": [None],
    "sonarfm.cl": [None],
    "tele13radio.cl": [None]
}

# ID de grupos por marca dentro de Mediastream Platform
grupo = {
    "horizonte.cl": "3570670d5064d58ee8ccdd3650506e3a",
    "oasisfm.cl": "35bfeb595ed83ebec22c1e3b5ed28f7f",
    "13cradio.cl": "2ea634f2e3058deeb047e998ba69c0c0",
    "playfm.cl": "06c2d59cd238c5848572bc1874acb044",
    "sonarfm.cl": "2757a2c258842237e149c98e55073b31",
    "tele13radio.cl": "5ae0b4ae44d45dbbaef992ea99512079"
}

# ID de contenido de transmisión live
content_live = {
    "horizonte.cl": "601415b58308405b0d11c82a",
    "oasisfm.cl": "5c915497c6fd7c085b29169d",
    "13cradio.cl": "5c915497c6fd7c085b29169d",
    "playfm.cl": "5c8d6406f98fbf269f57c82c",
    "sonarfm.cl": "5c915724519bce27671c4d15",
    "tele13radio.cl": "5c915613519bce27671c4caa"
}

# Diccionario de traducción de soporte
live_filter = {
    "horizonte.cl": "Horizonte",
    "oasisfm.cl": "Oasis FM",
    "13cradio.cl": "13c Radio",
    "playfm.cl": "Play FM",
    "sonarfm.cl": "Sonar FM",
    "tele13radio.cl": "Tele 13 Radio"
}

# ID de grupo de stream vip
stream_vip = {
    5: "b3583f38d358a82ecb3b6783664f1305"
}

# Datos de autenticación para API Mediastream
headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"


def del_current_data():
    """Eliminación de datos para remplazo. Debido al método de consulta a la 
    API y la imposibilidad de realizar actualizaciones incrementales, se 
    eliminan los datos de los últimos días y se remplazan con los datos actualizados
    """

    # Se extrae la cantidad de días a remplazar de las variables de Airflow
    replace_days = int(Variable.get("dias_remplazo"))

    # Se determina la fecha de inicio de la consulta para eliminar
    now_dt = datetime.now(cl)
    f_inicio_consulta = (
        now_dt + timedelta(days=-replace_days)).date().strftime(fmt_d)

    # Inicio de sesión de BigQuery con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    # Eliminación de los últimos días de datos
    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha_termino >= '{1}'
        """.format(bq_table, f_inicio_consulta))

    # Se imprimen logs para reconocer la ejecución
    print(f'Borrando periodo diario desde: ' + f_inicio_consulta)
    print(update_job_d.result())


def gen_array_dias(soporte:str) -> list:
    """Generación de la lista de los rangos de días a descargar desde la API de Mediastream

    Returns:
        list: lista de rangos de días a consultar 
    """

    # Se extrae la cantidad de días a remplazar de las variables de Airflow
    replace_days = int(Variable.get("dias_remplazo"))
    now_dt = datetime.now(cl)

    # Se determinan las fechas de inicio y término de la consulta
    f_dia_inicio = (now_dt + timedelta(days=-replace_days)).date()
    f_dia_termino = now_dt.date()

    start_soporte = date.min
    end_soporte = date.max

    start_soporte = fechas_soportes[soporte][0]
    end_soporte = fechas_soportes[soporte][1]

    f_dia_inicio = max(f_dia_inicio, start_soporte)
    f_dia_termino = min(f_dia_termino, end_soporte)

    # Se genera un rango de fechas entre ambas
    rango_dias = list(pd.date_range(f_dia_inicio, f_dia_termino, freq="1D"))

    # Para cada día dentro del rango, se genera un rango desde el inicio de ese mes hasta
    # la fecha correspondiente
    rangos = []
    for dia in rango_dias:
        
        if dia.day== 1:
            f_inicio = (cl.localize(dia) + timedelta(days=-1)).replace(day=1).astimezone(utc).strftime(fmt)
            f_fin = cl.localize(dia).astimezone(utc).strftime(fmt)
        else:
            f_inicio = cl.localize(dia.replace(day=1)).astimezone(utc).strftime(fmt)
            f_fin = cl.localize(dia).astimezone(utc).strftime(fmt)

        rangos.append([f_inicio, f_fin])

    # Se devuelve el listado de rangos a consultar
    return rangos


def generar_req(fechas: list, soporte: str) -> dict:
    """Generación automática del payload para consulta a API de Mediastream

    Args:
        fechas (list): rango de fechas a consultar
        soporte (str): marca a consultar

    Returns:
        dict: payload con el formato para consulta
    """
    # El payload corresponde a un diccionario json con elementos obligatorios
    req = {}
    # Nombre de la consulta
    req["name"] = "full_by_time"
    # Dimensiones, en este caso sólo se consulta el contenido live/OD
    req["dimension"] = ["content_live"]

    # Filtros. Se debe incluír siempre el filtro de fechas. 
    # Se añade filtro del grupo e ID de live de marca, y grupo de stream vip 5
    req["filter"] = [
        {"name": "date", "op": [">=", "<="], "value":fechas},
        #{"name": "group", "value": [grupo[soporte]],"logic_op": "and", "order": 1},
        {"name": "content_id", "value": [content_live[soporte]],"logic_op": "and", "order": 2},
        {"name": "group", "value": [stream_vip[5]],"logic_op": "and", "order": 3}
    ]

    # Se señalan opciones por defecto y agrupación mensual
    req["calendar"] = {"type": "all"}
    req["time"] = "0"
    req["trunc"] = ["MONTH"]

    return req


def response_to_dataframe(response: requests.Response, soporte: str, vip: int) -> pd.DataFrame:
    """Función de formateo de respuesta desde API Mediastream a tabla en BigQuery

    Args:
        response (requests.Response): Respuesta desde API Mediastream
        soporte (str): marca consultada
        vip (int): grupo de stream vip consultado

    Returns:
        pandas.DataFrame: datos con formato para subir a BigQuery
    """

    # Se crea un DataFrame por defecto con los datos provenientes desde la API
    df = pd.DataFrame(response.json()["data"])

    # En caso de que no existan datos se retorna un DataFrame vacío
    if df.empty: return df

    # Se añaden columnas de soporte y vip
    df["soporte"] = soporte
    df["vip"] = "v{0}".format(vip)

    # Se filtran sólo las filas de contenido live/OD según soporte
    # df_2 = df[df["content_type"].isin(["OnDemand", "OPE"])].copy()
    # if soporte != "emisorpodcasting.cl":
    #     df_2 = df[df["content_live"].isin([None, live_filter[soporte]])].copy()

    return df[["soporte", "content_type", "vip",
                "stream", "device"]]


def descargar_data(soporte: str, fechas: list) -> pd.DataFrame:
    """Descarga de datos desde API de Mediastream y conversión a formato adecuado

    Args:
        soporte (str): marca a consultar
        fechas (list): rango de fechas a consultar

    Returns:
        pandas.DataFrame: datos con formato
    """
    # Inicio de sesión en mediastream
    #headers, endpoint = sesion_platform()

    # Extracción de consultas según marca y creación de DataFrame final
    consultas = content_types[soporte]
    print("Descargando datos: " + soporte)
    df_soporte = pd.DataFrame(columns=["soporte"])

    # Para cada combinación de tipo de contenido y vip a consultar...
    for content_type, vip in list(itertools.product(consultas, stream_vip.keys())):
        print("{0}, {1}, {2}, vip {3}".format(
            fechas, soporte, content_type, vip))

        # Se genera el request para consultar a la API y se envía
        req = generar_req(fechas, soporte)
        response = requests.post(endpoint, headers=headers, json=req)

        # Se esperan los resultados y se convierten según formato
        try: df = response_to_dataframe(response, soporte, vip)
        except Exception as e: print("Error en resultados de Mediastream: ", response)

        # En caso de que no existan datos se retorna un DataFrame vacío
        if df.empty: return df

        # Se añaden columnas que indican el rango de fechas consultado
        df["fecha_inicio"] = pd.to_datetime(
            fechas[0]).tz_localize(utc).tz_convert(cl).date()
        df["fecha_termino"] = pd.to_datetime(
            fechas[1]).tz_localize(utc).tz_convert(cl).date()

        # Se añade al DataFrame final
        df_soporte = pd.concat([df_soporte, df])

    # Se ordenan los datos según las columnas señaladas
    df_soporte.sort_values(
        by=["soporte", "fecha_inicio", "fecha_termino", "content_type"], inplace=True)
    # Se seleccionan las columnas finales
    df_soporte = df_soporte[["soporte", "fecha_inicio",
                             "fecha_termino", "content_type", "vip", "stream", "device"]]
    # Se elimina el índice original previo al orden
    df_soporte.reset_index(drop=True, inplace=True)

    return df_soporte


def upload_to_bq(df: pd.DataFrame) -> None:
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pandas.DataFrame): DataFrame con formato
    """

    # Inicio de sesión
    client = bigquery.Client.from_service_account_json(_google_key)

    # Configuración del trabajo. Se detecta esquema automáticamente, y se añaden opciones de 
    # clusterización y particionamiento de los datos
    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["soporte", "content_type", "vip"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="fecha_inicio",
        )
    )

    # Se carga DataFrame a BigQuery y se espera el resultado
    job = client.load_table_from_dataframe(df, bq_table, job_config=job_config)
    print(job.result())

# Para cada marca se realiza la extracción, transformación y carga de los datos 
# según las funciones anteriores: se generan los rangos a consultar, se obtienen 
# los datos para cada rango, y se cargan a BigQuery
def etl_horizonte():

    rango = gen_array_dias("horizonte.cl")
    df_total = pd.DataFrame()

    for fecha in rango:
        df = descargar_data("horizonte.cl", fecha)
        df_total = pd.concat([df_total, df])
    upload_to_bq(df_total)


def etl_oasis():

    rango = gen_array_dias("oasisfm.cl")
    df_total = pd.DataFrame()

    for fecha in rango:
        df = descargar_data("oasisfm.cl", fecha)
        df_total = pd.concat([df_total, df])
    upload_to_bq(df_total)


def etl_13cradio():

    rango = gen_array_dias("13cradio.cl")
    df_total = pd.DataFrame()

    for fecha in rango:
        df = descargar_data("13cradio.cl", fecha)
        df_total = pd.concat([df_total, df])
    upload_to_bq(df_total)


def etl_play():

    rango = gen_array_dias("playfm.cl")
    df_total = pd.DataFrame()

    for fecha in rango:
        df = descargar_data("playfm.cl", fecha)
        df_total = pd.concat([df_total, df])
    upload_to_bq(df_total)


def etl_sonar():

    rango = gen_array_dias("sonarfm.cl")
    df_total = pd.DataFrame()

    for fecha in rango:
        df = descargar_data("sonarfm.cl", fecha)
        df_total = pd.concat([df_total, df])
    upload_to_bq(df_total)


def etl_tele13radio():

    rango = gen_array_dias("tele13radio.cl")
    df_total = pd.DataFrame()

    for fecha in rango:
        df = descargar_data("tele13radio.cl", fecha)
        df_total = pd.concat([df_total, df])
    upload_to_bq(df_total)

# Actualización de variables de Airflow para seguimiento de ejecución
def flag_off():
    Variable.update(key="acumulados_device",
                    value=False, serialize_json=True)


def flag_on():
    Variable.update(key="acumulados_device",
                    value=True, serialize_json=True)


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
    "acumulado_diario",
    default_args=args,
    description="Actualización diaria de acumulados device y stream. Contenido Live y vip5.",
    schedule_interval="0 7 * * *",
    catchup=False,
    start_date=datetime(2022, 6, 8, tzinfo=local_tz),
    tags=["device", "stream", "live", "vip"],

# Se definen los pasos del dag según funciones anteriores
) as dag:
    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off
    )

    borrar = PythonOperator(
        task_id="del_remplazo",
        python_callable=del_current_data
    )

    hor = PythonOperator(
        task_id="actualizar_horizonte",
        python_callable=etl_horizonte
    )

    trececr = PythonOperator(
        task_id="actualizar_13cradio",
        python_callable=etl_13cradio
    )

    pla = PythonOperator(
        task_id="actualizar_play",
        python_callable=etl_play
    )

    son = PythonOperator(
        task_id="actualizar_sonar",
        python_callable=etl_sonar
    )

    t13 = PythonOperator(
        task_id="actualizar_tele13",
        python_callable=etl_tele13radio
    )
    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on
    )

    # Configuración de la ejecución del DAG.
    marca_reset >> borrar >> [hor, trececr, pla, son, t13] >> marca_ok
