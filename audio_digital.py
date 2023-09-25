""" Audiencias y Consumo - Audio Digital

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos de audiencias de audio digital según distintas agrupaciones, y ser
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
# de credenciales dentro del servidor de Airflow.
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
bq_table = "conexion-datos-rdf.audio_digital.funnel_liveod_old"
bq_table_new = "conexion-datos-rdf.audio_digital.funnel_liveod_new"

# Datos de autenticación para API Mediastream
headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
cl = pytz.timezone("America/Santiago")
local_tz = pendulum.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'
agregaciones = ["MONTH", "DAY", "HOUR"]

# Diccionarios útiles
fechas_soportes = {
    "oasisfm.cl": [date(2019,8,1), date(2022,10,31)],
    "13cradio.cl": [date(2022,11,1), date.max],
    "playfm.cl": [date(2019,8,1), date.max],
    "sonarfm.cl": [date(2019,8,1), date.max],
    "tele13radio.cl": [date(2019,8,1), date.max],
    "horizonte.cl": [date(2019,8,1), date.max],
    "emisorpodcasting.cl": [date(2019,8,1), date.max]
}

# Tipos de contenido a extraer por marca
content_types = {
    "horizonte.cl": [None],
    "oasisfm.cl": [None],
    "13cradio.cl": [None],
    "playfm.cl": [None],
    "sonarfm.cl": [None],
    "tele13radio.cl": [None],
    "emisorpodcasting.cl": [None, "ope"]
}

# ID de grupos por marca dentro de Mediastream Platform
grupo = {
    "emisorpodcasting.cl": "b3583f38d358a82ecb3b6784664f1308",
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

# ID de grupo de stream vip
stream_vip = {
    0: None,
    1: "45afe3fdbe92500d3b72588549237cb3",
    5: "b3583f38d358a82ecb3b6783664f1305",
    20: "9e32ed53938d70972f115574cecc25d6",
    40: "e8bcb3395e6c37328e8cbb6424ebc376"
}

# Diccionario de traducción de soporte
live_filter = {
    "horizonte.cl": "Horizonte",
    "oasisfm.cl": "13c Radio",
    "13cradio.cl": "13c Radio",
    "playfm.cl": "Play FM",
    "sonarfm.cl": "Sonar",
    "tele13radio.cl": "Tele 13 Radio"
}

# region Funciones Auxiliares

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


def generar_req(fechas: list, agg: str, marca: str, content_type: str = None, id_vip: str = None) -> dict:
    """Generación automática del payload para consulta a API de Mediastream

    Args:
        fechas (list): rango de fechas a consultar (UTC)
        agg (str): agregación de tiempo
        marca (str): marca/soporte
        content_type (str, optional): tipo de contenido a consultar
        id_vip (str, optional): id del grupo de stream vip

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
    # Se añade filtro del grupo de marca
    date_range = convertir_fechas(fechas)
    req["filter"] = [
        {"name": "date", "op": [">=", "<="], "value":date_range},
        {"name": "group", "value": [grupo[marca]],
            "logic_op": "and", "order": 1}
    ]
    order = 2

    # Se añaden filtros de acuerdo al tipo de contenido consultado.
    if content_type == "live":
        # Para contenido live, se filtra sólo por el ID de contenido live
        req["filter"].append({"name": "content_id", "value": [
                             content_live[marca]], "logic_op": "and", "order": order})
        order = order + 1
    elif content_type == "on-demand":
        # Para contenido on demand, se filtra sólo por ese tipo de contenido
        req["filter"].append({"name": "content_type", "value": [
                             "aod"], "logic_op": "and", "order": order})
        order = order + 1
    elif content_type == "ope":
        # Para tipo especial OPE, se filtra por grupo "Only Property Emisor"
        # y se elimina contenido live
        req["filter"].append({"name": "group", "value": [
                             "084012cca93d1e0c2ef6e0d162389a44"], "logic_op": "and", "order": (order + 1)})  # G. OPE
        req["filter"].append({"name": "content_type", "value": [
                             "alive"], "logic_op": "and not", "order": (order + 2)})
        order = order + 3

    # Se añade filtro de acuerdo al stream vip a consultar
    if id_vip is not None:
        req["filter"].append(
            {"name": "group", "value": [id_vip], "logic_op": "and", "order": order})
        order = order + 1

    # Se señalan opciones por defecto y agrupación a consultar
    req["calendar"] = {"type": "all"}
    req["time"] = "0"
    req["trunc"] = [agg]

    return req


def response_to_dataframe(response: requests.Response, soporte: str, content_type: str, vip: int, agg: str) -> pd.DataFrame:
    """Función de formateo de respuesta desde API Mediastream a tabla en BigQuery

    Args:
        response (requests.Response): Respuesta desde API Mediastream
        soporte (str): marca consultada
        content_type (str): tipo de contenido consultado
        vip (int): grupo de stream vip consultado
        agg (str): agregación temporal consultada

    Returns:
        pd.DataFrame: datos con formato para subir a BigQuery
    """

    # Se crea un DataFrame por defecto con los datos provenientes desde la API
    df = pd.DataFrame(response.json()["data"])

    # En caso de que no existan datos se retorna un DataFrame vacío
    if df.empty: return df

    # Se añade columnas con datos de fecha
    df["date_time"] = pd.to_datetime(df["date_time"]).dt.tz_localize(
        utc).dt.tz_convert(cl).dt.tz_localize(None)
    df["fecha"] = df["date_time"].dt.date

    # Se añaden columnas con datos de hora de acuerdo a la agregación correspondiente
    if agg == "HOUR":
        df["hora_inicio"] = df["date_time"].dt.time.astype(str)
        df["hora_termino"] = (
            df["date_time"] + timedelta(hours=1, seconds=-1)).dt.time.astype(str)
    else:
        df["hora_inicio"] = "00:00:00"
        df["hora_termino"] = "23:59:59"

    # Se añade una columna con el tipo de agregación convertido
    periodo = ""
    if agg == "MONTH":
        periodo = "mensual"
    elif agg == "DAY":
        periodo = "diario"
    elif agg == "HOUR":
        periodo = "hora"
    df["periodo"] = periodo

    # Se añaden columnas de soporte y vip
    df["soporte"] = soporte
    df["vip"] = "v{0}".format(vip)

    # En caso de corresponder a contenido OPE, se cambia el nombre del tipo de contenido
    if content_type == "ope":
        df["content_type"].replace({"Ondemand": "OPE"}, inplace=True)
    else:
        df["content_type"].replace({"Ondemand": "OnDemand"}, inplace=True)

    # Se genera un id de fila para comprobar columnas únicas. Este incluirá soporte, fecha, 
    # agregación, vip y tipo de contenido y tendrá el siguiente formato de ejemplo: 
    # t13_21071600_h_v40_live
    df["id_suf"] = df["content_type"].replace(
        {"Live": "live", "OnDemand": "od", "OPE": "ope"})
    df["id_suf"] = "_v" + str(vip) + "_" + df["id_suf"]
    df["soporte_id"] = df["soporte"].str.lower().str[:3].replace({
        "tel": "t13"})
    df["id"] = df["soporte_id"] + "_" + \
        df["date_time"].dt.strftime("%y%m%d%H") + \
        "_" + periodo[0] + df["id_suf"]

    # Se filtran sólo las filas de contenido live/OD según soporte
    df_2 = df[df["content_type"].isin(["OnDemand", "OPE"])].copy()
    if soporte != "emisorpodcasting.cl":
        df_2 = df[df["content_live"].isin([None, live_filter[soporte]])].copy()

    return df_2[["id", "fecha", "hora_inicio", "hora_termino", "periodo", "soporte", "content_type", "vip",
                 "start", "stream", "device", "minutes", "avg_minutes"]]

# endregion

# region Funciones para Ejecutar


def descargar_data(soporte):
    """Descarga de datos desde API de Mediastream y conversión a formato adecuado

    Args:
        soporte (str): marca a consultar

    Returns:
        pandas.DataFrame: datos con formato
    """
    # Inicio de sesión en mediastream
    #headers, endpoint = sesion_platform()

    # Se extrae la cantidad de días a remplazar de las variables de Airflow
    replace_days = int(Variable.get("dias_remplazo"))

    # Según la cantidad de días a remplazar se genera el rango de fechas a consultar
    ts = date.today()
    end_time = ts + timedelta(days=-1)
    start_time = ts + timedelta(days=-replace_days)

    start_soporte = date.min
    end_soporte = date.max

    start_soporte = fechas_soportes[soporte][0]
    end_soporte = fechas_soportes[soporte][1]

    start_time = max(start_time, start_soporte)
    end_time = min(end_time, end_soporte)

    fechas = [start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")]

    # Extracción de consultas según marca y creación de DataFrame final
    consultas = content_types[soporte]
    print("Descargando datos: " + soporte)
    df_soporte = pd.DataFrame(columns=["id"])

    # Por cada combinación de agregación, consulta y vip correspondientes...
    for agg, content_type, vip in list(itertools.product(agregaciones, consultas, stream_vip.keys())):
        # Se copian las fechas de consutlas y se modifican para considerar el mes completo si corresponde
        fechas_con = fechas.copy()
        if agg == "MONTH":
            fechas_con[0] = datetime.strptime(
                fechas[0], "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")

        # Se imprime lína de logging
        print("{0}, {1}, {2}, {3}, vip {4}".format(
            fechas_con, agg, soporte, content_type, vip))

        # Se genera el payload para consultar a API de Mediastream y se ejecuta
        req = generar_req(fechas_con, agg, soporte,
                          content_type, stream_vip[vip])
        response = requests.post(endpoint, headers=headers, json=req)

        # Se convierte respuesta al formato correspondiente
        df = response_to_dataframe(response, soporte, content_type, vip, agg)

        # Se añade al DataFrame Final
        df_soporte = pd.concat([df_soporte, df])

    # Al finalizar, se ordenan los resultados y se elimina el índice previo
    df_soporte.sort_values(
        by=["fecha", "hora_inicio", "periodo", "soporte", "content_type"], inplace=True)
    df_soporte.reset_index(drop=True, inplace=True)

    return df_soporte


def upload_bq_old(df: pd.DataFrame):
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pandas.DataFrame): DataFrame con formato
    """
    
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    # Configuración del trabajo. Se detecta esquema automáticamente, y se añaden opciones de 
    # clusterización y particionamiento de los datos
    job_config = bigquery.LoadJobConfig(
        schema=[],
        # write_disposition="WRITE_TRUNCATE",
        clustering_fields=["periodo", "soporte", "content_type", "vip"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha",  # name of column to use for partitioning
        )
    )

    # Se carga DataFrame a BigQuery y se espera el resultado
    job = client.load_table_from_dataframe(df, bq_table, job_config=job_config)
    print(job.result())


def gen_query_new() -> str:
    """Generación de query en SQL para extraer devices new.
    
    Returns:
        str: query en SQL para extracción de datos
    """
    
    # Se extrae la cantidad de días a remplazar de las variables de Airflow
    replace_days = int(Variable.get("dias_remplazo"))

    # Se determinan las fechas de inicio y término de la consulta
    now_dt = datetime.now(cl)
    f_inicio_informe = (
        now_dt + timedelta(days=-replace_days)).replace(day=1)
    f_inicio_consumo = (f_inicio_informe +
                        timedelta(days=-replace_days)).astimezone(utc)
    fmt_d = '%Y-%m-%d'

    # Se insertan fechas en plantilla de la consulta
    query = """
select 
lives.soporte,
fechas.hora_inicio as inicio,
fechas.hora_fin as fin,
fechas.periodo,
vips.vip,
consumo.content_type,
count(distinct consumo.request_ip||consumo.user_agent) as devices_new
from `conexion-datos-rdf.consumo.consumo_detalle` as consumo
join `conexion-datos-rdf.diccionarios.dicc_lives` as lives
on consumo.content_id = lives.live_id
join `conexion-datos-rdf.diccionarios.dicc_vips` as vips
on IFNULL(consumo.minutes, 0) >= vips.vip
join `conexion-datos-rdf.diccionarios.dicc_fechas` as fechas
on DATETIME(consumo.start_date, "America/Santiago") < fechas.hora_fin
and datetime(consumo.end_date, "America/Santiago") >= fechas.hora_inicio
and fechas.hora_inicio >= "{0}"
and consumo.start_date >= "{1}"
group by lives.soporte, fechas.hora_inicio, 
fechas.hora_fin, fechas.periodo, vips.vip, consumo.content_type""".format(
        f_inicio_informe.strftime(fmt_d),
        f_inicio_consumo.strftime(fmt_d)
    )

    return query


def descargar_data_bq(query: str) -> pd.DataFrame:
    """Descarga de datos desde BigQuery a partir de una consulta

    Args:
        query (str): consulta en lenguaje SQL compatible

    Returns:
        pd.DataFrame: DataFrame con datos
    """
    
    # Inicio de sesión de BigQuery con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    # Ejecución de consulta y conversión a DataFrame de Pandas
    df = client.query(query).result().to_dataframe(
        create_bqstorage_client=False)
    
    return df


def upload_bq_new(df: pd.DataFrame) -> None:
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pandas.DataFrame): DataFrame con formato
    """

    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    # Configuración del trabajo. Se detecta esquema automáticamente, y se añaden opciones de 
    # clusterización y particionamiento de los datos
    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["soporte", "vip", "periodo", "content_type"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="inicio",
        )
    )

    # Se carga DataFrame a BigQuery y se espera el resultado
    job = client.load_table_from_dataframe(
        df, bq_table_new, job_config=job_config)
    print(job.result())

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

    # Se determina la fecha de inicio de la consulta para eliminar según agregación
    ts = date.today()
    fecha_inicio = (ts + timedelta(days=-replace_days)).strftime("%Y-%m-%d")
    fecha_inicio_m = (ts + timedelta(days=-replace_days)
                      ).replace(day=1).strftime("%Y-%m-%d")

    # Data día/hora old
    update_job = client.query(
        """
        DELETE FROM `{0}` 
        WHERE fecha >= "{1}"
        """.format(bq_table, fecha_inicio))

    print(update_job.result())

    # Data mes old
    update_job_m = client.query(
        """
        DELETE FROM `{0}` 
        WHERE fecha >= "{1}"
        AND periodo = "mensual"
        """.format(bq_table, fecha_inicio_m))

    print(update_job_m.result())

    # Data new
    update_job = client.query(
        """
        DELETE FROM `{0}` 
        WHERE inicio >= "{1}"
        """.format(bq_table_new, fecha_inicio_m))

    print(update_job.result())


# Para cada marca se realiza la extracción, transformación y carga de los datos 
# según las funciones anteriores: se obtienen los datos y se convierten, para luego
# realizar la carga a BigQuery
def actualizar_horizonte():
    df = descargar_data("horizonte.cl")

    upload_bq_old(df)


def actualizar_oasis():
    df = descargar_data("oasisfm.cl")

    upload_bq_old(df)


def actualizar_13cr():
    df = descargar_data("13cradio.cl")

    upload_bq_old(df)


def actualizar_play():
    df = descargar_data("playfm.cl")

    upload_bq_old(df)


def actualizar_sonar():
    df = descargar_data("sonarfm.cl")

    upload_bq_old(df)


def actualizar_tele13():
    df = descargar_data("tele13radio.cl")

    upload_bq_old(df)


def actualizar_emisor():
    df = descargar_data("emisorpodcasting.cl")

    upload_bq_old(df)

# Se genera y se ejecuta la carga de datos a la tabla de devices new
def actualizar_new():
    query = gen_query_new()

    df = descargar_data_bq(query)

    upload_bq_new(df)


# Actualización de variables de Airflow para seguimiento de ejecución
def flag_off():
    Variable.update(key="correo_audio_digital",
                    value=False, serialize_json=True)


def flag_on():
    Variable.update(key="correo_audio_digital",
                    value=True, serialize_json=True)

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
    "audio_digital",
    default_args=args,
    description="Actualización diaria de los datos de audio digital. Se incluyen los tipos de contenido y vip.",
    schedule_interval="0 6 * * *",
    catchup=False,
    start_date=datetime(2022, 1, 4, tzinfo=local_tz),
    tags=["audio", "platform", "diario", "live", "on demand"],
    
) as dag:
# Se definen los pasos del dag según funciones anteriores
    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off
    )

    borrar = PythonOperator(
        task_id="del_remplazo",
        python_callable=del_curr
    )

    hor = PythonOperator(
        task_id="actualizar_horizonte",
        python_callable=actualizar_horizonte
    )

    trececr = PythonOperator(
        task_id="actualizar_13cr",
        python_callable=actualizar_13cr
    )

    pla = PythonOperator(
        task_id="actualizar_play",
        python_callable=actualizar_play
    )

    son = PythonOperator(
        task_id="actualizar_sonar",
        python_callable=actualizar_sonar
    )

    t13 = PythonOperator(
        task_id="actualizar_tele13",
        python_callable=actualizar_tele13
    )

    emi = PythonOperator(
        task_id="actualizar_emisor",
        python_callable=actualizar_emisor
    )

    new = PythonOperator(
        task_id="actualizar_new",
        python_callable=actualizar_new
    )

    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on
    )

    # Configuración de la ejecución del DAG.
    marca_reset >> borrar >> [hor, trececr, pla, son, t13, emi] >> new >> marca_ok
