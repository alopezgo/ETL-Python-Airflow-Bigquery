
import itertools
from datetime import date, datetime, time, timedelta

import pandas as pd
import pendulum
import pytz
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

# Se establecen credenciales para autenticación
headers = {"X-API-Token": "bb46234943d0801b41fd027835d03b4b"}
endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
table_id = "conexion-datos-rdf.funnel_vip.map_consumo"

cl = pytz.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'
local_tz = pendulum.timezone("America/Santiago")

# Diccionarios útiles
fechas_soportes = {
    "oasisfm.cl": [date(2019, 8, 1), date(2022, 10, 31)],
    "13cradio.cl": [date(2022, 11, 1), date.max],
    "playfm.cl": [date(2019, 8, 1), date.max],
    "sonarfm.cl": [date(2019, 8, 1), date.max],
    "tele13radio.cl": [date(2019, 8, 1), date.max],
    "horizonte.cl": [date(2019, 8, 1), date.max]
}

content_live = {
    "13cradio.cl": "5c915497c6fd7c085b29169d",
    "horizonte.cl": "601415b58308405b0d11c82a",
    "oasisfm.cl": "5c915497c6fd7c085b29169d",
    "playfm.cl": "5c8d6406f98fbf269f57c82c",
    "sonarfm.cl": "5c915724519bce27671c4d15",
    "tele13radio.cl": "5c915613519bce27671c4caa"
}

stream_vip = {
    0: None,
    1: "45afe3fdbe92500d3b72588549237cb3",
    5: "b3583f38d358a82ecb3b6783664f1305",
    20: "9e32ed53938d70972f115574cecc25d6",
    40: "e8bcb3395e6c37328e8cbb6424ebc376"
}

v_emisor = [False, True]
v_var = ["device", "device_category", "property", "country_name"]
columns = ["id", "fecha", "hora_inicio", "hora_termino", "periodo", "soporte",
           "vip", 'var_mapa', 'valor_mapa', "start", "stream", "device", "minutes", "avg_minutes"]


def generar_fechas():
    replace_days = int(Variable.get("dias_remplazo"))

    fecha_termino = date.today()
    fecha_inicio = (fecha_termino + relativedelta(months=-
                    1, days=-replace_days)).replace(day=1)

    return fecha_inicio, fecha_termino


def convertir_fechas(fecha_inicio, fecha_termino):
    fecha_inicio_cl = cl.localize(datetime.combine(fecha_inicio, time.min))
    fecha_inicio_utc = fecha_inicio_cl.astimezone(utc).strftime(fmt)

    fecha_fin_cl = cl.localize(datetime.combine(
        fecha_termino, time.min)) + relativedelta(days=1)
    fecha_fin_utc = fecha_fin_cl.astimezone(utc).strftime(fmt)

    return [fecha_inicio_utc, fecha_fin_utc]


def generar_req(fechas: list, vip: int, emisor: bool, var: str) -> dict:
    """Función de generación de petición para la utilización de la API de Mediastream.

    Args:
        fechas (list): periodo de fechas según formato requerido por Mediastream
        agg (str): agregación por periodo (en este caso solo Mensual)
        vip (int): key en lista stream_vip
        emisor (bool): si corresponde o no a emisor
        var (str): dimension a consultar

    Returns:
        dict: diccionario de consulta
    """

    req = {}
    req["name"] = "full_by_time"
    req["dimension"] = ["content_live", var]

    order = 1
    req["filter"] = [{"name": "date", "op": [">=", "<="], "value":fechas}]
    if emisor:
        req["filter"].append({"name": "group", "value": [
                             "b3583f38d358a82ecb3b6784664f1308"], "logic_op": "and", "order": order})
        req["filter"].append({"name": "group", "value": [
                             "084012cca93d1e0c2ef6e0d162389a44"], "logic_op": "and", "order": order + 1})
        req["filter"].append({"name": "content_type", "value": [
                             "alive"], "logic_op": "and not", "order": order + 2})
        order = order + 3
    else:
        req["dimension"].append("content_id")
        req["filter"].append({"name": "content_type", "value": [
                             "aod"], "logic_op": "and not", "order": order})
        order = order + 1

    if vip > 0:
        req["filter"].append({"name": "group", "value": [
                             stream_vip[vip]], "logic_op": "and", "order": order})
        order = order + 1

    req["calendar"] = {"type": "all"}
    req["time"] = 0
    req["trunc"] = ["MONTH"]

    return req


def format_df(df_0: pd.DataFrame, vip: int, emisor: bool, var: str):
    df = df_0.copy()
    df["date_time"] = pd.to_datetime(df["date_time"]).dt.tz_localize(
        utc).dt.tz_convert(cl).dt.tz_localize(None)
    df["fecha"] = df["date_time"].dt.date
    df["hora_inicio"] = "00:00:00"
    df["hora_termino"] = "23:59:59"

    if emisor:
        df["soporte"] = "emisorpodcasting.cl"
    else:
        soportes = []
        for soporte, live_id in content_live.items():
            fechas = fechas_soportes[soporte]
            soportes.append([soporte, live_id, fechas[0], fechas[1]])

        soportes = pd.DataFrame(
            soportes, columns=["soporte", "live_id", "inicio_soporte", "fin_soporte"])

        df = pd.merge(df, soportes, "left",
                      left_on="content_id", right_on="live_id")

        df = df[(df["fecha"] >= df["inicio_soporte"]) &
                (df["fecha"] <= df["fin_soporte"])].copy()

    df["periodo"] = "mensual"
    df["vip"] = vip
    df["var_mapa"] = var

    df.rename(columns={'device_1': 'valor_mapa', "device_category": 'valor_mapa',
              'property': 'valor_mapa', 'country_name': 'valor_mapa'}, inplace=True)
    if 'radioboxnews,' in df["valor_mapa"].values:
        df["valor_mapa"] = df["valor_mapa"].map(
            {'radioboxnews,': 'radioboxnews'})

    df["id"] = df["soporte"].str[:3] + "_" + df["date_time"].dt.strftime(
        "%y%m") + "_" + df["periodo"].str[0] + "_v" + df["vip"].astype(str)
    df["var_map_str"] = df["valor_mapa"].str.replace(
        '[^\dA-Za-z]', '', regex=True).str.lower()
    df["id"] = df["id"] + "_" + df["var_mapa"] + "_" + df["var_map_str"]

    return df[~df["valor_mapa"].str.contains("=|;|\"", na=False)].copy()[columns]


def del_bq():
    fecha_inicio, fecha_termino = generar_fechas()

    client = bigquery.Client.from_service_account_json(_google_key)

    # Eliminación de los últimos días de datos
    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha >= '{1}'
        """.format(table_id, fecha_inicio))

    # Se imprimen logs para reconocer la ejecución
    print("Borrando datos desde: {0}".format(fecha_inicio))
    print(update_job_d.result())
    

def upload_bq(df_total):
    # Inicio de sesión de BigQuery con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

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
            bigquery.SchemaField("vip", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField(
                "var_mapa", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "valor_mapa", bigquery.enums.SqlTypeNames.STRING),
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
    job = client.load_table_from_dataframe(
        df_total, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete.


def descargar_data(**kwargs):
    var = kwargs["var"]

    fecha_inicio, fecha_termino = generar_fechas()
    fechas = convertir_fechas(fecha_inicio, fecha_termino)

    df_final = pd.DataFrame()

    for vip, emisor in list(itertools.product(stream_vip, v_emisor)):
        print((fecha_inicio, fecha_termino, var, vip, emisor))
        req = generar_req(fechas, vip, emisor, var)

        response = requests.post(endpoint, headers=headers, json=req)
        df = format_df(pd.DataFrame(
            response.json()["data"]), vip, emisor, var)

        df_final = pd.concat([df_final, df])

    upload_bq(df_final)


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
    "mapa_consumo",
    default_args=args,
    description="Actualización diaria de acumulados device y stream. Contenido Live y vip5.",
    schedule_interval="0 9 4 * *",
    catchup=False,
    start_date=datetime(2022, 11, 5, tzinfo=local_tz),
    tags=["mapa de consumo", "device", "stream", "live", "vip"],

# Se definen los pasos del dag según funciones anteriores
) as dag:
    del_cur = PythonOperator(
        task_id="eliminar_actual",
        python_callable=del_bq
    )

    device = PythonOperator(
        task_id="actualizar_device",
        python_callable=descargar_data,
        op_kwargs={'var': "device"}
    )

    device_category = PythonOperator(
        task_id="actualizar_device_category",
        python_callable=descargar_data,
        op_kwargs={'var': "device_category"}
    )

    property = PythonOperator(
        task_id="actualizar_property",
        python_callable=descargar_data,
        op_kwargs={'var': "property"}
    )

    country_name = PythonOperator(
        task_id="actualizar_country_name",
        python_callable=descargar_data,
        op_kwargs={'var': "country_name"}
    )

    del_cur >> [device, device_category, property, country_name]