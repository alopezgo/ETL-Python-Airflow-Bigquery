import pytz
import pandas as pd
import pendulum
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import requests
import itertools
from google.cloud import bigquery
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
bq_table_show = "conexion-datos-rdf.audio_digital.funnel_show_property_old"
bq_table_total = "conexion-datos-rdf.audio_digital.funnel_liveod_property_old"

login_platform = {"username": "jlizana@rdfmedia.cl", "password": "jlizana123", "withJWT": "true", "login": "Login"}
session = requests.session()
r = session.post("https://platform.mediastre.am/login", data=login_platform)
r = session.get('https://platform.mediastre.am/analytics/now')
token = r.cookies.get_dict()["jwt"]
headers = {"X-API-Token": token}
endpoint = "https://metrics.mdstrm.com/outbound/v1/metric/api"

fechas_soportes = {
    "oasisfm.cl": [date(2019,8,1), date(2022,10,31)],
    "13cradio.cl": [date(2022,11,1), date.max],
    "playfm.cl": [date(2019,8,1), date.max],
    "sonarfm.cl": [date(2019,8,1), date.max],
    "tele13radio.cl": [date(2019,8,1), date.max],
    "horizonte.cl": [date(2019,8,1), date.max],
    "emisorpodcasting.cl": [date(2019,8,1), date.max]
}

grupos = {
    "emisorpodcasting.cl": "b3583f38d358a82ecb3b6784664f1308",
    "horizonte.cl": "3570670d5064d58ee8ccdd3650506e3a",
    "oasisfm.cl": "35bfeb595ed83ebec22c1e3b5ed28f7f",
    "13cradio.cl": "2ea634f2e3058deeb047e998ba69c0c0",
    "playfm.cl": "06c2d59cd238c5848572bc1874acb044",
    "sonarfm.cl": "2757a2c258842237e149c98e55073b31",
    "tele13radio.cl": "5ae0b4ae44d45dbbaef992ea99512079"}

content_live = {
    "horizonte.cl": "601415b58308405b0d11c82a",
    "oasisfm.cl": "5c915497c6fd7c085b29169d",
    "13cradio.cl": "5c915497c6fd7c085b29169d",
    "playfm.cl": "5c8d6406f98fbf269f57c82c",
    "sonarfm.cl": "5c915724519bce27671c4d15",
    "tele13radio.cl": "5c915613519bce27671c4caa"}

stream_vip = {
    0: None,
    1: "45afe3fdbe92500d3b72588549237cb3",
    5: "b3583f38d358a82ecb3b6783664f1305",
    20: "9e32ed53938d70972f115574cecc25d6",
    40: "e8bcb3395e6c37328e8cbb6424ebc376"}

cl = pytz.timezone("America/Santiago")
utc = pytz.utc
local_tz = pendulum.timezone("America/Santiago")
fmt = '%Y-%m-%dT%H:%M:%SZ'
now_dt = datetime.now(cl)

def del_current_data():

    replace_days = int(Variable.get("dias_remplazo"))
    f_inicio = (now_dt + timedelta(days=-replace_days)).date()
    f_inicio_consulta = (now_dt + timedelta(days=-replace_days)).date().strftime(fmt)
    fecha_inicio_m = (f_inicio).replace(day=1).strftime(fmt)
    client = bigquery.Client.from_service_account_json(_google_key)

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha_inicio >= '{1}'
        and periodo in ('hora', 'diario')
        """.format(bq_table_show, f_inicio_consulta))

    print(f'Borrando datos periodo hora y diario desde funnel property show: ' + f_inicio_consulta)
    print(update_job_d.result())

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha_inicio >= '{1}'
        and periodo in ('mensual')
        """.format(bq_table_show, fecha_inicio_m))

    print(f'Borrando datos periodo mensual desde funnel property show: ' + fecha_inicio_m)
    print(update_job_d.result())

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha_inicio >= '{1}'
        and periodo in ('hora', 'diario')
        """.format(bq_table_total, f_inicio_consulta))

    print(f'Borrando datos periodo hora y diario desde funnel property total: ' + f_inicio_consulta)
    print(update_job_d.result())

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha_inicio >= '{1}'
        and periodo in ('mensual')
        """.format(bq_table_total, fecha_inicio_m))

    print(f'Borrando datos periodo mensual desde funnel property total: ' + fecha_inicio_m)
    print(update_job_d.result())


def convertir_fechas(fechas: list) -> list:

    fecha_inicio_cl = cl.localize(datetime.strptime(fechas[0], "%Y-%m-%d"))
    fecha_inicio_utc = fecha_inicio_cl.astimezone(utc).strftime(fmt)
    
    fecha_fin_cl = cl.localize(datetime.strptime(
        fechas[1], "%Y-%m-%d")) + timedelta(seconds=-1, days=1)
    fecha_fin_utc = fecha_fin_cl.astimezone(utc).strftime(fmt)

    return [fecha_inicio_utc, fecha_fin_utc]

def generar_req(fechas: list, agg: str, marca: str, id_vip: str = None, group:bool = False, show = False) -> dict:
    properties = Variable.get("properties", deserialize_json=True)["properties"]

    req = {}
    req["name"] = "full_by_time"
    req["dimension"] = ["content_live"]

    if show : req["dimension"].append("show_id")

    if group: req["dimension"].append("property")

    # Filtros. Se debe incluír siempre el filtro de fechas, que se convierten previamente.
    # Se añade filtro del grupo de marca
    date_range = convertir_fechas(fechas)

    req["filter"] = [
        {"name": "date", "op": [">=", "<="], "value":date_range},
        {"name": "group", "value": [grupos[marca]], "logic_op": "and", "order": 1}
    ]

    if id_vip is not None:
        req["filter"].append(
            {"name": "group", "value": [id_vip], "logic_op": "and", "order": 2})

    logic_op = "and not"
    if not group:
        order = 3
        for property in properties:
            req["filter"].append(
                {"name": "property", "value": [property], "logic_op": logic_op, "order": order})
            order += 1

    req["calendar"] = {"type": "all"}
    req["time"] = "0"
    req["trunc"] = [agg]

    return req

def formatear_df(df, agg, soporte, vip, show = False):
    df["fecha_inicio"] = pd.to_datetime(df["date_time"]).dt.tz_localize(
        utc).dt.tz_convert(cl).dt.tz_localize(None)
    if agg == "HOUR":
        df["fecha_fin"] = df["fecha_inicio"].apply(lambda x: x + relativedelta(hours=1, seconds=-1))
        df["periodo"] = "hora"
    elif agg == "DAY":
        df["fecha_fin"] = df["fecha_inicio"].apply(lambda x: x + relativedelta(days=1, seconds=-1))
        df["periodo"] = "diario"
    elif agg == "MONTH":
        df["fecha_fin"] = df["fecha_inicio"].apply(lambda x: x + relativedelta(months=1, seconds=-1))
        df["periodo"] = "mensual"

    df["soporte"] = soporte
    df["vip"] = vip

    live_filter = {
        "horizonte.cl": "Horizonte",
        "oasisfm.cl": "Oasis FM",
        "13cradio.cl": "13c Radio",
        "playfm.cl": "Play FM",
        "sonarfm.cl": "Sonar",
        "tele13radio.cl": "Tele 13 Radio",
        "emisorpodcasting.cl": "Emisor Live"
    }

    df_2 = df[df["content_live"].isin([None, live_filter[soporte]])].copy()

    if show:
        df_2 = df_2[["fecha_inicio", "fecha_fin", "periodo", "soporte", "show_id", "property", "content_type", 
        "vip", "start", "stream", "device", "minutes", "avg_minutes"]].copy()
    else:
        df_2 = df_2[["fecha_inicio", "fecha_fin", "periodo", "soporte", "property", "content_type", 
        "vip", "start", "stream", "device", "minutes", "avg_minutes"]].copy()
    df_2 = df_2.sort_values(by=["fecha_inicio", "soporte", "property", "content_type"]).copy()
    
    return df_2

def descargar_data(soporte: str, show = False):
    properties = Variable.get("properties", deserialize_json=True)["properties"]

    df_total = pd.DataFrame()
    agregaciones = ["HOUR", "DAY", "MONTH"]
    filtros_grupo = [True, False]

    replace_days = int(Variable.get("dias_remplazo"))

    ts = date.today()
    end_time = (ts + timedelta(days=-1))
    start_time = (ts + timedelta(days=-replace_days))

    start_soporte = date.min
    end_soporte = date.max

    start_soporte = fechas_soportes[soporte][0]
    end_soporte = fechas_soportes[soporte][1]

    start_time = max(start_time, start_soporte)
    end_time = min(end_time, end_soporte)

    fechas = [start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")]

    print("Descargando datos: " + soporte)

    for agg, vip, filtro_grupo in list(itertools.product(agregaciones, stream_vip.keys(), filtros_grupo)):

        fechas = fechas.copy()
        
        if agg == "MONTH": 
            fechas[0] = datetime.strptime(fechas[0], "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")

        req = generar_req(fechas, agg, soporte, stream_vip[vip], filtro_grupo, show)

        for i in range(3):
            try:
                response = requests.post(endpoint, headers=headers, json=req)
            
                df = pd.DataFrame(response.json()["data"])
                if df.empty: break

                if not filtro_grupo: df["property"] = "Others"
                else: df = df[df["property"].isin(properties)]

                df_2 = formatear_df(df, agg, soporte, vip, show)
                df_total = pd.concat([df_total, df_2])
                
                print("{0}, {1}, {2}, vip {3}, {4}".format(fechas, agg, soporte, vip, filtro_grupo))

            except: 
                if i == 2: raise
                else: continue
            
            break

    df_total.sort_values(
        by=["fecha_inicio", "periodo", "soporte"], inplace=True)
    df_total.reset_index(drop=True, inplace=True)
    return df_total

def upload_bq(df: pd.DataFrame, show = False):

    client = bigquery.Client.from_service_account_json(_google_key) 

    tabla_final = bq_table_total
    schema = [
            bigquery.SchemaField("fecha_inicio", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("fecha_fin", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("periodo", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("soporte", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("property", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vip", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("start", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("stream", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("device", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("minutes", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("avg_minutes", bigquery.enums.SqlTypeNames.FLOAT)
        ]

    if show:
        tabla_final= bq_table_show
        schema = [
            bigquery.SchemaField("fecha_inicio", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("fecha_fin", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("periodo", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("soporte", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("show_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("property", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("vip", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("start", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("stream", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("device", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("minutes", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField("avg_minutes", bigquery.enums.SqlTypeNames.FLOAT)
        ]
    
    job_config = bigquery.LoadJobConfig(
        schema= schema,
        clustering_fields=["soporte", "vip", "property"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha_inicio",
        )
    )

    job = client.load_table_from_dataframe(df, tabla_final, job_config=job_config)
    job.result()
    

def actualizar_horizonte():
    df = descargar_data("horizonte.cl")
    upload_bq(df)

def actualizar_horizonte_show():
    df = descargar_data("horizonte.cl", True)
    upload_bq(df, True)

def actualizar_oasis():
    df = descargar_data("oasisfm.cl")
    upload_bq(df)

def actualizar_oasis_show():
    df = descargar_data("oasisfm.cl", True)
    upload_bq(df, True)

def actualizar_13cr():
    df = descargar_data("13cradio.cl")
    upload_bq(df)

def actualizar_13cr_show():
    df = descargar_data("13cradio.cl", True)
    upload_bq(df, True)

def actualizar_play():
    df = descargar_data("playfm.cl")
    upload_bq(df)

def actualizar_play_show():
    df = descargar_data("playfm.cl", True)
    upload_bq(df, True)

def actualizar_sonar():
    df = descargar_data("sonarfm.cl")
    upload_bq(df)

def actualizar_sonar_show():
    df = descargar_data("sonarfm.cl", True)
    upload_bq(df, True)

def actualizar_tele13():
    df = descargar_data("tele13radio.cl")
    upload_bq(df)

def actualizar_tele13_show():
    df = descargar_data("tele13radio.cl", True)
    upload_bq(df, True)

def actualizar_emisor():
    df = descargar_data("emisorpodcasting.cl")
    upload_bq(df)

def actualizar_emisor_show():
    df = descargar_data("emisorpodcasting.cl", True)
    upload_bq(df, True)

def flag_off():
    Variable.update(key="correo_audio_digital",
                    value=False, serialize_json=True)
def flag_on():
    Variable.update(key="correo_audio_digital",
                    value=True, serialize_json=True)

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['audiencias@rdfmedia.cl'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "funnel_property",
    default_args=args,
    description="Actualización diaria de los datos de funnel property por show y total marca.",
    schedule_interval="0 8 * * 1-5",
    catchup=False,
    start_date=datetime(2022, 10, 21, tzinfo=local_tz),
    tags=["audio", "platform", "diario", "property", "on demand"],
    
) as dag:

    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off)

    borrar = PythonOperator(
        task_id="del_remplazo",
        python_callable=del_current_data)

    hor = PythonOperator(
        task_id="actualizar_horizonte",
        python_callable=actualizar_horizonte)

    hor_show = PythonOperator(
        task_id="actualizar_horizonte_show",
        python_callable=actualizar_horizonte_show)

    trececr = PythonOperator(
        task_id="actualizar_13cr",
        python_callable=actualizar_13cr)
    
    trececr_show = PythonOperator(
        task_id="actualizar_13cr_show",
        python_callable=actualizar_13cr_show)

    pla = PythonOperator(
        task_id="actualizar_play",
        python_callable=actualizar_play)
    
    pla_show = PythonOperator(
        task_id="actualizar_play_show",
        python_callable=actualizar_play_show)

    son = PythonOperator(
        task_id="actualizar_sonar",
        python_callable=actualizar_sonar)
    
    son_show = PythonOperator(
        task_id="actualizar_sonar_show",
        python_callable=actualizar_sonar_show)

    t13 = PythonOperator(
        task_id="actualizar_tele13",
        python_callable=actualizar_tele13)
    
    t13_show = PythonOperator(
        task_id="actualizar_tele13_show",
        python_callable=actualizar_tele13_show)

    emi = PythonOperator(
        task_id="actualizar_emisor",
        python_callable=actualizar_emisor)
    
    emi_show = PythonOperator(
        task_id="actualizar_emisor_show",
        python_callable=actualizar_emisor_show)

    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on)

    marca_reset >> borrar >> [hor, trececr, pla, son, t13, emi]
    hor >> hor_show
    trececr >> trececr_show
    pla >> pla_show
    son >> son_show
    t13 >> t13_show
    emi >> emi_show
    [trececr_show, pla_show, son_show, t13_show, emi_show] >> marca_ok