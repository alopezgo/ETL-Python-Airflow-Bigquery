""" Audiencias y Consumo - Programas Live

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos de audiencias de programas live y su contraparte on-demand según 
distintas agrupaciones, y ser programadas para la extracción periódica en la 
plataforma Apache Airflow.

"""

# Importación de librerías
import json
from datetime import date, datetime, timedelta, time

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
_programas_json = "/home/airflow/airflow/dags/dicc_programas.json"
bq_table_old = "conexion-datos-rdf.audio_digital.programas_old"
bq_table_new = "conexion-datos-rdf.audio_digital.programas_new"

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
cl = pytz.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'
fmt_d = '%Y-%m-%d'
fmt_dt = '%Y-%m-%d %H:%M:%S'

aggs = ["HOUR", "DAY", "MONTH"]


# Diccionarios útiles
fechas_soportes = {
    "Horizonte": [date(2019,8,1), date.max],
    "OasisFM": [date(2019,8,1), date(2022,10,31)],
    "13c Radio": [date(2022,11,1), date.max],
    "PlayFM": [date(2019,8,1), date.max],
    "Sonar": [date(2019,8,1), date.max],
    "T13Radio": [date(2019,8,1), date.max],
}


# ID de contenido de transmisión live
content_live = {
    # "OasisFM": "5c915497c6fd7c085b29169d",
    "13c Radio": "5c915497c6fd7c085b29169d",
    "PlayFM": "5c8d6406f98fbf269f57c82c",
    "Sonar": "5c915724519bce27671c4d15",
    "T13Radio": "5c915613519bce27671c4caa",
    "Horizonte": "601415b58308405b0d11c82a"
}

# ID de grupo de stream vip
stream_vip = {
    0: None,
    1: "45afe3fdbe92500d3b72588549237cb3",
    5: "b3583f38d358a82ecb3b6783664f1305",
    20: "9e32ed53938d70972f115574cecc25d6",
    40: "e8bcb3395e6c37328e8cbb6424ebc376"
}

dicc_replace = {
    "Horizonte": "horizonte.cl",
    # "Oasis FM": "oasisfm.cl",
    "13c Radio": "13cradio.cl",
    "Play FM": "playfm.cl",
    "Sonar": "sonarfm.cl",
    "Tele 13 Radio": "tele13radio.cl"
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


def horario_ipsos(horario_oficial: list) -> list:
    """Conversión del horario oficial del programa a horario IPSOS

    Args:
        horario_oficial (list): horario oficial (CL)

    Returns:
        list: horario IPSOS (CL)
    """
    # Se lleva la hora de inicio al comienzo del bloque horario (xx:00:00)
    inicio_oficial = datetime.strptime(horario_oficial[0], "%H:%M:%S")
    fin_oficial = datetime.strptime(
        horario_oficial[1], "%H:%M:%S") + timedelta(seconds=-1)

    # Se lleva la hora de término al fin del bloque horario (xx:59:59)
    inicio_ipsos = inicio_oficial.replace(minute=0).strftime("%H:%M:%S")
    fin_ipsos = (fin_oficial.replace(minute=59) +
                 timedelta(seconds=1)).strftime("%H:%M:%S")

    return [inicio_ipsos, fin_ipsos]


def generar_req(live: str, fechas: list, horas: list, dias: list, agg: str, vip: int) -> dict:
    """Generación automática del payload para consulta a API de Mediastream

    Args:
        live (str): id de transmisión live a consultar
        fechas (list): fechas de consulta
        horas (list): horario del programa (CL)
        dias (list): listado de días en que se emite el programa, comenzando en domingo
        agg (str): agregación temporal
        vip (int): grupo de consumo vip

    Returns:
        dict: payload con el formato para consulta
    """

    # Se generan el datetime de inicio y término de la consulta
    date_range = []
    # Para cada combinación de fecha y hora de inicio y término...
    for fecha, hora in zip(fechas, horas):
        # Se une la fecha de consulta con la hora del programa
        date_time = cl.localize(datetime.strptime(
            "{0} {1}".format(fecha, hora), "%Y-%m-%d %H:%M:%S"))
        # Se hace la conversión a formato UTC
        date_time_utc = date_time.astimezone(utc).strftime(fmt)

        date_range.append(date_time_utc)

    # Se corrige la hora de consulta final hasta el final del bloque
    date_range[1] = (datetime.strptime(date_range[1], fmt) +
                     timedelta(seconds=-1)).strftime(fmt)

    # El payload corresponde a un diccionario json con elementos obligatorios
    req = {}
    # Nombre de la consulta
    req["name"] = "full_by_time"
    # Dimensiones, en este caso sólo se consulta el contenido live/OD y el content_id
    req["dimension"] = ["content_live", "content_id"]

    # Filtros. Se debe incluír siempre el filtro de fechas, que se convierten previamente.
    # Se añaden filtros de trnsmisión live, y se elimina contenido on-demand
    req["day"] = {"type": dias}
    req["filter"] = [
        {"name": "date", "op": [">=", "<="], "value":date_range},
        {"name": "content_id", "value": [live], "logic_op": "and", "order": 0},
        {"name": "content_type", "value": [
            "aod"], "logic_op": "and not", "order": 1}
    ]
    
    # Se añade filtro de acuerdo al stream vip a consultar
    if vip:
        req["filter"].append(
            {"name": "group", "value": [stream_vip[vip]], "logic_op": "and", "order": 2})

    # req["calendar"] = {"type": "all"}
    req["time"] = "1"
    req["trunc"] = [agg]

    return req


def formatear_df(response: requests.Response, programa:str, def_horario:str, horario:list, agg:str, vip: int, feriados: dict) -> pd.DataFrame:
    """Función de formateo de respuesta desde API Mediastream a tabla en BigQuery

    Args:
        response (requests.Response): Respuesta desde API Mediastream
        programa (str): nombre de programa
        def_horario (str): tipo de bloque (oficial/ipsos)
        horario (list): horario del programa
        agg (str): agregación temporal
        vip (int): grupo de stream vip
        feriados (dict): diccionario de feriados

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
    df["feriado"] = df["date_time"].dt.strftime('%Y-%m-%d').replace(feriados)
    df.loc[(df["feriado"] != True), "feriado"] = False
    df["fecha"] = df["date_time"].dt.date
    df["hora_inicio_prog"] = datetime.strptime(horario[0], '%H:%M:%S').time()
    df["hora_termino_prog"] = (datetime.strptime(
        horario[1], '%H:%M:%S') + timedelta(seconds=-1)).time()
    df["hora_inicio_bloq"] = df["date_time"].dt.time

    # Se corrigen columnas con datos de hora de acuerdo a la agregación correspondiente
    if agg == "HOUR":
        df["hora_termino_bloq"] = (
            df["date_time"] + timedelta(hours=1, seconds=-1)).dt.time
    else:
        df["hora_termino_bloq"] = df["hora_termino_prog"]

    df["hora_inicio"] = df[["hora_inicio_prog",
                            "hora_inicio_bloq"]].max(axis=1).astype(str)
    df["hora_termino"] = df[["hora_termino_bloq",
                             "hora_termino_prog"]].min(axis=1).astype(str)

    # Se añaden columnas de programa y vip
    df["programa"] = programa.lower()
    df["vip"] = vip

    # Diccionario de traducción de soporte
    df["soporte"] = df["content_live"].replace(dicc_replace)

    # Se añade una columna con el tipo de agregación convertido
    periodo = ""
    if agg == "MONTH":
        periodo = "mensual"
    elif agg == "DAY":
        periodo = "diario"
    elif agg == "HOUR":
        periodo = "hora"
    df["periodo"] = periodo
    df.loc[(df["periodo"] == "mensual"), "feriado"] = False
    df["tipo_bloque"] = def_horario

    # Se filtran sólo las columnas a utilizar
    return df[["soporte", "programa", "tipo_bloque", "vip", "fecha", "feriado",
               "hora_inicio", "hora_termino", "periodo", "start", "stream", "device", "minutes", "avg_minutes"]]


def descargar_programas_old(vip: int) -> pd.DataFrame:
    """Descarga de datos desde API Mediastream

    Args:
        vip (int): grupo de stream vip

    Returns:
        pd.DataFrame: datos con formato para subir a BigQuery
    """

    # Se extrae la cantidad de días a remplazar de las variables de Airflow
    replace_days = int(Variable.get("dias_remplazo"))

    # Se abre el archivo de programas para obtener datos de horarios
    with open(_programas_json, encoding="UTF-8") as json_file:
        horarios_programas = json.load(json_file)

    # Inicio de Sesión de Mediastream
    headers, endpoint = sesion_platform()

    ts = date.today()

    # Se obtienen los feriados para considerarlos en la consulta
    header_feriados = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"}
    feriados_resp = requests.get("https://apis.digital.gob.cl/fl/feriados", headers=header_feriados)
    feriados = {}
    for feriado in feriados_resp.json(): feriados[feriado["fecha"]] = True

    # Por cada agregación...
    df_total = pd.DataFrame()
    for agg in aggs:

        # Por cada soporte...
        for marca, info_marca in horarios_programas.items():
            start_soporte = fechas_soportes[marca][0]
            end_soporte = fechas_soportes[marca][1]

            # Por cada programa del soporte
            for info_programa in info_marca["programas"].values():
                # Se obtienen las fechas de inicio y término y se corrige si es agregación mensual
                end_time = ts +  timedelta(days=-1)
                start_time = ts + timedelta(days=-replace_days)

                if agg == "MONTH":
                    start_time = (ts + timedelta(days=-replace_days)).replace(day=1)

                start_programa = date.min
                end_programa = date.max

                if "fecha_inicio" in info_programa:
                    start_programa = datetime.strptime(info_programa["fecha_inicio"], "%Y-%m-%d").date()
                if "fecha_fin" in info_programa:
                    end_programa = datetime.strptime(info_programa["fecha_fin"], "%Y-%m-%d").date()

                start_time = max(start_time, start_soporte, start_programa)
                end_time = min(end_time, end_soporte, end_programa)

                fechas = [start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")]

                if end_time < start_time: continue

                oficial = info_programa["horario"]
                ipsos = horario_ipsos(oficial)

                print(agg, vip, marca,
                      info_programa["nombre"], "oficial", fechas, oficial)
                
                req = generar_req(
                    info_marca["content_id"], fechas, oficial, info_programa["dias"], agg, vip)
                df_oficial = pd.DataFrame()

                tries = 3
                for i in range(tries):
                    try:
                        response = requests.post(endpoint, headers=headers, json=req)
                        df_oficial = formatear_df(
                            response, info_programa["nombre"], "oficial", oficial, agg, vip, feriados)
                    except KeyError as e:
                        if i < tries - 1: continue
                        else: raise
                    break

                df_ipsos = pd.DataFrame()
                if oficial == ipsos:
                    df_ipsos = df_oficial.copy()
                    df_ipsos["tipo_bloque"] = "ipsos"
                else:
                    print(agg, vip, marca,
                          info_programa["nombre"], "ipsos", fechas, ipsos)
                    req = generar_req(
                        info_marca["content_id"], fechas, ipsos, info_programa["dias"], agg, vip)

                    tries = 3
                    for i in range(tries):
                        try:
                            response = requests.post(
                                endpoint, headers=headers, json=req)
                            df_ipsos = formatear_df(
                                response, info_programa["nombre"], "ipsos", ipsos, agg, vip, feriados)
                        except KeyError as e:
                            if i < tries - 1: continue
                            else: raise
                        break

                df_programa = pd.concat([df_oficial, df_ipsos])

                df_total = pd.concat([df_total, df_programa])

    return df_total


def upload_bq_old(df):
    client = bigquery.Client.from_service_account_json(_google_key)

    job_config = bigquery.LoadJobConfig(
        schema=[],
        # write_disposition="WRITE_TRUNCATE",
        clustering_fields=["soporte", "vip", "tipo_bloque"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha",  # name of column to use for partitioning
        )
    )

    # Make an API request.
    job = client.load_table_from_dataframe(
        df, bq_table_old, job_config=job_config)
    job.result()  # Wait for the job to complete.


def gen_query(tipo_bloque: str, periodo: str):
    replace_days = int(Variable.get("dias_remplazo"))

    # creamos variable que nos trae el datetime actual en zona horaria de Chile
    now_dt = datetime.now(cl)
    f_inicio_d = (now_dt + timedelta(days=-replace_days)).date()
    f_inicio_dt = datetime.combine(f_inicio_d, time())
    fi_consumo_dt = f_inicio_dt.astimezone(utc)

    # fecha para filtrar query  periodo diario en datetime UTC
    fi_consumo_str = fi_consumo_dt.strftime(fmt_dt)

    f_inicio_m = (now_dt + timedelta(days=-replace_days)).date()
    f_inicio_m_01 = f_inicio_m.replace(day=1)
    f_inicio_m_01_dt = datetime.combine(f_inicio_m_01, time())
    f__inicio_m_01_dt_utc = f_inicio_m_01_dt.astimezone(utc)

    f_inicio_m_str = f__inicio_m_01_dt_utc.strftime(fmt_dt)

    mensual = """
    SELECT soporte, programa, '{0}' as tipo_bloque, vip, date(fechas.hora_inicio) as fecha, time(dnews.hora_inicio) as hora_inicio, 
time(dnews.hora_termino) as hora_termino, fechas.periodo, count(distinct devices) as device_new
from dnews
join `conexion-datos-rdf.diccionarios.dicc_fechas` fechas
ON EXTRACT(month FROM dnews.hora_inicio) = EXTRACT(month FROM fechas.hora_inicio)
AND EXTRACT(year FROM dnews.hora_inicio) = EXTRACT(year FROM fechas.hora_fin)
AND fechas.periodo= 'mensual'

group by soporte, programa, vip, date(fechas.hora_inicio), time(dnews.hora_inicio), time(dnews.hora_termino), fechas.periodo

    """

    diario = """
    SELECT soporte, programa, '{0}' as tipo_bloque, vip, date(dnews.hora_inicio) as fecha, time(dnews.hora_inicio) as hora_inicio, 
time(dnews.hora_termino) as hora_termino, fechas.periodo, count(distinct devices) as device_new
from dnews
join `conexion-datos-rdf.diccionarios.dicc_fechas` fechas
ON EXTRACT(month FROM dnews.hora_inicio) = EXTRACT(month FROM fechas.hora_inicio)
AND EXTRACT(year FROM dnews.hora_inicio) = EXTRACT(year FROM fechas.hora_fin)
AND EXTRACT(day FROM dnews.hora_inicio) = EXTRACT(day FROM fechas.hora_fin)
AND fechas.periodo= 'diario'

group by soporte, programa, vip, date(dnews.hora_inicio), time(dnews.hora_inicio) , time(dnews.hora_termino), fechas.periodo
"""

    select = """
    with dnews as (
 SELECT
  dicc.soporte,
  dicc.programa,
  vips.vip,
  date({1}) as fecha,
  {1} as hora_inicio,
  datetime_sub({2}, interval 1 second) as hora_termino,
  con.request_ip||con.user_agent as devices

  FROM  `conexion-datos-rdf.consumo.consumo_detalle`  as con 
  join `conexion-datos-rdf.diccionarios.dicc_lives` as lives
  on con.content_id= lives.live_id
  join `conexion-datos-rdf.diccionarios.dicc_vips` vips
  on ifnull(con.minutes, 0) >= vips.vip
  join `conexion-datos-rdf.diccionarios.dicc_programas_fechas` as dicc
  ON  con.content_id = dicc.content_id
    AND DATETIME(TIMESTAMP(con.start_date), 'America/Santiago') < {2}
    AND DATETIME(TIMESTAMP(con.end_date), 'America/Santiago') >= {1}
  and con.start_date >= '{0}'
  and dicc.activo is true
)
{3}"""

    dt_inicio_oficial = 'dicc.datetime_inicio'
    dt_inicio_ipsos = 'dicc.datetime_inicio_ipsos'
    dt_final_oficial = 'dicc.datetime_final'
    dt_final_ipsos = 'dicc.datetime_final_ipsos'
    bloque_oficial = 'oficial'
    bloque_ipsos = 'ipsos'

    if tipo_bloque == 'oficial' and periodo == 'diario':
        diario = diario.format(bloque_oficial)
        query = select.format(
            fi_consumo_str, dt_inicio_oficial, dt_final_oficial, diario)
    elif tipo_bloque == 'ipsos' and periodo == 'diario':
        diario = diario.format(bloque_ipsos)
        query = select.format(
            fi_consumo_str, dt_inicio_ipsos, dt_final_ipsos, diario)
    elif tipo_bloque == 'oficial' and periodo == 'mensual':
        mensual = mensual.format(bloque_oficial)
        query = select.format(
            f_inicio_m_str, dt_inicio_oficial, dt_final_oficial, mensual)
    elif tipo_bloque == 'ipsos' and periodo == 'mensual':
        mensual = mensual.format(bloque_ipsos)
        query = select.format(
            f_inicio_m_str, dt_inicio_ipsos, dt_final_ipsos, mensual)

    return query


def descargar_programas_new(query: str) -> pd.DataFrame:
    client = bigquery.Client.from_service_account_json(_google_key)

    df = client.query(query).result().to_dataframe(
        create_bqstorage_client=False)
    return df


def upload_bq_new(df: pd.DataFrame) -> None:
    client = bigquery.Client.from_service_account_json(_google_key)

    job_config = bigquery.LoadJobConfig(
        schema=[],
        # write_disposition="WRITE_TRUNCATE",
        clustering_fields=["periodo", "soporte", "tipo_bloque"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fecha",  # name of column to use for partitioning
        )
    )

    # Make an API request.
    job = client.load_table_from_dataframe(
        df, bq_table_new, job_config=job_config)

    print(job.result())  # Wait for the job to complete.


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
        DELETE FROM `{0}`
        WHERE fecha >= "{1}"
        """.format(bq_table_old, fecha_inicio))
    print(f'Borrando old diario/hora desde: ' + fecha_inicio)

    print(update_job.result())

    update_job = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha >= "{1}"
        AND CONTAINS_SUBSTR(periodo, "mensual")
        """.format(bq_table_old, fecha_inicio_m))
    print(f'Borrando old mensual desde: ' + fecha_inicio_m)

    print(update_job.result())

    update_job = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha >= '{1}'
        and periodo = 'diario'
        """.format(bq_table_new, fecha_inicio))  # elimina datos de la tabla por periodo "dia"
    print(f'Borrando new diario desde: ' + fecha_inicio)
    print(update_job.result())

    update_job = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        and periodo = 'mensual'
        """.format(bq_table_new, fecha_inicio_m))  # elimina datos de la tabla por periodo "dia"
    print(f'Borrando new mensual desde: ' + fecha_inicio_m)
    print(update_job.result())


def descargar_programas_v0():
    df = descargar_programas_old(0)

    upload_bq_old(df)


def descargar_programas_v1():
    df = descargar_programas_old(1)

    upload_bq_old(df)


def descargar_programas_v5():
    df = descargar_programas_old(5)

    upload_bq_old(df)


def descargar_programas_v20():
    df = descargar_programas_old(20)

    upload_bq_old(df)


def descargar_programas_v40():
    df = descargar_programas_old(40)

    upload_bq_old(df)


def dummy():
    print("dummy")


def descargar_programas_new_m_of():
    query = gen_query('oficial', "mensual")

    df = descargar_programas_new(query)

    upload_bq_new(df)


def descargar_programas_new_d_of():
    query = gen_query('oficial', "diario")

    df = descargar_programas_new(query)

    upload_bq_new(df)


def descargar_programas_new_m_ip():
    query = gen_query('ipsos', 'mensual')

    df = descargar_programas_new(query)

    upload_bq_new(df)


def descargar_programas_new_d_ip():
    query = gen_query('ipsos', 'diario')

    df = descargar_programas_new(query)

    upload_bq_new(df)


def flag_off():
    Variable.update(key="correo_programas", value=False, serialize_json=True)


def flag_on():
    Variable.update(key="correo_programas", value=True, serialize_json=True)

# endregion


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

local_tz = pendulum.timezone("America/Santiago")

with DAG(
    "programas_live",
    default_args=args,
    description="Actualización diaria de datos de programas en vivo",
    schedule_interval='30 6 * * *',
    catchup=False,
    start_date=datetime(2022, 4, 21, tzinfo=local_tz),
    tags=["audio", "mediastream",  "programas", "live", "diario", "new"],
) as dag:

    limpiar = PythonOperator(
        task_id="limpiar_tabla",
        python_callable=del_curr
    )

    v0 = PythonOperator(
        task_id="descargar_v0",
        python_callable=descargar_programas_v0
    )

    v1 = PythonOperator(
        task_id="descargar_v1",
        python_callable=descargar_programas_v1
    )

    v5 = PythonOperator(
        task_id="descargar_v5",
        python_callable=descargar_programas_v5
    )

    v20 = PythonOperator(
        task_id="descargar_v20",
        python_callable=descargar_programas_v20
    )

    v40 = PythonOperator(
        task_id="descargar_v40",
        python_callable=descargar_programas_v40
    )

    dum = PythonOperator(
        task_id="dummy",
        python_callable=dummy
    )

    new_m_of = PythonOperator(
        task_id="new_oficial_mensual",
        python_callable=descargar_programas_new_m_of
    )

    new_m_ip = PythonOperator(
        task_id="new_ipsos_mensual",
        python_callable=descargar_programas_new_m_ip
    )

    new_d_of = PythonOperator(
        task_id="new_oficial_dia",
        python_callable=descargar_programas_new_d_of
    )

    new_d_ip = PythonOperator(
        task_id="new_ipsos_dia",
        python_callable=descargar_programas_new_d_ip
    )

    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off
    )

    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on
    )

    marca_reset >> limpiar >> [v0, v1, v5, v20, v40] >> dum >> [
        new_m_of, new_d_of, new_m_ip, new_d_ip] >> marca_ok
