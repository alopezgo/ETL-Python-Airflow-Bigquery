#Importación de Librerías
import pandas as pd
from datetime import datetime, timedelta, time
from google.cloud import bigquery
import pytz
import pendulum

#Airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

#Transformaciones de fechas y horas necesarias
#La librería pytz es preferible a datetime porque 
# resuelve conflictos de datetimes provenientes de la máquina local en que se corre el proceso
# Pytz convierte cualquier datetime a un timezone deseado, tomando como referencia el UTC time
cl = pytz.timezone("America/Santiago")
utc = pytz.utc
local_tz = pendulum.timezone("America/Santiago")
now_dt = datetime.now(cl)
fmt = '%Y-%m-%d %H:%M:%S'
fmt_d = '%Y-%m-%d'
dias_reemplazo = int(Variable.get("dias_remplazo"))

# Inicio de sesión con archivo de credenciales
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
client = bigquery.Client.from_service_account_json(_google_key)
bq_table = "conexion-datos-rdf.consumo_registrados_cia.consumo_bloques"

def del_current() -> None:
    """
    Eliminación de datos para remplazo. Debido al método de consulta a la 
    API y la imposibilidad de realizar actualizaciones incrementales, se 
    eliminan los datos de los últimos días y se remplazan con los datos actualizados
    """
    
    # datetime actual - dias de reemplazo, transformado a Date (YYYY-MM-DD)
    fi_periodo_d = (now_dt + timedelta(days= - dias_reemplazo)).date()
    # combinamos el date con time() es decir en 00:00:00
    fi_periodo_dt = datetime.combine(fi_periodo_d, time())
    # Atributo strftime convierte datetime en string para usar en DELETE según periodo
    fi_dia = fi_periodo_d.strftime(fmt_d)
    fi_mes = fi_periodo_dt.replace(day=1).strftime(fmt_d)

    # Se ejecuta consulta DELETE periodo hora y diario
    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE inicio_periodo >= '{1}'
        and periodo in ('hora', 'diario')          
        """.format(bq_table, fi_dia))  
    
    print("Borrando datos periodo hora y diario desde :"+ fi_dia) 

    print(update_job_d.result()) 
    
    # Se ejecuta consulta DELETE periodo mensual
    update_job_m = client.query(
        """
        DELETE FROM `{0}`
        WHERE inicio_periodo >= '{1}'
        AND periodo = 'mensual'
        """.format(bq_table, fi_mes)) 
    
    print("Borrando datos periodo mensual desde :"+ fi_mes) 

    print(update_job_m.result())

def gen_query(periodo:str):
  """Generación automática de la sentencia a ejecutar en Google BigQuery

    Args:
        periodo (str): agrupación de datos por periodo

    Returns:
        query (str): sentencia sql 
    """

  # datetime actual - dias de reemplazo, transformado a Date (YYYY-MM-DD)
  fi_periodo_d = (now_dt + timedelta(days= - dias_reemplazo)).date()
  # combinamos el date con time() es decir en 00:00:00
  fi_periodo_dt = datetime.combine(fi_periodo_d, time())
  # Atributo strftime convierte datetime en string para usar en filtro query inicio_periodo
  fi_periodo_str = fi_periodo_d.strftime(fmt_d)

  fi_periodo_mes_str = fi_periodo_dt.replace(day=1).strftime(fmt_d)
    
  # Variable toma 1 día menos que fi_periodo_dt y es transformado a zona horaria UTC 
  fi_consumo_dt = (fi_periodo_dt).astimezone(utc)
  # Atributo strftime convierte datetime en string
  fi_consumo_str = fi_consumo_dt.strftime(fmt)

  ff_consumo_d = (now_dt).date()
  ff_consumo_dt = datetime.combine(ff_consumo_d, time()).astimezone(utc)

  #ff_consumo_str = ff_consumo_dt.strftime(fmt)

  fi_consumo_mes_dt = fi_periodo_dt.astimezone(utc)
  fi_consumo_mes_dt = fi_consumo_mes_dt.replace(day=1)
  fi_consumo_mes_str = fi_consumo_mes_dt.strftime(fmt)

  query = """
  WITH 
  consumo as (SELECT
  datetime(start_date, 'America/Santiago') as start_date, datetime(end_date, 'America/Santiago') as end_date, 
  user_id, to_hex(md5(request_ip||user_agent)) as device_id, content_id, content_type, device_type, system, 
  country_name as country, region_name as region, city_name as city, city_lat, city_lon, property
  FROM `conexion-datos-rdf.consumo.consumo_detalle` WHERE start_date >= '{0}'),

  periodos as (SELECT
  periodos.hora_inicio as inicio_periodo, periodos.hora_fin as fin_periodo, periodos.periodo
  FROM `diccionarios.dicc_fechas` as periodos WHERE hora_inicio >= '{1}'
  and periodos.periodo {2}),

  dt_programa as (SELECT programas.* 
  FROM `diccionarios.dicc_programas_fechas` as programas
  WHERE programas.activo is true and programas.datetime_inicio >= '{1}'),

  userradios as (SELECT platform_id, federation_name, email, date(safe_cast(birthday as TIMESTAMP)) as birthday, gender
  FROM `webhooks.usuarios_unicos`),

  useremisor as (
  SELECT platform_id, 'Emisor Podcasting' as federation_name, email, date(birthday) as birthday, gender
  FROM `usuarios.usuarios_emisor`),

  users as (
  SELECT * from userradios
  union all
  SELECT * from useremisor)

  SELECT
  userradios.federation_name as soporte, consumo.content_type, string(null) as bloque_programa, periodos.inicio_periodo,
  periodos.fin_periodo, periodos.periodo, consumo.device_type, consumo.system, consumo.country, consumo.region,
  consumo.city, consumo.city_lat, consumo.city_lon, consumo.user_id, userradios.email, userradios.birthday,
  userradios.gender, consumo.property, sum(trunc(cast(DATETIME_DIFF(LEAST(consumo.end_date,periodos.fin_periodo),
  GREATEST(consumo.start_date, periodos.inicio_periodo), SECOND )/ 60 as float64), 1)) AS minutos_consumo,
  consumo.device_id

  FROM consumo JOIN userradios ON consumo.user_id = userradios.platform_id
  JOIN periodos ON consumo.start_date < periodos.fin_periodo AND consumo.end_date >= periodos.inicio_periodo
  WHERE lower(consumo.content_type) != 'ondemand'

  GROUP BY soporte, consumo.content_type, periodos.inicio_periodo, periodos.fin_periodo,periodos.periodo, consumo.device_type, 
  consumo.system, consumo.country, consumo.region, consumo.city, consumo.city_lat, consumo.city_lon, consumo.user_id,userradios.email, consumo.device_id,consumo.property, userradios.birthday, userradios.gender

  UNION ALL

  SELECT 
  users.federation_name as soporte, consumo.content_type, string(null) as bloque, periodos.inicio_periodo, periodos.fin_periodo, 
  periodos.periodo, consumo.device_type, consumo.system, consumo.country, consumo.region, consumo.city, consumo.city_lat,
  consumo.city_lon, consumo.user_id, users.email, users.birthday, users.gender, consumo.property, sum(trunc(cast(DATETIME_DIFF(LEAST(end_date,periodos.fin_periodo),
  GREATEST(start_date, periodos.inicio_periodo), SECOND )/ 60 as float64), 1)) AS minutos_consumo, consumo.device_id

  FROM consumo JOIN users on consumo.user_id = users.platform_id
  JOIN periodos ON start_date < periodos.fin_periodo and end_date >= periodos.inicio_periodo
  WHERE lower(consumo.content_type) != 'live' 
  GROUP BY soporte, content_type, periodos.inicio_periodo, periodos.fin_periodo, periodos.periodo, consumo.device_type, 
  consumo.system, consumo.country, consumo.region, consumo.city, consumo.city_lat, consumo.city_lon, consumo.user_id, users.email, consumo.device_id, consumo.property, users.birthday, users.gender

  UNION ALL
  
  SELECT 
  users.federation_name as soporte, 'LiveOD' as content_type, string(null) as bloque, periodos.inicio_periodo, periodos.fin_periodo, 
  periodos.periodo, consumo.device_type, consumo.system, consumo.country, consumo.region, consumo.city, consumo.city_lat,
  consumo.city_lon, consumo.user_id, users.email, users.birthday, users.gender, consumo.property,sum(trunc(cast(DATETIME_DIFF(LEAST(end_date,periodos.fin_periodo),
  GREATEST(start_date, periodos.inicio_periodo), SECOND )/ 60 as float64), 1)) AS minutos_consumo, consumo.device_id

  FROM consumo JOIN users on consumo.user_id = users.platform_id
  JOIN periodos ON start_date < periodos.fin_periodo AND end_date >= periodos.inicio_periodo

  GROUP BY soporte, periodos.inicio_periodo, periodos.fin_periodo, periodos.periodo, consumo.device_type, 
  consumo.system, consumo.country, consumo.region, consumo.city, consumo.city_lat, consumo.city_lon, consumo.city, consumo.user_id, users.email, consumo.device_id, consumo.property, users.birthday, users.gender
    
  UNION ALL

  SELECT
  userradios.federation_name AS soporte, consumo.content_type,
  CASE WHEN pro.programa is not null then initcap(pro.programa) ELSE initcap(shows.title) END AS bloque, 
  periodos.inicio_periodo, periodos.fin_periodo, periodos.periodo, consumo.device_type, consumo.system,consumo.country, 
  consumo.region, consumo.city, consumo.city_lat, consumo.city_lon, consumo.user_id, userradios.email, userradios.birthday,
  userradios.gender, consumo.property, TRUNC(sum(cast(DATETIME_DIFF(LEAST(consumo.end_date, periodos.fin_periodo),
  GREATEST(consumo.start_date, periodos.inicio_periodo), SECOND) as float64)/60),1) AS minutos_consumo, consumo.device_id

  FROM consumo JOIN userradios ON consumo.user_id = userradios.platform_id
  JOIN `conexion-datos-rdf.dicc_medios.dicc_medios`  medios ON medios.media_id = consumo.content_id
  JOIN`conexion-datos-rdf.dicc_medios.dicc_shows` shows ON medios.show_id = shows._id
  LEFT JOIN `conexion-datos-rdf.dicc_medios.dicc_programas` pro ON shows._id= pro.show_id
  JOIN periodos ON consumo.start_date < periodos.fin_periodo AND consumo.end_date >= periodos.inicio_periodo
  WHERE lower(consumo.content_type) != 'live'
  
  GROUP BY 
  soporte, consumo.content_type, bloque, periodos.inicio_periodo, periodos.fin_periodo, periodos.periodo, 
  consumo.device_type, consumo.system, consumo.country, consumo.region, consumo.city, consumo.city_lat, consumo.city_lon, consumo.user_id,
  userradios.email, consumo.device_id, consumo.property, userradios.birthday, userradios.gender
    
  UNION ALL

  SELECT
  useremisor.federation_name AS soporte, consumo.content_type, initcap(shows.title) as bloque, periodos.inicio_periodo,
  periodos.fin_periodo, periodos.periodo, consumo.device_type, consumo.system, consumo.country, consumo.region,consumo.city, 
  consumo.city_lat, consumo.city_lon, consumo.user_id,useremisor.email, useremisor.birthday,useremisor.gender, consumo.property,
  TRUNC(sum(cast(DATETIME_DIFF(LEAST(consumo.end_date, periodos.fin_periodo), 
  GREATEST(consumo.start_date, periodos.inicio_periodo), SECOND) as float64)/60),1) AS minutos_consumo, consumo.device_id
  
  FROM consumo JOIN useremisor ON consumo.user_id = useremisor.platform_id
  JOIN `conexion-datos-rdf.dicc_medios.dicc_medios` medios ON medios.media_id = consumo.content_id
  JOIN `conexion-datos-rdf.dicc_medios.dicc_shows` shows ON medios.show_id = shows._id
  JOIN periodos ON consumo.start_date < periodos.fin_periodo AND consumo.end_date >= periodos.inicio_periodo
  WHERE LOWER(consumo.content_type) != 'live'
    
  GROUP BY soporte, content_type, bloque, periodos.inicio_periodo, periodos.fin_periodo, periodos.periodo, consumo.device_type, 
  consumo.system, consumo.country, consumo.region, consumo.city, consumo.city_lat, consumo.city_lon, consumo.user_id, 
  useremisor.email, consumo.device_id, consumo.property, useremisor.birthday, useremisor.gender
  {3}"""

  final_diario = """
  UNION ALL

  SELECT
  users.federation_name as soporte, consumo.content_type, initcap(dt_programa.programa) as bloque,  dt_programa.datetime_inicio as inicio_periodo,
  dt_programa.datetime_final as fin_periodo, 'diario' as periodo, consumo.device_type, consumo.system, consumo.country, consumo.region,
  consumo.city, consumo.city_lat, consumo.city_lon, consumo.user_id, users.email, users.birthday, users.gender, consumo.property,
  TRUNC(sum(DATETIME_DIFF(LEAST(consumo.end_date,dt_programa.datetime_final),
  GREATEST(consumo.start_date, dt_programa.datetime_inicio), second)/60),1) AS minutos_consumo, consumo.device_id

  FROM consumo JOIN users ON consumo.user_id = users.platform_id 
  JOIN dt_programa ON  consumo.content_id = dt_programa.content_id AND consumo.start_date < dt_programa.datetime_final
  AND consumo.end_date >= dt_programa.datetime_inicio
  WHERE users.federation_name != 'Emisor Podcasting'
      
  GROUP BY soporte, consumo.content_type, programa, dt_programa.datetime_inicio, dt_programa.datetime_final, 
  consumo. device_type, consumo.system, consumo.country, consumo.region, consumo.city, consumo.city_lat, consumo.city_lon, consumo.user_id, 
  users.email, consumo.property, consumo.device_id, users.birthday, users.gender
  """

  final_mensual = """;"""
  filtro_periodo_mes = 'in ("mensual")'
  filtro_periodo_dia = ' in ("hora", "diario")'


  if periodo == "MONTH":
    query = query.format(fi_consumo_mes_str, fi_periodo_mes_str, filtro_periodo_mes, final_mensual)
  
  elif periodo == "DAY" or periodo == "HOUR":
    query = query.format(fi_consumo_str, fi_periodo_str, filtro_periodo_dia, final_diario)

  return query

def descargar_data(query:str) -> pd.DataFrame:
  """Descarga de datos desde API de BigQuery y conversión a formato adecuado

    Args:
        query (str): sentencia a consultar

    Returns:
        pandas.DataFrame: datos con formato
    """
  
  #El resultado se convierte en un Dataframe
  df = client.query(query).result().to_dataframe(create_bqstorage_client=False)  
  return df

def upload_data_bq(df: pd.DataFrame) -> None:
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pandas.DataFrame): DataFrame con formato
    """

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                "soporte", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "bloque_programa", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "inicio_periodo", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField(
                "fin_periodo", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField(
                "periodo", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "device_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "system", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "country", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "region", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "city", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "city_lat", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField(
                "city_lon", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField(
                "user_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "email", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "birthday", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField(
                "gender", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "property", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "minutos_consumo", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField(
                "device_id", bigquery.enums.SqlTypeNames.STRING)
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="inicio_periodo",  # name of column to use for partitioning
        )
    )
    
    # Make an API request.
    job = client.load_table_from_dataframe(df, bq_table, job_config=job_config) 

    print(job.result())  # Wait for the job to complete.   

def etl_dia():
  query= gen_query("DAY")
  df = descargar_data(query)
  upload_data_bq(df)

def etl_mes(): 
  query= gen_query("MONTH")
  df = descargar_data(query)
  upload_data_bq(df)

def flag_off():
    Variable.update(key="consumo_bloques",
                    value=False, serialize_json=True)


def flag_on():
    Variable.update(key="consumo_bloques",
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
    "consumo_bloques_hora",
    default_args=args,
    description="Actualización periodos hora, diaria y mensual de consumo registrados por bloque programa, horario tipo Live, Ondemand y LiveOD",
    catchup=False,
    start_date=datetime(2022, 11, 22, tzinfo=local_tz),
    tags=["bloques", "content_type", "registrados"],
) as dag:

    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off
    )

    borrar = PythonOperator(
        task_id="del_remplazo",
        python_callable= del_current
    )

    descargar_dia = PythonOperator(
        task_id="actualizar_dia_hora",
        python_callable=etl_dia
    )

    descargar_mes = PythonOperator(
        task_id="actualizar_mes",
        python_callable=etl_mes
    )

    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on
    )

    marca_reset >> borrar >> [descargar_dia, descargar_mes] >> marca_ok