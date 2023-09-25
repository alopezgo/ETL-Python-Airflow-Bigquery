""" Audiencias y Consumo - Consumo de Usuarios Registrados
Este script tiene como objetivo la extracción, transformación y almacenado
de los datos del consumo de usuarios registrados, y ser programadas para 
la extracción periódica en la plataforma Apache Airflow.
"""

#Importar librerías
from datetime import datetime, timedelta, time
from google.cloud import bigquery
import pandas as pd
import pytz
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Configuración de BigQuery. La ruta corresponde a la dirección del archivo
# de credenciales dentro del servidor de Airflow. 
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
bq_table = "conexion-datos-rdf.consumo_registrados_cia.consumo_registrados"

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
cl = pytz.timezone("America/Santiago")
local_tz = pendulum.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S'
dias_reemplazo = int(Variable.get("dias_remplazo"))
now_dt = datetime.now(cl)


def del_current() -> None:
    """Eliminación de datos para remplazo. Debido al método de consulta a la 
    API y la imposibilidad de realizar actualizaciones incrementales, se 
    eliminan los datos de los últimos días y se remplazan con los datos actualizados
    """
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key) 

    # datetime actual - dias de reemplazo, transformado a Date (YYYY-MM-DD)
    fi_periodo_d = (now_dt + timedelta(days= - dias_reemplazo)).date()
    # Atributo combine une date y time sin parametros (00:00:00)
    fi_periodo_dt = datetime.combine(fi_periodo_d, time())
    # Atributo strftime convierte datetime en string
    fi_periodo_str = fi_periodo_dt.strftime(fmt)
    
    # Se determina la fecha de inicio de la consulta para eliminar
    fecha_inicio_d = fi_periodo_str 
    # La fecha de inicio del mes se setea en el primer día del mes
    fecha_inicio_m = fi_periodo_dt.replace(day=1).strftime(fmt)

    # Se ejecuta consulta
    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE inicio_consumo >= '{1}'
        and periodo = 'diario'          
        """.format(bq_table, fecha_inicio_d))  
    
    print("Borrando datos periodo diario desde :"+ fecha_inicio_d) 

    print(update_job_d.result()) 
    
    # Se ejecuta consulta
    update_job_m = client.query(
        """
        DELETE FROM `{0}`
        WHERE inicio_consumo >= '{1}'
        AND periodo = 'mensual'
        """.format(bq_table, fecha_inicio_m)) 
    
    print("Borrando datos periodo mensual desde :"+ fecha_inicio_m) 

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
  
  # Variable toma 1 día menos que fi_periodo_dt y es transformado a zona horaria UTC 
  fi_consumo_dt = (fi_periodo_dt).astimezone(utc)
  # Atributo strftime convierte datetime en string
  fi_consumo_str = fi_consumo_dt.strftime(fmt)

  ff_consumo_d = (now_dt).date()
  ff_consumo_dt = datetime.combine(ff_consumo_d, time()).astimezone(utc)
  ff_consumo_str = ff_consumo_dt.strftime(fmt)

  fi_consumo_mes_dt = fi_periodo_dt.astimezone(utc)
  fi_consumo_mes_dt = fi_consumo_mes_dt.replace(day=1)
  fi_consumo_mes_str = fi_consumo_mes_dt.strftime(fmt)
  
  select= """
  WITH 
    consumo as (
    SELECT
    start_date,
    end_date,
    user_id,
    to_hex(md5(user_agent||request_ip)) as device_id,
    content_id,
    content_type,
    content_live,
    property,
    device_type,
    system,
    country_name as country,
    region_name as region
    FROM `conexion-datos-rdf.consumo.consumo_detalle`
    WHERE user_id IS NOT NULL
    and start_date between '{0}' and '{1}'),
  
    periodos as (
      SELECT
      hora_inicio as inicio_periodo,
      hora_fin as fin_periodo,
      periodo
      FROM `diccionarios.dicc_fechas`
      where periodo in ('{2}')
    )

  SELECT

    CASE    
    WHEN LOWER(consumo.content_live) = 'horizonte' THEN 'Círculo Horizonte'  
    WHEN LOWER(consumo.content_live) = 'oasis fm' THEN 'Mundo Mejor'    
    WHEN LOWER(consumo.content_live) = '13c radio' THEN 'Comunidad 13c Radio'    
    WHEN LOWER(consumo.content_live) = 'play fm' THEN 'Comunidad Play'    
    WHEN LOWER(consumo.content_live) = 'sonar' THEN 'Comunidad Sonar'    
    WHEN LOWER(consumo.content_live) = 'tele 13 radio' THEN 'La Bancada'
    WHEN LOWER(consumo.content_live) = '13c radio' THEN '13C Radio'    
    ELSE consumo.content_live    
    END AS federacion, 
    consumo.content_type,
    initcap(programas.programa) as programa, 
    string(null) as media,
    datetime(periodos.inicio_periodo) as inicio_consumo,
    datetime(periodos.fin_periodo) as fin_consumo,
    periodos.periodo,
    consumo.device_type,
    consumo.system,
    consumo.country,
    consumo.region,
    consumo.user_id,
    sum(DATETIME_DIFF(LEAST(datetime(consumo.end_date, 'America/Santiago'),programas.datetime_final),
          GREATEST(datetime(consumo.start_date, 'America/Santiago'), programas.datetime_inicio), SECOND)/60) AS minutos_consumo,
    count(distinct consumo.device_id) as q_devices

  FROM consumo
  JOIN `conexion-datos-rdf.diccionarios.dicc_programas_fechas` programas
  ON  consumo.content_id = programas.content_id
  AND datetime(consumo.start_date, 'America/Santiago') < programas.datetime_final
  AND datetime(consumo.end_date, 'America/Santiago') >= programas.datetime_inicio
  AND programas.activo is true
  JOIN periodos  
  ON programas.datetime_inicio < periodos.fin_periodo
  and programas.datetime_final >= periodos.inicio_periodo
  WHERE lower(consumo.content_type) != 'ondemand'

  GROUP BY
    federacion, consumo.content_type, programas.programa ,media, periodos.inicio_periodo,
    periodos.fin_periodo, periodos.periodo, consumo.device_type, consumo.system, consumo.country, 
    consumo.region, consumo.user_id
  
  UNION ALL

  SELECT
    CASE
    WHEN LOWER(consumo.property) in ('horizonte-player','horizonte-app-mobile', 'horizonte-web-app') THEN 'Círculo Horizonte'
    WHEN LOWER(consumo.property) = 'oasis-player' THEN 'Mundo Mejor'
    WHEN LOWER(consumo.property) in ('play-player', 'play-fm-app-web', 'playfm-app-mobile')THEN 'Comunidad Play'
    WHEN LOWER(consumo.property) in ('sonar-player','sonar-app-mobile', 'sonar-app-web') THEN 'Comunidad Sonar'
    WHEN LOWER(consumo.property) = 'tele13 radio-player' THEN 'La Bancada'
    WHEN LOWER(consumo.property)  = '13c radio player' THEN '13C Radio'
    END AS federacion,
    consumo.content_type,
    CASE
    WHEN pro.programa is not null then initcap(pro.programa)
    ELSE initcap(shows.title)
    END AS programa,
    string(null) as media, 
    datetime(periodos.inicio_periodo) as inicio_consumo,
    datetime(periodos.fin_periodo) as fin_consumo,
    periodos.periodo,
    consumo.device_type,
    consumo.system,
    consumo.country,
    consumo.region,
    consumo.user_id,
    sum(DATETIME_DIFF(consumo.end_date, consumo.start_date, SECOND)/60) as minutos_consumo,
    count(distinct consumo.device_id) as q_devices

  FROM consumo
    JOIN `conexion-datos-rdf.dicc_medios.dicc_medios`  medios
    ON medios.media_id = consumo.content_id
    AND lower(consumo.content_type) != 'live'
    AND lower(consumo.property) in ('horizonte-player', 'horizonte-app-mobile', 'horizonte-web-app',
    'oasis-player', 'play-player', 'play-fm-app-web', 'playfm-app-mobile','sonar-app-mobile', 'sonar-app-web',
    'sonar-player', 'tele13 radio-player', '13c radio player')
    JOIN`conexion-datos-rdf.dicc_medios.dicc_shows` shows
    ON medios.show_id = shows._id
    LEFT JOIN `conexion-datos-rdf.dicc_medios.dicc_programas` pro
    ON shows._id= pro.show_id
    JOIN periodos
    ON datetime(consumo.start_date, 'America/Santiago') < periodos.fin_periodo
    and datetime(consumo.end_date, 'America/Santiago') >= periodos.inicio_periodo

  GROUP BY 
    federacion, consumo.content_type, pro.programa, shows.title, media, periodos.inicio_periodo, periodos.fin_periodo,
    periodos.periodo, consumo.device_type, consumo.system, consumo.country, consumo.region, consumo.user_id
  
  UNION ALL

  SELECT
    CASE
    WHEN LOWER(consumo.property) = 'emisor-mobile-app ' THEN 'emisorpodcasting.cl'
    WHEN LOWER(consumo.property) = 'emisor-web-app' THEN 'emisorpodcasting.cl'
    ELSE consumo.property
    END AS federacion,
    consumo.content_type,
    initcap(shows.title) as programa,
    initcap(medios.title) as media, 
    datetime(periodos.inicio_periodo)  as inicio_consumo,
    datetime(periodos.fin_periodo) as fin_consumo,
    periodos.periodo,
    consumo.device_type,
    consumo.system,
    consumo.country,
    consumo.region,
    consumo.user_id,
    sum(DATETIME_DIFF(consumo.end_date, consumo.start_date, SECOND)/60) as minutos_consumo,
    count(distinct consumo.device_id) as q_devices

  FROM consumo
    JOIN `conexion-datos-rdf.dicc_medios.dicc_medios` medios
    ON medios.media_id = consumo.content_id
    and LOWER(consumo.content_type) != 'live'
    and lower(consumo.property) in ('emisorpodcasting.cl', 'emisor-web-app')
    JOIN `conexion-datos-rdf.dicc_medios.dicc_shows` shows
    ON medios.show_id = shows._id
    JOIN periodos
    ON  datetime(consumo.start_date, 'America/Santiago') < periodos.fin_periodo
    AND datetime(consumo.end_date, 'America/Santiago') >= periodos.inicio_periodo
  
  GROUP BY 
    federacion, content_type, shows.title, medios.title, periodos.inicio_periodo,
    periodos.fin_periodo, periodos.periodo, consumo.device_type, consumo.system,
    consumo.country, consumo.region, consumo.user_id
  """

  # Se define la sentencia según periodo de agrupación de datos

  periodo_mensual = 'mensual'
  periodo_diario = 'diario'

  if periodo == "dia":
      
    query= select.format(fi_consumo_str, ff_consumo_str, periodo_diario)

  elif periodo == "mes":
    query = select.format(fi_consumo_mes_str, ff_consumo_str, periodo_mensual)
  
  return query

def descargar_data(query:str) -> pd.DataFrame:
  """Descarga de datos desde API de Mediastream y conversión a formato adecuado

    Args:
        query (str): sentencia a consultar

    Returns:
        pandas.DataFrame: datos con formato
    """
    
  # Inicio de sesión con archivo de credenciales
  client = bigquery.Client.from_service_account_json(_google_key)
  
  #El resultado se convierte en un Dataframe
  df = client.query(query).result().to_dataframe(create_bqstorage_client=False)  
  return df

def upload_data_bq(df: pd.DataFrame) -> None:
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pandas.DataFrame): DataFrame con formato
    """
    client = bigquery.Client.from_service_account_json(_google_key) 

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField(
                "federacion", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "content_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "programa", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "media", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "inicio_consumo", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField(
                "fin_consumo", bigquery.enums.SqlTypeNames.DATETIME),
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
                "user_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "minutos_consumo", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField(
                "q_devices", bigquery.enums.SqlTypeNames.INTEGER)
        ],
        #write_disposition="WRITE_TRUNCATE",
        clustering_fields=["periodo", "content_type", "federacion"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="inicio_consumo",  # name of column to use for partitioning
        )
    )
    
    # Make an API request.
    job = client.load_table_from_dataframe(df, bq_table, job_config=job_config) 

    print(job.result())  # Wait for the job to complete.   


def etl_dia():
  query= gen_query("dia")
  
  df = descargar_data(query)
  
  upload_data_bq(df)

def etl_mes(): 
  query= gen_query("mes")
  
  df = descargar_data(query)
  
  upload_data_bq(df)

def flag_off():
    Variable.update(key="consumo_registrados",
                    value=False, serialize_json=True)


def flag_on():
    Variable.update(key="consumo_registrados",
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
    "consumo_registrados",
    default_args=args,
    description="Actualización diaria y mensual de consumo de usuarios registrados, por programas y shows",
    schedule_interval="30 6 * * *",
    catchup=False,
    start_date=datetime(2022, 7, 19, tzinfo=local_tz),
    tags=["live", "programas", "show"],
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
        task_id="actualizar_dia",
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