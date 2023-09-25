""" Audiencias y Consumo - Cantidad de registrados

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos de audiencias de audio digital según distintas agrupaciones, y ser
programadas para la extracción periódica en la plataforma Apache Airflow.

"""

#importar librerías
from datetime import datetime, timedelta, time
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
import pandas as pd
import pytz
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logica = ["NEW", "OLD"]
# tabla big query a utilizar
bq_table = ["conexion-datos-rdf.consumo_registrados_cia.q_registrados_new", "conexion-datos-rdf.consumo_registrados_cia.q_registrados_old" ]
#credenciales airflow
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"

#variables globales para conversión de fechas
cl = pytz.timezone("America/Santiago")
local_tz = pendulum.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%d %H:%M:%S'
fmtdate = '%Y-%m-%d'
dias_reemplazo = 4
# Variable now_dt convierte actual zona horario de Chile en datetime
now_dt = datetime.now(cl)

def del_current() -> None:

    #creamos objeto cliente BQ
    client = bigquery.Client.from_service_account_json(_google_key) 

    fi_periodo_d = (now_dt + timedelta(days= - dias_reemplazo + 1)).date()
    fi_periodo = fi_periodo_d.strftime(fmtdate)

    # Se crea trabajo BQ con sentencia SQL
    for table in bq_table:
        update_job_d = client.query(
            
            """
            DELETE FROM `{0}`
            WHERE inicio_consumo >= '{1}'      
            """.format(table, fi_periodo))  # Le pasamos la fi_periodo para eliminar datos desde ese día
        print("Borrando datos periodo diario desde :"+ fi_periodo) # Imprime dia inicial de borrado

        print(update_job_d.result()) # Imprime resultado del trabajo ejecutado en BQ

def gen_query(logica: str):
  
  # datetime actual - dias de reemplazo, transformado a Date (YYYY-MM-DD)
  fi_consumo_d = (now_dt + timedelta(days= - dias_reemplazo)).date()
  # combinamos el date con time() es decir en 00:00:00
  fi_consumo_dt = datetime.combine(fi_consumo_d, time()).astimezone(utc)
  # Atributo strftime convierte datetime en string para usar en filtro query inicio_periodo
  fi_consumo = fi_consumo_dt.strftime(fmt) 

  fi_periodo_d = (now_dt + timedelta(days= - dias_reemplazo + 1)).date()
  fi_periodo = fi_periodo_d.strftime(fmtdate)
  
  select= """

  with consumo as (
  Select
  content_id, 
  content_type,
  user_id,
  start_date,
  end_date
  from `conexion-datos-rdf.consumo.consumo_detalle`
  where user_id is not null
  and start_date >= '{0}')

    Select distinct
    programas.datetime_inicio as inicio_consumo, 
    programas.datetime_final as fin_consumo,
    initcap(programas.programa) as bloque_audio,
    'programa' as tipo_bloque,
    lives.soporte,
    consumo.content_type,
    'diario' as periodo,
    count(distinct (consumo.user_id)) users_{2},
    sum(trunc(cast(DATETIME_DIFF(LEAST(datetime(consumo.end_date, 'America/Santiago'),programas.datetime_final),
        GREATEST(datetime(consumo.start_date, 'America/Santiago'), programas.datetime_inicio), SECOND)/ 60 as float64), 1)) AS minutos_{2}

    from consumo
    join `conexion-datos-rdf.diccionarios.dicc_lives` lives
    on consumo.content_id = lives.live_id
    join `conexion-datos-rdf.diccionarios.dicc_programas_fechas` programas
    on consumo.content_id = programas.content_id
    and datetime(consumo.start_date, 'America/Santiago') < programas.datetime_final
    and datetime(consumo.{3}_date,  'America/Santiago') >= programas.datetime_inicio
    where programas.activo IS TRUE
    and lower(content_type) = 'live'
    and date(programas.datetime_inicio) >= '{1}'
    group by programas.datetime_inicio, programas.datetime_final, programas.programa, lives.soporte, consumo.content_type, periodo

    Union all 

    Select 
    fechas.hora_inicio as inicio_consumo,
    fechas.hora_fin as fin_consumo,
    extract (time from fechas.hora_inicio)||' - '||extract (time from fechas.hora_fin) as bloque_audio,
    'hora' as tipo_bloque,
    lives.soporte,
    consumo.content_type,
    'diario' as periodo,
    count(distinct consumo.user_id) as users_{2},
    sum(trunc(cast(DATETIME_DIFF(LEAST(datetime(consumo.end_date, 'America/Santiago'),fechas.hora_fin),
        GREATEST(datetime(consumo.start_date, 'America/Santiago'), fechas.hora_inicio), SECOND)/ 60 as float64), 1)) AS minutos_{2}

    from consumo
    join `conexion-datos-rdf.diccionarios.dicc_lives` lives
    on consumo.content_id = lives.live_id
    join `conexion-datos-rdf.diccionarios.dicc_fechas` fechas
    on datetime(consumo.start_date, 'America/Santiago') < fechas.hora_fin
    and datetime(consumo.{3}_date, 'America/Santiago') >= fechas.hora_inicio
    and fechas.periodo = 'hora'
    Where date(fechas.hora_inicio) >= '{1}'
    group by fechas.hora_inicio, fechas.hora_fin, bloque_audio, lives.soporte, consumo.content_type, periodo
    
    """

  new= "new"
  old= "old"
  start = "start"
  end = "end"

  if logica == "NEW":
      #Con la función .format pasamos variable fecha para filtrar campo start_date y extractos de la consulta
      # que varían según sea periodo diario o mensual 
    query= select.format(fi_consumo, fi_periodo, new, end)

  elif logica == "OLD":
    query= select.format(fi_consumo, fi_periodo, old, start)
  
  return query

def descargar_data(query:str) -> pd.DataFrame:
    """Descarga de datos desde BigQuery

    Args:
        query (str): Consulta SQL a ejecutar a BigQuery

    Returns:
        pandas.DataFrame: DataFrame con datos
    """
    
    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key)

    # Ejecución de consulta
    df = client.query(query).result().to_dataframe(create_bqstorage_client=False)  
    return df

def upload_data_bq(df: pd.DataFrame,table: str) -> None:
    """Carga de los datos formateados a Google BigQuery

    Args:
        df (pd.DataFrame): DataFrame con formato
        table (str): tabla de destino
    """

    # Inicio de sesión con archivo de credenciales
    client = bigquery.Client.from_service_account_json(_google_key) 
    
    # Se detecta el nombre de la tabla para asignar el esquema adecuado
    if table== bq_table[0]:
        # Configuración del trabajo. Debido a los tipos de datos involucrados, se debe 
        # declarar el esquema manualmente.
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(
                    "inicio_consumo", bigquery.enums.SqlTypeNames.DATETIME),
                bigquery.SchemaField(
                    "fin_consumo", bigquery.enums.SqlTypeNames.DATETIME),
                bigquery.SchemaField(
                    "bloque_audio", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "tipo_bloque", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "soporte", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "content_type", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "periodo", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "users_new", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField(
                    "minutos_new", bigquery.enums.SqlTypeNames.FLOAT)
            
            ],
            #write_disposition="WRITE_TRUNCATE",
            clustering_fields=["periodo", "soporte", "bloque_audio"],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="inicio_consumo",  # name of column to use for partitioning
            )
        )
    
    else:
        # Configuración del trabajo. Debido a los tipos de datos involucrados, se debe 
        # declarar el esquema manualmente.
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(
                    "inicio_consumo", bigquery.enums.SqlTypeNames.DATETIME),
                bigquery.SchemaField(
                    "fin_consumo", bigquery.enums.SqlTypeNames.DATETIME),
                bigquery.SchemaField(
                    "bloque_audio", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "tipo_bloque", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "soporte", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "content_type", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "periodo", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField(
                    "users_old", bigquery.enums.SqlTypeNames.INTEGER),
                bigquery.SchemaField(
                    "minutos_old", bigquery.enums.SqlTypeNames.FLOAT)
            ],
            #write_disposition="WRITE_TRUNCATE",
            clustering_fields=["periodo", "soporte", "bloque_audio"],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="inicio_consumo",  # name of column to use for partitioning
            )
        )
    
    # Se carga DataFrame a BigQuery y se espera el resultado
    job = client.load_table_from_dataframe(df, table, job_config=job_config) 
    print(job.result())

# Proceso ETL para cantidad de registrados. Se ejecutan las funciones descritas previamente para 
# generar la consulta, descargar los datos y cargarlos a BigQuery
def etl_qregistrados_new():
    query= gen_query(logica[0])
    df = descargar_data(query)
    upload_data_bq(df, bq_table[0])

def etl_qregistrados_old():
    query= gen_query(logica[1])
    df = descargar_data(query)
    upload_data_bq(df, bq_table[1])


# Actualización de variables de Airflow para seguimiento de ejecución
def flag_off():
    Variable.update(key="acumulados_device",
                    value=False, serialize_json=True)

def flag_on():
    Variable.update(key="acumulados_device",
                    value=True, serialize_json=True)

# Configuración del DAG de Airflow. Se configuran tiempos de ejecución e inicio del mismo
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
    "q_registrados_new",
    default_args=args,
    description="Actualización diaria cantidad de usuarios registrados que consumen audio digital live",
    schedule_interval="0 7 * * *",
    catchup=False,
    start_date=datetime(2022, 7, 7, tzinfo=local_tz),
    tags=["live", "new", "old", "registrados"],
    
# Se definen los pasos del dag según funciones anteriores
) as dag:
    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off
    )

    borrar = PythonOperator(
        task_id="del_remplazo",
        python_callable=del_current
    )

    actualizar_new = PythonOperator(
        task_id="actualizar_new",
        python_callable=etl_qregistrados_new
    )

    actualizar_old = PythonOperator(
        task_id="actualizar_old",
        python_callable=etl_qregistrados_old
    )

    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on
    )

    # Configuración de la ejecución del DAG.
    marca_reset >> borrar >> actualizar_new >> actualizar_old>> marca_ok