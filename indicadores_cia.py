from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
import pandas as pd
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

table_registrados = "conexion-datos-rdf.indicadores_cia.indicadores_registrados"
table_total = "conexion-datos-rdf.indicadores_cia.indicadores_total"
table_30h = "conexion-datos-rdf.indicadores_cia.indicadores_30h"
table_live = "conexion-datos-rdf.indicadores_cia.indicadores_live"
table_od = "conexion-datos-rdf.indicadores_cia.indicadores_od"
table_device = "conexion-datos-rdf.indicadores_cia.indicadores_device"
table_bloques = "conexion-datos-rdf.indicadores_cia.indicadores_bloques"
table_30ih = "conexion-datos-rdf.indicadores_cia.indicadores_30ih"

_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"
local_tz = pendulum.timezone("America/Santiago")

def bq_client():
    return bigquery.Client.from_service_account_json(_google_key)

def fechas():
    today = date.today()

    fin_mes = today.replace(day=1) + timedelta(days=-1)
    inicio_mes = (today + relativedelta(months=-1)).replace(day=1)
    inicio_consumo = inicio_mes + timedelta (days= -2)
    fin_consumo = fin_mes

    return inicio_mes, fin_mes, inicio_consumo, fin_consumo

def dummy():
    print("Inicio")

def indicadores_registrados():
    client = bq_client()

    inicio_mes, fin_mes, inicio_consumo, fin_consumo = fechas()

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        """.format(table_registrados, fin_mes))

    print(update_job_d.result())

    query = """
    With

    registrado as (
    SELECT 
    platform_id,
    federation_id,
    email,
    cast(date(date_created) as string) as date_created,
    case when date_created < '2022-03-28'
    then date_diff('{0}', '2022-03-28', day) 
    else date_diff('{0}', date(date_created), day) 
    end as dias_registrado,
    case
    when
    date_diff('{0}', date(date_created), day) < 30
    then 'menos de 30 días creado'
    else  'más de 30 días creado'
    end as condicion
    from `webhooks.usuarios_unicos` as users
    where users.date_created < '{0}')

    Select * from registrado
    """.format(fin_mes)

    ind_registrados = client.query(query).result().to_dataframe(create_bqstorage_client=False)

    ind_registrados["fecha"] =  fin_mes
    ind_registrados = ind_registrados[["fecha", "federation_id", "platform_id", "email", "date_created", "dias_registrado", "condicion"]].copy()

    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["federation_id", "platform_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="fecha",
        ))
        
    # Make an API request.
    job = client.load_table_from_dataframe(ind_registrados, table_registrados, job_config=job_config)

    print(job.result())  # Wait for the job to complete.

def indicadores_total():
    client = bq_client()

    inicio_mes, fin_mes, inicio_consumo, fin_consumo = fechas()

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        """.format(table_total, fin_mes))

    print(update_job_d.result())

    query = """
    With

    registrado as (
    SELECT 
    platform_id,
    date_created,
    case when date_created < '2022-03-28'
    then date_diff('{0}', '2022-03-28', day) 
    else date_diff('{0}', date(date_created), day) 
    end as dias_registrado
    from `webhooks.usuarios_unicos` as users
    where users.date_created < '{0}'),

    consumo as (
    SELECT
    user_id,
    datetime(start_date, 'America/Santiago') as start_date,
    datetime(end_date,'America/Santiago') as end_date
    from `consumo.consumo_detalle`
    where user_id is not null
    and start_date between TIMESTAMP('2022-03-26', "America/Santiago") 
    and timestamp ('{1}', 'America/Santiago')),

    fechas_habiles as (

    --fechas sin feriados ni interferiados
    SELECT 
    fechas.hora_inicio,
    fechas.hora_fin,
    fechas.periodo,
    feriados.fecha,
    feriados.dia
    FROM `diccionarios.dicc_fechas` as fechas
    join `diccionarios.dicc_feriados_interferiados` feriados
    on fechas.hora_inicio = date(feriados.fecha)
    and fechas.periodo = 'diario'
    where feriados.feriado is false and feriados.interferiado is null
    and fechas.hora_inicio between '2022-03-28' and '{0}'),

    fechas as (
    select * from fechas_habiles
    where dia between 2 and 6) 

    Select
        registrado.platform_id,
        sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND)/60) as sum_total,
        count(distinct fechas.hora_inicio) as q_total,
        sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND)/ 60)/count(distinct fechas.hora_inicio) AS prom_total,
        case when registrado.dias_registrado > 0 then (count (distinct fechas.hora_inicio))/registrado.dias_registrado
        else 0
        end as freq_total
        from registrado 
        join consumo
        on registrado.platform_id = consumo.user_id
        join fechas
        on consumo.start_date < fechas.hora_fin
        and consumo.end_date >= fechas.hora_inicio
        group by registrado.platform_id, registrado.dias_registrado
        order by freq_total""". format(fin_mes, fin_consumo)

    ind_total = client.query(query).result().to_dataframe(create_bqstorage_client=False)

    ind_total["fecha"] =  fin_mes
    ind_total = ind_total[["fecha", "platform_id", "sum_total", "q_total", "prom_total", "freq_total"]].copy()

    job_config = bigquery.LoadJobConfig(
            schema=[],
            clustering_fields=["platform_id"],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field="fecha",
            )
    )
        
    # Make an API request.
    job = client.load_table_from_dataframe(ind_total, table_total, job_config=job_config)

    print(job.result())  # Wait for the job to complete.

def indicadores_30h():
    client = bq_client()

    inicio_mes, fin_mes, inicio_consumo, fin_consumo = fechas()

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        """.format(table_30h, fin_mes))
        
    print(update_job_d.result())

    query = """
    With
    registrado as (
    SELECT 
    platform_id,
    date_created,
    case when date_created < '2022-03-28'
    then date_diff('{0}', '2022-03-28', day) 
    else date_diff('{0}', date(date_created), day) 
    end as dias_registrado
    from `webhooks.usuarios_unicos` as users
    where users.date_created < '{0}'),

    consumo as (
    SELECT
    user_id,
    datetime(start_date, 'America/Santiago') as start_date,
    datetime(end_date,'America/Santiago') as end_date
    from `consumo.consumo_detalle`
    where user_id is not null
    and start_date between TIMESTAMP('{3}', "America/Santiago") 
    and timestamp ('{1}', 'America/Santiago')),

    fechas_habiles as (
    --fechas sin feriados ni interferiados
    SELECT 
    fechas.hora_inicio,
    fechas.hora_fin,
    fechas.periodo,
    feriados.fecha,
    feriados.dia
    FROM `diccionarios.dicc_fechas` as fechas
    join `diccionarios.dicc_feriados_interferiados` feriados
    on fechas.hora_inicio = date(feriados.fecha)
    and fechas.periodo = 'diario'
    where feriados.feriado is false and feriados.interferiado is null
    and fechas.hora_inicio between '{2}' and '{0}'),

    fechas as (
    select * from fechas_habiles
    where dia between 2 and 6)

    Select 
        registrado.platform_id,
        trunc(sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND)/ 60),1) AS sum_30h,
        count(distinct fechas.hora_inicio) as q_30h,
        sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND)/ 60)/count(distinct fechas.hora_inicio) as prom_30h,
        case when registrado.dias_registrado > 0 then (count (distinct fechas.hora_inicio))/registrado.dias_registrado
        else 0
        end as freq_30h
        -- date(EXTRACT(YEAR FROM fechas.hora_inicio), EXTRACT(MONTH FROM fechas.hora_inicio), 1) as fecha
        
        from registrado 
        join consumo
        on registrado.platform_id = consumo.user_id
        join fechas
        on consumo.start_date < fechas.hora_fin
        and consumo.end_date >= fechas.hora_inicio
        group by registrado.platform_id, registrado.dias_registrado
    """.format(fin_mes, fin_consumo, inicio_mes, inicio_consumo)

    ind_30h = client.query(query).result().to_dataframe(create_bqstorage_client=False)

    ind_30h["fecha"] =  fin_mes
    ind_30h = ind_30h[["fecha", "platform_id", "sum_30h", "q_30h", "prom_30h", "freq_30h"]]

    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["platform_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="fecha",
        )
    )
        
    # Make an API request.
    job = client.load_table_from_dataframe(ind_30h, table_30h, job_config=job_config)

    print(job.result())  # Wait for the job to complete.

def indicadores_live():
    client = bq_client()

    inicio_mes, fin_mes, inicio_consumo, fin_consumo = fechas()

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        """.format(table_live, fin_mes))
        
    print(update_job_d.result())

    query = """
    With
    registrado as (
    SELECT 
    platform_id,
    date_created,
    case when date_created < '2022-03-28'
    then date_diff('{0}', '2022-03-28', day) 
    else date_diff('{0}', date(date_created), day) 
    end as dias_registrado
    from `webhooks.usuarios_unicos` as users
    where users.date_created < '{0}'),

    consumo as (
    SELECT
    user_id,
    datetime(start_date, 'America/Santiago') as start_date,
    datetime(end_date,'America/Santiago') as end_date,
    content_type
    from `consumo.consumo_detalle`
    where user_id is not null
    and start_date between TIMESTAMP ('{3}', "America/Santiago") 
    and timestamp ('{1}', 'America/Santiago')),

    fechas_habiles as (
    --fechas sin feriados ni interferiados
    SELECT 
    fechas.hora_inicio,
    fechas.hora_fin,
    fechas.periodo,
    feriados.fecha,
    feriados.dia
    FROM `diccionarios.dicc_fechas` as fechas
    join `diccionarios.dicc_feriados_interferiados` feriados
    on fechas.hora_inicio = date(feriados.fecha)
    and fechas.periodo = 'diario'
    where feriados.feriado is false and feriados.interferiado is null
    and fechas.hora_inicio between '{2}' and '{0}'),

    fechas as (
    select * from fechas_habiles
    where dia between 2 and 6)

    SELECT 
    registrado.platform_id,
    consumo.content_type,
    trunc(sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND)/ 60),1) AS sum_live,
        count(distinct fechas.hora_inicio) as q_live,
        sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND)/ 60)/count(distinct fechas.hora_inicio) as prom_live,
        case when registrado.dias_registrado > 0 then (count (distinct fechas.hora_inicio))/registrado.dias_registrado
        else 0
        end as freq_live
    from registrado 
    join consumo
    on registrado.platform_id = consumo.user_id
    and content_type = 'Live'
    join fechas
    on consumo.start_date < fechas.hora_fin
    and consumo.end_date >= fechas.hora_inicio
    GROUP BY
    registrado.platform_id,
    consumo.content_type,
    registrado.dias_registrado""".format(fin_mes, fin_consumo, inicio_mes, inicio_consumo)

    ind_live= client.query(query).result().to_dataframe(create_bqstorage_client=False) 

    ind_live["fecha"] =  fin_mes
    ind_live = ind_live[["fecha", "platform_id", "content_type", "sum_live", "q_live", "prom_live", "freq_live"]]

    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["platform_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="fecha",
        )
    )
        
    # Make an API request.
    job = client.load_table_from_dataframe(ind_live, table_live, job_config=job_config)

    print(job.result())  # Wait for the job to complete.

def indicadores_od():
    client = bq_client()

    inicio_mes, fin_mes, inicio_consumo, fin_consumo = fechas()

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        """.format(table_od, fin_mes))
        
    print(update_job_d.result())

    query = """
    With
    registrado as (
    SELECT 
    platform_id,
    date_created,
    case when date_created < '2022-03-28'
    then date_diff('{0}', '2022-03-28', day) 
    else date_diff('{0}', date(date_created), day) 
    end as dias_registrado
    from `webhooks.usuarios_unicos` as users
    where users.date_created < '{0}'),

    consumo as (
    SELECT
    user_id,
    datetime(start_date, 'America/Santiago') as start_date,
    datetime(end_date,'America/Santiago') as end_date,
    content_type,
    device_type
    from `consumo.consumo_detalle`
    where user_id is not null
    and start_date between TIMESTAMP('{3}',  "America/Santiago") 
    and timestamp ('{1}', 'America/Santiago')),

    fechas_habiles as (
    --fechas sin feriados ni interferiados
    SELECT 
    fechas.hora_inicio,
    fechas.hora_fin,
    fechas.periodo,
    feriados.fecha,
    feriados.dia
    FROM `diccionarios.dicc_fechas` as fechas
    join `diccionarios.dicc_feriados_interferiados` feriados
    on fechas.hora_inicio = date(feriados.fecha)
    and fechas.periodo = 'diario'
    where feriados.feriado is false and feriados.interferiado is null
    and fechas.hora_inicio between '{2}' and '{0}'),

    fechas as (
    select * from fechas_habiles
    where dia between 2 and 6
    )

    SELECT 
    registrado.platform_id,
    trunc(sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND)/ 60),1) AS sum_od,
        count(distinct fechas.hora_inicio) as q_od,
        sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND)/ 60)/count(distinct fechas.hora_inicio) as prom_od,
        case when registrado.dias_registrado > 0 then (count (distinct fechas.hora_inicio))/registrado.dias_registrado
        else 0
        end as freq_od
    from registrado 
    join consumo
    on registrado.platform_id = consumo.user_id
    and content_type = 'Ondemand'
    join fechas
    on consumo.start_date < fechas.hora_fin
    and consumo.end_date >= fechas.hora_inicio
    GROUP BY 
    registrado.platform_id, registrado.dias_registrado
    """.format(fin_mes, fin_consumo, inicio_mes, inicio_consumo)

    ind_od= client.query(query).result().to_dataframe(create_bqstorage_client=False) 

    ind_od["fecha"] =  fin_mes
    ind_od = ind_od[["fecha", "platform_id", "sum_od", "q_od", "prom_od", "freq_od"]]

    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["platform_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="fecha",
        )
    )
        
    # Make an API request.
    job = client.load_table_from_dataframe(ind_od, table_od, job_config=job_config)

    print(job.result())  # Wait for the job to complete.

def indicadores_devices():
    client = bq_client()

    inicio_mes, fin_mes, inicio_consumo, fin_consumo = fechas()

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        """.format(table_device, fin_mes))
        
    print(update_job_d.result())

    query = """
    With

    registrado as (
    SELECT 
    platform_id,
    date_created,
    case when date_created < '2022-03-28'
    then date_diff('{0}', '2022-03-28', day) 
    else date_diff('{0}', date(date_created), day) 
    end as dias_registrado
    from `webhooks.usuarios_unicos` as users
    where users.date_created < '{0}'),

    consumo as (
    SELECT
    user_id,
    datetime(start_date, 'America/Santiago') as start_date,
    datetime(end_date,'America/Santiago') as end_date,
    case when device_type = 'Tablet'
        then 'Phone'
    when device_type = 'Other'
        then 'Phone'
        else device_type
        end as device_type
    from `consumo.consumo_detalle`
    where user_id is not null
    and start_date between TIMESTAMP('{3}', "America/Santiago") 
    and timestamp ('{1}', 'America/Santiago')),

    fechas_habiles as (
    --fechas sin feriados ni interferiados
    SELECT 
    fechas.hora_inicio,
    fechas.hora_fin,
    fechas.periodo,
    feriados.fecha,
    feriados.dia
    FROM `diccionarios.dicc_fechas` as fechas
    join `diccionarios.dicc_feriados_interferiados` feriados
    on fechas.hora_inicio = date(feriados.fecha)
    and fechas.periodo = 'diario'
    where feriados.feriado is false and feriados.interferiado is null
    and fechas.hora_inicio between '{2}' and '{0}'),

    fechas as (
    select * from fechas_habiles
    where dia between 2 and 6) 
    
    Select 
        registrado.platform_id,
        consumo.device_type,
        sum(cast(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
            GREATEST(consumo.start_date, fechas.hora_inicio), SECOND )/ 60 as float64)) AS sum_device,
        count(distinct fechas.fecha) as q_device,
        sum(cast(DATETIME_DIFF(LEAST(consumo.end_date,fechas.hora_fin),
        GREATEST(consumo.start_date, fechas.hora_inicio), SECOND )/ 60 as float64))/count(distinct fechas.fecha) as prom_device,
        case when registrado.dias_registrado > 0 then (count (distinct fechas.hora_inicio))/registrado.dias_registrado
        else 0
        end as freq_device

        from registrado 
        join consumo
        on registrado.platform_id = consumo.user_id
        join fechas
        on consumo.start_date < fechas.hora_fin
        and consumo.end_date >= fechas.hora_inicio
        group by 
        registrado.platform_id, consumo.device_type, registrado.dias_registrado
    """.format(fin_mes, fin_consumo, inicio_mes, inicio_consumo)

    ind_device = client.query(query).result().to_dataframe(create_bqstorage_client=False) 

    ind_device_desk = ind_device[ind_device['device_type']=='Desktop'].copy()
    ind_device_desk.rename(columns={'sum_device':'sum_desk', 'q_device': 'q_desk', 
    'prom_device': 'prom_desk', 'freq_device': 'freq_desk'},inplace = True)
    del ind_device_desk['device_type']
    ind_device_phone = ind_device[ind_device['device_type']=='Phone'].copy()
    ind_device_phone.rename(columns={'sum_device':'sum_phone', 'q_device': 'q_phone', 
    'prom_device': 'prom_phone', 'freq_device': 'freq_phone'},inplace = True)
    del ind_device_phone['device_type']

    ind_device_final = pd.merge(ind_device_desk, ind_device_phone, on= 'platform_id', how= 'left')

    ind_device_final["fecha"] =  fin_mes
    ind_device_final = ind_device_final[["fecha", "platform_id", "sum_desk", "q_desk", "prom_desk", "freq_desk", "sum_phone", "q_phone", "prom_phone", "freq_phone"]]

    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["platform_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="fecha",
        )
    )
        
    # Make an API request.
    job = client.load_table_from_dataframe(ind_device_final, table_device, job_config=job_config)

    print(job.result())  # Wait for the job to complete.

def indicadores_bloques():
    client = bq_client()

    inicio_mes, fin_mes, inicio_consumo, fin_consumo = fechas()

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        """.format(table_bloques, fin_mes))
        
    print(update_job_d.result())

    query= """
    WITH
    registrado as (
    SELECT 
    platform_id,
    date_created,
    case when date_created < '2022-03-28'
    then date_diff('{0}', '2022-03-28', day) 
    else date_diff('{0}', date(date_created), day) 
    end as dias_registrado
    from `webhooks.usuarios_unicos` as users
    where users.date_created < '{0}'),

    consumo as (
    SELECT
    user_id,
    datetime(start_date, 'America/Santiago') as start_date,
    datetime(end_date,'America/Santiago') as end_date
    from `consumo.consumo_detalle`
    where user_id is not null
    and start_date between TIMESTAMP('{3}', "America/Santiago") 
    and timestamp ('{1}', 'America/Santiago')),

    fechas_habiles as (

    --fechas sin feriados ni interferiados
    SELECT 
    bloques.bloque,
    datetime(date(feriados.fecha),cast(bloques.hora_inicio as time)) as dt_inicio,
    datetime(date(feriados.fecha), cast(bloques.hora_fin as time)) as dt_fin,
    feriados.dia
    FROM `diccionarios.dicc_feriados_interferiados` feriados
    cross join `diccionarios.dicc_bloque` bloques
    where feriados.feriado is false and feriados.interferiado is null
    and date(fecha) between '{2}' and '{0}'),

    fechas as (
    select * from fechas_habiles
    where dia between 2 and 6
    )  

    Select 
        registrado.platform_id,
        fechas.bloque,
        sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.dt_fin),
        GREATEST(consumo.start_date, fechas.dt_inicio), SECOND )/ 60) as sum_bloque,
        count(distinct fechas.dt_inicio) as q_bloque,
        sum(DATETIME_DIFF(LEAST(consumo.end_date,fechas.dt_fin),
        GREATEST(consumo.start_date, fechas.dt_inicio), SECOND )/ 60)/count(distinct fechas.dt_inicio) as prom_min,
        case when registrado.dias_registrado > 0 then (count (distinct fechas.dt_inicio))/registrado.dias_registrado
    else 0
    end as freq_bloque

        from registrado 
        join consumo
        on registrado.platform_id = consumo.user_id
        join fechas
        on consumo.start_date < fechas.dt_fin
        and consumo.end_date >= fechas.dt_inicio
        group by registrado.platform_id,fechas.bloque, registrado.dias_registrado""".format(fin_mes, fin_consumo, inicio_mes,
        inicio_consumo)

    ind_bloque = client.query(query).result().to_dataframe(create_bqstorage_client=False)

    ind_bloque_6a7 = ind_bloque[ind_bloque['bloque'] == '6 a 7'].copy()
    ind_bloque_6a7.rename(columns={'sum_bloque':'sum_6a7', 'q_bloque': 'q_6a7', 
    'prom_min': 'prom_6a7', 'freq_bloque': 'freq_6a7'},inplace = True)
    del ind_bloque_6a7['bloque']

    ind_bloque_7a9 = ind_bloque[ind_bloque['bloque'] == '7 a 9'].copy()
    ind_bloque_7a9.rename(columns={'sum_bloque':'sum_7a9', 'q_bloque': 'q_7a9', 
    'prom_min': 'prom_7a9', 'freq_bloque': 'freq_7a9'},inplace = True)
    del ind_bloque_7a9['bloque']

    ind_bloque_9a13 = ind_bloque[ind_bloque['bloque'] == '9 a 13'].copy()
    ind_bloque_9a13.rename(columns={'sum_bloque':'sum_9a13', 'q_bloque': 'q_9a13', 
    'prom_min': 'prom_9a13', 'freq_bloque': 'freq_9a13'},inplace = True)
    del ind_bloque_9a13['bloque']

    ind_bloque_13a16 = ind_bloque[ind_bloque['bloque'] == '13 a 16'].copy()
    ind_bloque_13a16.rename(columns={'sum_bloque':'sum_13a16', 'q_bloque': 'q_13a16', 
    'prom_min': 'prom_13a16', 'freq_bloque': 'freq_13a16'},inplace = True)
    del ind_bloque_13a16['bloque']

    ind_bloque_16a18 = ind_bloque[ind_bloque['bloque'] == '16 a 18'].copy()
    ind_bloque_16a18.rename(columns={'sum_bloque':'sum_16a18', 'q_bloque': 'q_16a18', 
    'prom_min': 'prom_16a18', 'freq_bloque': 'freq_16a18'},inplace = True)
    del ind_bloque_16a18['bloque']

    ind_bloque_18a21 = ind_bloque[ind_bloque['bloque'] == '18 a 21'].copy()
    ind_bloque_18a21.rename(columns={'sum_bloque':'sum_18a21', 'q_bloque': 'q_18a21', 
    'prom_min': 'prom_18a21', 'freq_bloque': 'freq_18a21'},inplace = True)
    del ind_bloque_18a21['bloque']

    ind_bloque_21a06 = ind_bloque[ind_bloque['bloque'] == '21 a 06'].copy()
    ind_bloque_21a06.rename(columns={'sum_bloque':'sum_21a06', 'q_bloque': 'q_21a06', 
    'prom_min': 'prom_21a06', 'freq_bloque': 'freq_21a06'},inplace = True)
    del ind_bloque_21a06['bloque']

    ind_bloque_total = pd.merge(ind_bloque_6a7, ind_bloque_7a9, on= 'platform_id', how= 'left')
    ind_bloque_total = pd.merge(ind_bloque_total, ind_bloque_9a13, on= 'platform_id', how= 'left')
    ind_bloque_total = pd.merge(ind_bloque_total, ind_bloque_13a16, on= 'platform_id', how= 'left')
    ind_bloque_total = pd.merge(ind_bloque_total, ind_bloque_16a18, on= 'platform_id', how= 'left')
    ind_bloque_total = pd.merge(ind_bloque_total, ind_bloque_18a21, on= 'platform_id', how= 'left')
    ind_bloque_total = pd.merge(ind_bloque_total, ind_bloque_21a06, on= 'platform_id', how= 'left')

    cols = list(ind_bloque_total.columns)
    ind_bloque_total["fecha"] =  fin_mes
    ind_bloque_total = ind_bloque_total[["fecha"] + cols]

    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["platform_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="fecha",
        )
    )
        
    # Make an API request.
    job = client.load_table_from_dataframe(ind_bloque_total, table_bloques, job_config=job_config)

    print(job.result())  # Wait for the job to complete.

def indicadores_30ih():
    client = bq_client()

    inicio_mes, fin_mes, inicio_consumo, fin_consumo = fechas()

    update_job_d = client.query(
        """
        DELETE FROM `{0}`
        WHERE fecha = '{1}'
        """.format(table_30ih, fin_mes))
        
    print(update_job_d.result())

    query = """
    WITH
    registrado as (
    SELECT 
    platform_id,
    date_created,
    case when date_created < '2022-03-28'
    then date_diff('{0}', '2022-03-28', day) 
    else date_diff('{0}', date(date_created), day) 
    end as dias_registrado
    from `webhooks.usuarios_unicos` as users
    where users.date_created < '{0}'),

    consumo as (
    SELECT
    user_id,
    datetime(start_date, 'America/Santiago') as start_date,
    datetime(end_date,'America/Santiago') as end_date
    from `consumo.consumo_detalle`
    where user_id is not null
    and start_date between TIMESTAMP('{3}', "America/Santiago") 
    and timestamp ('{1}', 'America/Santiago')),

    fechas_inhabiles as ( 
    SELECT 
    fechas.* ,
    feriados.fecha,
    feriados.dia
    FROM `diccionarios.dicc_fechas` as fechas
    join `diccionarios.dicc_feriados_interferiados` feriados
    on fechas.hora_inicio = date(feriados.fecha)
    and fechas.periodo = 'diario'
    and fechas.hora_inicio between '{2}' and '{0}'
    WHERE feriados.dia in (1,7)
    or feriados.feriado is true
    or feriados.interferiado is true
    ),

    nohabil as (
    Select * from fechas_inhabiles
    where periodo = 'diario'
    )
    -- /*Indicador total días de escucha en últimos 30 días inhábiles*/

    SELECT 
    registrado.platform_id,
        trunc(sum(DATETIME_DIFF(LEAST(consumo.end_date,nohabil.hora_fin),
        GREATEST(consumo.start_date, nohabil.hora_inicio), SECOND)/ 60),1) AS sum_30ih,
        count(distinct nohabil.hora_inicio) as q_30ih,
        sum(DATETIME_DIFF(LEAST(consumo.end_date,nohabil.hora_fin),
        GREATEST(consumo.start_date, nohabil.hora_inicio), SECOND)/ 60)/count(distinct nohabil.hora_inicio) as prom_30ih,
        case when registrado.dias_registrado > 0 then (count (distinct nohabil.hora_inicio))/registrado.dias_registrado
        else 0
        end as freq_30ih
    --date(EXTRACT(YEAR FROM nohabil.hora_inicio), EXTRACT(MONTH FROM nohabil.hora_inicio), 1) as fecha
    from registrado 
    join consumo
    on registrado.platform_id = consumo.user_id
    join nohabil
    on consumo.start_date < nohabil.hora_fin
    and consumo.end_date >= nohabil.hora_inicio
    GROUP BY 
    registrado.platform_id, registrado.dias_registrado
    """.format(fin_mes, fin_consumo, inicio_mes, inicio_consumo)

    ind_30ih = client.query(query).result().to_dataframe(create_bqstorage_client= False)

    ind_30ih["fecha"] =  fin_mes
    ind_30ih = ind_30ih[["fecha", "platform_id", "sum_30ih", "q_30ih", "prom_30ih", "freq_30ih"]]
    
    job_config = bigquery.LoadJobConfig(
        schema=[],
        clustering_fields=["platform_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="fecha",
        )
    )
        
    # Make an API request.
    job = client.load_table_from_dataframe(ind_30ih, table_30ih, job_config=job_config)

    print(job.result())  # Wait for the job to complete.

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
    "indicadores_cia",
    default_args=args,
    description="Actualización mensual de indicadores de consumo de usuarios CIA.",
    schedule_interval="0 9 4 * *",
    catchup=False,
    start_date=datetime(2022, 11, 1, tzinfo=local_tz),
    tags=["indicadores", "CIA"],

# Se definen los pasos del dag según funciones anteriores
) as dag:

    inicio = PythonOperator(
        task_id="inicio",
        python_callable=dummy
    )

    i_reg = PythonOperator(
        task_id="indicadores_registrados",
        python_callable=indicadores_registrados
    )

    i_total = PythonOperator(
        task_id="indicadores_total",
        python_callable=indicadores_total
    )

    i_30h = PythonOperator(
        task_id="indicadores_30h",
        python_callable=indicadores_30h
    )

    i_live = PythonOperator(
        task_id="indicadores_live",
        python_callable=indicadores_live
    )

    i_od = PythonOperator(
        task_id="indicadores_od",
        python_callable=indicadores_od
    )

    i_dev = PythonOperator(
        task_id="indicadores_devices",
        python_callable=indicadores_devices
    )

    i_bloq = PythonOperator(
        task_id="indicadores_bloques",
        python_callable=indicadores_bloques
    )
    
    i_30ih = PythonOperator(
        task_id="indicadores_30ih",
        python_callable=indicadores_30ih
    )

    # Configuración de la ejecución del DAG.
    inicio >> [i_reg, i_total, i_30h, i_live, i_od, i_dev, i_bloq, i_30ih]



