""" Audiencias y Consumo - Detalle de Consumo

Este script tiene como objetivo la extracción, transformación y almacenado
de los datos del detalle de consumo de audio digital por playback, y ser
programadas para la extracción periódica en la plataforma Apache Airflow.

"""

import itertools
import os
from datetime import date, datetime, timedelta

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

# Configuración de BigQuery. La ruta corresponde a la dirección del archivo
# de credenciales dentro del servidor de Airflow.
_google_key_bq = "/home/airflow/airflow/dags/servacc_bigquery.json"
_google_key_ga = "/home/airflow/airflow/dags/servacc_trafico.json"

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
local_tz = pendulum.timezone("America/Santiago")

# Diccionario auxiliares
agregaciones = ["mensual", "diario", "hora", "mes hora"]
marcas = ["emisor", "horizonte", "oasis",
          "13cradio", "play", "sonar", "tele13"]


cols_final = ['id', 'fecha', 'hora_inicio', 'hora_termino', 'periodo', 'tipo',
              'soporte', 'fuente_dato', 'target', 'target_tableau', 'porc_rebote', 'sesiones', 'pag_vistas',
              'tprom_pagina', 'ttot_1erplano', 'usuarios_unicos', 'usuarios_nuevos']

# region IDs y variables para consultas

# Id Analytics
ana_view_ids = {
    "emisor": "191886029",
    "horizonte": "238364456",
    "oasis": "105301076",
    "play": "105301836",
    "sonar": "105302317",
    "tele13": "128891529"
}

ga4_prop_ids = {
    "13cradio": "338717019"
}

# ID Firebase
fire_prop_ids = {
    "emisor": "196459353",
    "oasis": "200861130",
    "play": "202209153",
    "sonar": "200665400",
    "tele13": "199662917"
}

# ID Firebase
fire_new_prop_ids = {
    "sonar": "317974679",
    "horizonte": "328818171",
    "play": "340404217"
}


# Métricas Analytics
ana_metricas = {
    "ga:users": "usuarios_unicos",
    "ga:newUsers": "usuarios_nuevos",
    "ga:sessions": "sesiones",
    "ga:pageviews": "pag_vistas",
    "ga:bounceRate": "porc_rebote",
    "ga:avgTimeOnPage": "tprom_pagina"
}

# Métricas Firebase y GA4
ga4_metricas = {
    "totalUsers": "usuarios_unicos",
    "newUsers": "usuarios_nuevos",
    "sessions": "sesiones",
    "screenPageViews": "pag_vistas",
    "userEngagementDuration": "ttot_1erplano"
}

# Dimensiones demográficas Analytics
ana_dim_demogr = {
    "total": {},
    "genero": {"ga:userGender": "genero"},
    "edad": {"ga:userAgeBracket": "edad"},
    "generoedad": {"ga:userGender": "genero", "ga:userAgeBracket": "edad"}
}

# Dimensiones demográficas Firebase y GA4
ga4_dim_demogr = {
    "total": {},
    "genero": {"userGender": "genero"},
    "edad": {"userAgeBracket": "edad"},
    "generoedad": {"userGender": "genero", "userAgeBracket": "edad"}
}

# Dimensiones tiempo Analytics
ana_dim_tiempo = {
    "mensual": {"ga:yearMonth": "mes"},
    "diario": {"ga:date": "dia"},
    "hora": {"ga:dateHour": "hora"},
    "mes hora": {"ga:yearMonth": "mes", "ga:hour": "hora"}
}

# Dimensiones tiempo GA4 y Firebase
ga4_dim_tiempo = {
    "mensual": {"year": "año", "month": "mes"},
    "diario": {"date": "dia"},
    "hora": {"dateHour": "hora"},
    "mes hora": {"year": "año", "month": "mes", "hour": "hora"}
}

# endregion

# region Valores columnas demográficas

# Diccionario valores demográficos
t_value = ["value", "('value', 'female')", "('value', 'male')",
           "('value', '18-24')", "('value', '25-34')", "('value', '35-44')",
           "('value', '45-54')", "('value', '55-64')", "('value', '65+')",
           "('value', 'female', '18-24')", "('value', 'female', '25-34')",
           "('value', 'female', '35-44')", "('value', 'female', '45-54')",
           "('value', 'female', '55-64')", "('value', 'female', '65+')",
           "('value', 'male', '18-24')", "('value', 'male', '25-34')",
           "('value', 'male', '35-44')", "('value', 'male', '45-54')",
           "('value', 'male', '55-64')", "('value', 'male', '65+')"]

cols_form = ["datetime", "variable"] + t_value

# Diccionario demográficos Google
t_text = ['Total', 'Mujeres', 'Hombres', '18-24', '25-34', '35-44', '45-54', '55-64', '65+',
          'Mujeres 18-24', 'Mujeres 25-34', 'Mujeres 35-44', 'Mujeres 45-54', 'Mujeres 55-64', 'Mujeres 65+',
          'Hombres 18-24', 'Hombres 25-34', 'Hombres 35-44', 'Hombres 45-54', 'Hombres 55-64', 'Hombres 65+']

# Diccionario demográficos Tableau
t_tableau = ['Tot. Pob.', "m", "h", "1824", "2534", "3544", "4554",  "5564", "65",
             "m 1824", "m 2534", "m 3544", "m 4554", "m 5564", "m 65",
             "h 1824", "h 2534", "h 3544", "h 4554", "h 5564", "h 65"]

targets_tab = {}
targets_tex = {}
for tv, ttext, ttab in zip(t_value, t_text, t_tableau):
    targets_tex[tv] = ttext
    targets_tab[tv] = ttab

# endregion

# region Funciones auxiliares

# Inicio de Sesión Analytics para GA4


def ga4_client():
    credentials = service_account.Credentials.from_service_account_file(
        _google_key_ga)
    analytics_credentials = credentials.with_scopes(
        ['https://www.googleapis.com/auth/analytics.readonly'])
    analytics_client = build('analyticsdata', 'v1beta',
                             credentials=analytics_credentials)

    return analytics_client

# Generar request Analytics


def request_analytics(view_id, rango_fechas, agg):
    view_requests = []
    for d_demo in ana_dim_demogr:
        view_request = {}
        view_request["viewId"] = view_id
        view_request["dateRanges"] = [
            {"startDate": rango_fechas[0], "endDate": rango_fechas[1]}]

        metricas_ana = []
        for m_key in ana_metricas:
            metricas_ana.append({"expression": m_key})
        view_request["metrics"] = metricas_ana

        dimensiones_ana = []
        for d_key in ana_dim_tiempo[agg]:
            dimensiones_ana.append({"name": d_key})
        for d_key in ana_dim_demogr[d_demo]:
            dimensiones_ana.append({"name": d_key})

        view_request["dimensions"] = dimensiones_ana
        view_request["pageSize"] = 9000
        view_requests.append(view_request)

    return {"reportRequests": [view_requests]}

# Generar request Firebase y GA4
def request_ga4(property, fechas, agg, demos):
    fecha_inicio = fechas[0]
    fecha_fin = fechas[1]
    if agg == "mensual":
        fecha_inicio

    reqs = []
    for demo in demos:
        dimensions = [{'name': name} for name in ga4_dim_tiempo[agg]
                      ] + [{'name': name} for name in ga4_dim_demogr[demo]]
        req = {
            "dateRanges": [{
                "startDate": fecha_inicio,
                "endDate": fecha_fin}],
            "dimensions":  dimensions,
            "metrics": [{'name': name} for name in ga4_metricas.keys()],
            "limit": 100000}

        reqs.append(req)

    final_req = {"requests": reqs}

    analytics = ga4_client()
    reports = analytics.properties().batchRunReports(
        property=property, body=final_req).execute()

    return reports

# Formatear respuesta Analytics


def analytics_resp_table(response, agreg):
    dfs = {}
    # Cada request corresponde a una agregación demográfica, se debn convertir todas las contenidas en la respuesta
    for request, report in zip(list(ana_dim_demogr.keys()), response.get("reports", [])):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders_dict = columnHeader.get(
            'metricHeader', {}).get('metricHeaderEntries', [])

        metricHeaders = []
        for metricHeader in metricHeaders_dict:
            metricHeaders.append(metricHeader.get("name"))

        headers = dimensionHeaders + metricHeaders

        values = []
        for row in report.get("data", []).get('rows', []):
            dimensionValues = row.get("dimensions", [])
            metricValues = row.get("metrics", [])[0].get("values", [])

            row_values = dimensionValues + metricValues
            values.append(row_values)

        df = pd.DataFrame(columns=headers, data=values)
        for col in list(ana_dim_demogr[request].keys()):
            df = df[df[col] != "unknown"]
        if df.empty:
            continue

        if agreg == "mensual":
            df["datetime"] = df["ga:yearMonth"].astype(str) + "0100"
            df.drop(["ga:yearMonth"], axis=1, inplace=True)
        elif agreg == "diario":
            df["datetime"] = df["ga:date"].astype(str) + "00"
            df.drop(["ga:date"], axis=1, inplace=True)
        elif agreg == "hora":
            df["datetime"] = df["ga:dateHour"].astype(str)
            df.drop(["ga:dateHour"], axis=1, inplace=True)
        elif agreg == "mes hora":
            df["datetime"] = df["ga:yearMonth"].astype(
                str) + "01" + df["ga:hour"].astype(str)
            df.drop(["ga:yearMonth", "ga:hour"], axis=1, inplace=True)

        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H")

        df = pd.melt(df, id_vars=[
                     "datetime"] + list(ana_dim_demogr[request].keys()), value_vars=list(ana_metricas.keys()))
        df["value"] = pd.to_numeric(df["value"])
        df = pd.DataFrame(pd.pivot(df, index=["datetime", "variable"], columns=list(
            ana_dim_demogr[request].keys())).to_records())

        dfs[request] = df.copy()

    return dfs

# Formatear respuesta Firebase


def ga4_resp_table(reports: dict, agg: str, demos: list):
    dfs = {}

    for demo, result in zip(demos, reports["reports"]):
        headers = []
        for dim_header in result["dimensionHeaders"]:
            headers.append(dim_header["name"])
        for metr_header in result["metricHeaders"]:
            headers.append(metr_header["name"])

        values = []
        if "rows" not in result:
            continue
        for row in result["rows"]:
            row_val = []
            for dim in row["dimensionValues"]:
                row_val.append(dim["value"])

            for metr in row["metricValues"]:
                row_val.append(float(metr["value"]))

            values.append(row_val)

        df = pd.DataFrame(values, columns=headers)

        for col in list(ga4_dim_demogr[demo].keys()):
            df = df[df[col] != "unknown"]
        if df.empty:
            continue

        if agg == "mensual":
            df["datetime"] = df["year"].astype(
                str) + df["month"].astype(str) + "0100"
            df.drop(["year", "month"], axis=1, inplace=True)
        elif agg == "diario":
            df["datetime"] = df["date"].astype(str) + "00"
            df.drop(["date"], axis=1, inplace=True)
        elif agg == "hora":
            df["datetime"] = df["dateHour"].astype(str)
            df.drop(["dateHour"], axis=1, inplace=True)
        elif agg == "mes hora":
            df["datetime"] = df["year"].astype(
                str) + df["month"].astype(str) + "01" + df["hour"].astype(str)
            df.drop(["year", "month", "hour"], axis=1, inplace=True)

        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H")

        df = pd.melt(df, id_vars=[
                     "datetime"] + list(ga4_dim_demogr[demo].keys()), value_vars=list(ga4_metricas.keys()))
        df["value"] = pd.to_numeric(df["value"])

        df = pd.DataFrame(pd.pivot(df, index=["datetime", "variable"], columns=list(
            ga4_dim_demogr[demo].keys())).to_records()).fillna(0)

        dfs[demo] = df

    return dfs

# Unión demográficos Firebase. Esto debe hacerse ya que se obtienen en consutlas separadas


def unir_demograficos(dfs):
    df_final = dfs["total"].copy()
    for informe, df in dfs.items():
        if informe == "total":
            continue

        df_merged = pd.merge(dfs["total"].copy(), df, how="left")

        cols_calc = [col for col in list(df_merged.columns) if col not in [
            "datetime", "variable", "value"]]

        no_calc_cols = ["ga:avgTimeOnPage", "ga:bounceRate"]

        df_nocalc = df_merged[df_merged["variable"].isin(no_calc_cols)].copy()
        df_calc = df_merged[~df_merged["variable"].isin(no_calc_cols)].copy()

        df_calc["total_demo"] = df_calc[cols_calc].sum(
            axis=1, numeric_only=False)
        for col in cols_calc:
            df_calc[col] = df_calc[col] * \
                df_calc["value"] / df_calc["total_demo"]
            df_calc[col] = df_calc[col].round()
        df_calc.drop(["total_demo"], axis=1, inplace=True)

        df_demo = pd.concat([df_calc, df_nocalc])
        df_final = pd.merge(df_final, df_demo, how="left")

    return pd.concat([pd.DataFrame(columns=cols_form), df_final]).fillna(0)

# Formatear tabla final


def formatear_tabla(df, fuente, marca, agg, nombres_columnas, cols_final):
    df_final = pd.melt(
        df, id_vars=["datetime", "variable"], value_name="valor", var_name="target_piv")

    df_final = pd.DataFrame(pd.pivot(df_final, index=["datetime", "target_piv"], columns=[
                            "variable"]).droplevel(0, axis=1).to_records())
    df_final = df_final[df_final["target_piv"].isin(t_value)]

    df_final["target_tableau"] = df_final["target_piv"].replace(targets_tab)
    df_final["target"] = df_final["target_piv"].replace(targets_tex)

    soportes = {
        "emisor": "emisorpodcasting.cl",
        "horizonte": "horizonte.cl",
        "oasis": "oasisfm.cl",
        "13cradio": "13cradio.cl",
        "play": "playfm.cl",
        "sonar": "sonarfm.cl",
        "tele13": "tele13radio.cl"
    }

    df_final["fecha"] = df_final["datetime"].dt.date

    if agg == "hora" or agg == "mes hora":
        df_final["hora_inicio"] = df_final["datetime"].dt.time.astype(str)
        df_final["hora_termino"] = (
            df_final["datetime"] + timedelta(hours=1, seconds=-1)).dt.time.astype(str)
    else:
        df_final["hora_inicio"] = "00:00:00"
        df_final["hora_termino"] = "23:59:59"

    df_final.rename(columns=nombres_columnas, inplace=True)

    periodo = agg
    tipo = agg
    agreg = agg[0]
    if agg == "mensual":
        tipo = "mes"
    elif agg == "diario":
        tipo = "dia"
    elif agg == "mes hora":
        agreg = "mh"
        periodo = "hora"

    df_final["periodo"] = periodo
    df_final["tipo"] = tipo
    df_final["soporte"] = soportes[marca]
    df_final["fuente_dato"] = fuente

    df_final["target_id"] = df_final["target_tableau"].str.replace(
        r'\W+', '', regex=True).str.lower()
    df_final["id"] = df_final["fuente_dato"].str[0] + "_" + marca.lower()[:3] + "_" + \
        df_final["datetime"].dt.strftime(
            "%y%m%d%H") + "_" + agreg + "_" + df_final["target_id"]

    return pd.concat([pd.DataFrame(columns=cols_final), df_final])[cols_final]

# Subir datos a BigQuery


def subir_datos(df: pd.DataFrame) -> None:
    client = bigquery.Client.from_service_account_json(_google_key_bq)
    table_id = "conexion-datos-rdf.trafico_digital.carga_diaria"

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
            bigquery.SchemaField("tipo", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "soporte", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "fuente_dato", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("target", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "target_tableau", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField(
                "porc_rebote", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField(
                "sesiones", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField(
                "pag_vistas", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField(
                "tprom_pagina", bigquery.enums.SqlTypeNames.FLOAT),
            bigquery.SchemaField(
                "usuarios_unicos", bigquery.enums.SqlTypeNames.INTEGER)
        ]
    )

    # Make an API request.
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    print(job.result())  # Wait for the job to complete.

# endregion

# region Funciones principales

# limpiar tabla update. Esto se hace debido a la creación de ID para eliminación de duplicados


def limpiar_tabla():
    replace_days = int(Variable.get("dias_remplazo"))

    ts = date.today()
    start_time = (ts + timedelta(days=-replace_days)).strftime("%Y-%m-%d")
    start_time_m = (ts + timedelta(days=-replace_days)
                    ).replace(day=1).strftime("%Y-%m-%d")

    client = bigquery.Client.from_service_account_json(_google_key_bq)

    delete_job = client.query(
        """
        DELETE
        FROM `conexion-datos-rdf.trafico_digital.carga_diaria`
        WHERE fecha >= "{0}"
        """.format(start_time))

    print(delete_job.result())

    delete_job_m = client.query(
        """
        DELETE
        FROM `conexion-datos-rdf.trafico_digital.carga_diaria`
        WHERE fecha >= "{0}"
        AND periodo = "mensual"
        """.format(start_time_m))

    print(delete_job_m.result())

# Ejecución consulta Analytics


def datos_analytics():
    # configuraciones de las credenciales de autenticación de Google
    key_file_location = "/home/airflow/airflow/dags/servacc_trafico.json"
    scopes = ['https://www.googleapis.com/auth/analytics.readonly']

    replace_days = int(Variable.get("dias_remplazo"))

    # Iniciar servicio de API de reportes de Analytics
    credenciales = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes)
    analytics = build('analyticsreporting', 'v4', credentials=credenciales)

    ts = date.today()
    end_time = ts.strftime("%Y-%m-%d")
    start_time = (ts + timedelta(days=-replace_days)).strftime("%Y-%m-%d")
    fechas = [start_time, end_time]

    df_total = pd.DataFrame(columns=cols_final)

    for agg, marca in list(itertools.product(agregaciones, marcas)):
        if not marca in list(ana_view_ids.keys()):
            continue

        fechas_con = fechas.copy()
        if agg == "mensual" or agg == "mes hora":
            fechas_con[0] = datetime.strptime(
                fechas[0], "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")
        request = request_analytics(ana_view_ids[marca], fechas_con, agg)
        response = analytics.reports().batchGet(body=request).execute()
        dfs = analytics_resp_table(response, agg)
        df = unir_demograficos(dfs)
        df_final = formatear_tabla(
            df, "analytics", marca, agg, ana_metricas, cols_final)

        df_total = pd.concat([df_total, df_final])

    df_total = df_total.convert_dtypes().sort_values(
        by="id").drop(["ttot_1erplano", "usuarios_nuevos"], axis=1)
    subir_datos(df_total)

# Ejecución consulta Firebase


def datos_ga4(prop_ids, fuente):
    replace_days = int(Variable.get("dias_remplazo"))

    ts = date.today()
    end_time = (ts + timedelta(days=-1)).strftime("%Y-%m-%d")
    start_time = (ts + timedelta(days=-replace_days)).strftime("%Y-%m-%d")
    fechas = [start_time, end_time]

    aggs = list(ga4_dim_tiempo.keys())
    demos = list(ga4_dim_demogr.keys())

    df_total = pd.DataFrame(columns=cols_final)

    for agg, soporte in list(itertools.product(aggs, prop_ids)):
        fechas_con = fechas.copy()
        if agg == "mensual" or agg == "mes hora":
            fechas_con[0] = datetime.strptime(
                fechas[0], "%Y-%m-%d").replace(day=1).strftime("%Y-%m-%d")

        print(fechas_con, agg, soporte)
    
        prop_id = "properties/" + prop_ids[soporte]
        reports = request_ga4(prop_id, fechas_con, agg, demos)

        dfs = ga4_resp_table(reports, agg, demos)
        df = unir_demograficos(dfs)
        df_final = formatear_tabla(
            df, fuente, soporte, agg, ga4_metricas, cols_final)
        df_total = pd.concat([df_total, df_final])

    df_total = df_total.convert_dtypes().sort_values(
        by="id").drop(["ttot_1erplano", "usuarios_nuevos"], axis=1)

    subir_datos(df_total)


def consulta_analytics_ga3():
    print("Datos vistas Analytics v3...")
    datos_analytics()

def consulta_analytics_ga4():
    print("Datos vistas Analytics v4...")
    datos_ga4(ga4_prop_ids, "analytics")


def consulta_firebase_old():
    print("Datos apps antiguas...")
    datos_ga4(fire_prop_ids, "firebase")

def consulta_firebase_new():
    print("Datos apps nuevas...")
    datos_ga4(fire_new_prop_ids, "firebase_new")


def flag_off():
    Variable.update(key="correo_trafico", value=False)


def flag_on():
    Variable.update(key="correo_trafico", value=True)

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

with DAG(
    "trafico_digital",
    default_args=args,
    description="Actualización diaria de datos de audio digital",
    schedule_interval="0 7 * * *",
    catchup=False,
    start_date=datetime(2021, 9, 6, tzinfo=local_tz),
    tags=["trafico", "analytics",  "firebase", "diario"],
) as dag:

    limpiar = PythonOperator(
        task_id="limpiar_tabla",
        python_callable=limpiar_tabla
    )

    ga3 = PythonOperator(
        task_id="consulta_analytics",
        python_callable=consulta_analytics_ga3
    )

    ga4 = PythonOperator(
        task_id="consulta_analytics_ga4",
        python_callable=consulta_analytics_ga4
    )

    fire_old = PythonOperator(
        task_id="consulta_firebase",
        python_callable=consulta_firebase_old
    )

    fire_new = PythonOperator(
        task_id="consulta_firebase_new",
        python_callable=consulta_firebase_new
    )

    marca_reset = PythonOperator(
        task_id="reset_marca",
        python_callable=flag_off
    )

    marca_ok = PythonOperator(
        task_id="marcar_ok",
        python_callable=flag_on
    )

    marca_reset >> limpiar >> [ga3, fire_old]
    ga3 >> ga4
    fire_old >> fire_new
    [ga4, fire_new] >> marca_ok