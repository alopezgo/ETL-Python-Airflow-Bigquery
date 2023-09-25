""" Audiencias y Consumo - Email Seguimiento

Este script tiene como objetivo informar al Equipo de Audiencias mediante correo 
sobre la actualización diaria de fuentes de datos de consumo realizada de forma 
automática en Apache Airflow, y ser programadas para el envío periódico 
en la plataforma Apache Airflow.

"""

# Importar librerías
import pandas as pd
from google.cloud import bigquery

import smtplib
import ssl
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pytz
from dateutil.relativedelta import relativedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
local_tz = pendulum.timezone("America/Santiago")
cl = pytz.timezone("America/Santiago")
fmt = '%d-%m-%Y'
now = datetime.today().date()

fmt_query = '%Y-%m-%d'

fin_mes = now + timedelta( days = -7)
fin_mes = fin_mes + relativedelta(day=31)
inicio_mes = fin_mes.replace(day= 1)
inicio_consumo = inicio_mes + timedelta (days= -2)
fin_consumo = fin_mes + (timedelta (days= 2))

inicio_mes = inicio_mes.strftime(fmt_query)
fin_mes = fin_mes.strftime(fmt)
inicio_consumo = inicio_consumo.strftime(fmt_query)
fin_consumo = fin_consumo.strftime(fmt_query)

print('inicio mes: ' + inicio_mes + '\n' + 'inicio mes: ' + fin_mes + '\n' + 'inicio consumo: ' + inicio_consumo 
+ '\n' + 'fin consumo: ' + fin_consumo)

def gen_query():

  query1 = """
  With raw as (
    select distinct 
    date(con.fecha_inicio) as fecha_inicio, 
    date(con.fecha_fin) as fecha_fin, 
    sho.categoria,
    sho.subcategoria,  
    con.vip, 
    con.start, 
    con.stream
    FROM `audio_digital.funnel_show_property_old` as con
    join `plataformas_externas.available_shows` as sho
    on con.show_id = sho.id_platform
    where sho.catalogo is true
    and lower(con.content_type) != 'live' 
    and con.periodo = 'mensual'
    and fecha_inicio = '{0}'
    and con.vip in (0,1)),

    start_plat_categoria as(
    select 
    categoria,
    sum(start) as start  
    from raw
    where vip = 0
    group by categoria),

    stream_plat_categoria as (
      select 
      categoria,
      sum(stream) as stream
      from raw
      where vip = 1
      group by categoria)
    

  Select 
    s_plat.categoria,
    s_plat.start,
    sd_plat.stream 
    from start_plat_categoria as s_plat
    join stream_plat_categoria as sd_plat
    on s_plat.categoria = sd_plat.categoria

  """.format(inicio_mes)

  query2 = """
  WITH 
  bun AS (
    SELECT distinct rdf_name, id_platform, categoria, subcategoria
    FROM `plataformas_externas.available_shows` 
    where catalogo is true),

  medias AS (
      SELECT distinct
      medias.show_id,
      medias.media_id
      FROM bun
      JOIN `dicc_medios.dicc_medios` AS medias
      ON bun.id_platform = medias.show_id),

  periodo as (
    SELECT 
    hora_inicio,
    hora_fin,
    periodo
    from `diccionarios.dicc_fechas`
    where periodo= 'mensual'
    and hora_inicio >= '{0}'),

  consumo AS (
    Select 
    datetime(start_date, 'America/Santiago') as start_date_cl,
    datetime(end_date, 'America/Santiago') as end_date_cl,
    content_id,
    to_hex(md5(request_ip||user_agent)) as device_id
    from `consumo.consumo_detalle` 
    where start_date between '{1}' and '{2}'
    and content_type = 'Ondemand'
    and minutes >= 1)


    SELECT
    categoria, 
    count(distinct device_id) as devices
    from consumo
    join medias
    on consumo.content_id = medias.media_id
    join bun 
    on medias.show_id = bun.id_platform
    join periodo
    on consumo.start_date_cl < periodo.hora_fin
    and consumo.end_date_cl >= periodo.hora_inicio
    group by categoria

  """.format(inicio_mes, inicio_consumo, fin_consumo)

  return query1, query2

def descarga_data (query1 : str, query2 : str):

    df_total = pd.DataFrame()

    # Inicio de sesión de BigQuery con archivo de credenciales
    client = bigquery.Client()

    # Ejecución de consulta y conversión a DataFrame de Pandas
    df1 = client.query(query1).result().to_dataframe(
        create_bqstorage_client=False)
    df2 = client.query(query2).result().to_dataframe(
        create_bqstorage_client=False)
    
    df_total = pd.merge(df1, df2, how="inner").sort_values(by= 'categoria')
    
    return df_total

def gen_email(df: pd.DataFrame) -> str:
    """Generación del HTML para correo de seguimiento

    Args:
        data (dict): diccionario de procesos para envío

    Returns:
        str: correo HTML
    """
    df=df.reset_index(drop=True)
    df_dict = df.to_dict(orient='split')

    fin_mes = now + timedelta( days = -7)
    fin_mes = fin_mes + relativedelta(day=31)
    inicio_mes = fin_mes.replace(day= 1)
    inicio_consumo = inicio_mes + timedelta (days= -2)
    fin_consumo = fin_mes + (timedelta (days= 2))

    inicio_mes = inicio_mes.strftime(fmt)
    fin_mes = fin_mes.strftime(fmt)

    # Por cada proceso de fuente de datos...
    rows_code = ""

    for item in df_dict['data']:
            # Se genera una nueva fila con el HTML correspondiente
        row_code = """
                <tr style="border: 1px;">
                <td style="vertical-align: middle padding-bottom: 5px font-size: 18px; font-weight: bold;">{0}</td>
                <td style="vertical-align: middle padding-bottom: 5px font-size: 18px; font-weight: bold;">{1}</td>
                <td style="vertical-align: middle padding-bottom: 5px font-size: 18px; font-weight: bold;">{2}</td>
                <td style="vertical-align: middle padding-bottom: 5px font-size: 18px; font-weight: bold;">{3}</td>
                </tr>
                """.format(item[0], item[1], item[2], item[3])

            # Se anexa al final de la cadena HTML de la tabla de procesos
        rows_code = rows_code + row_code

    print(rows_code)

        # Se utiliza la plantilla del correo añadiendo
    email_html = """
    <!doctype html><html xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office"><head><title></title><!--[if !mso]><!--><meta http-equiv="X-UA-Compatible" content="IE=edge"><!--<![endif]--><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style type="text/css">#outlook a { padding:0; }
              body { margin:0;padding:0;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%; }
              table, td { border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt; }
              img { border:0;height:auto;line-height:100%; outline:none;text-decoration:none;-ms-interpolation-mode:bicubic; }
              p { display:block;margin:13px 0; }</style><!--[if mso]>
            <noscript>
            <xml>
            <o:OfficeDocumentSettings>
              <o:AllowPNG/>
              <o:PixelsPerInch>96</o:PixelsPerInch>
            </o:OfficeDocumentSettings>
            </xml>
            </noscript>
            <![endif]--><!--[if lte mso 11]>
            <style type="text/css">
              .mj-outlook-group-fix { width:100% !important; }
            </style>
            <![endif]--><!--[if !mso]><!--><link href="https://fonts.googleapis.com/css?family=Ubuntu:300,400,500,700" rel="stylesheet" type="text/css"><style type="text/css">@import url(https://fonts.googleapis.com/css?family=Ubuntu:300,400,500,700);</style><!--<![endif]--><style type="text/css">@media only screen and (min-width:480px) {
            .mj-column-per-100 { width:100% !important; max-width: 100%; }
    .mj-column-per-90 { width:90% !important; max-width: 90%; }
          }</style><style media="screen and (min-width:480px)">.moz-text-html .mj-column-per-100 { width:100% !important; max-width: 100%; }
    .moz-text-html .mj-column-per-90 { width:90% !important; max-width: 90%; }</style><style type="text/css"></style></head><body style="word-spacing:normal;"><div><!--[if mso | IE]><table align="center" border="0" cellpadding="0" cellspacing="0" class="" style="width:600px;" width="600" bgcolor="#FC5000" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#FC5000;background-color:#FC5000;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#FC5000;background-color:#FC5000;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:600px;" ><![endif]--><div class="mj-column-per-100 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td align="left" style="font-size:0px;padding:10px 25px;word-break:break-word;"><table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:22px;table-layout:auto;width:100%;border:none;"><tr><td style="color:white; vertical-align: middle;"><span style="font-size: 12px;">Audiencias y Consumo</span><br><span style="font-size: 18px;font-weight: bold;">Podcasts Catálogo Comercial</span></td><td style="width:40px; vertical-align: middle;"><img src="https://cdn-icons-png.flaticon.com/512/2368/2368447.png" width="60px"></td></tr></table></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" class="" style="width:600px;" width="600" bgcolor="#ffe6d2" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#ffe6d2;background-color:#ffe6d2;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#ffe6d2;background-color:#ffe6d2;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" width="600px" ><table align="center" border="0" cellpadding="0" cellspacing="0" class="" style="width:600px;" width="600" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:540px;" ><![endif]--><div class="mj-column-per-90 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td align="left" style="font-size:0px;padding:10px 25px;padding-top:0px;word-break:break-word;"><div style="font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:1;text-align:left;color:#000000;">Consumo entre """

    email_html2 = """:</div></td></tr><tr><td align="center" style="font-size:0px;padding:10px 25px;word-break:break-word;"><table cellpadding="0" cellspacing="0" width="90%" border="0" style="color:#000000;font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:22px;table-layout:auto;width:90%;border:1px solid black;"><tr style="border: 1px solid;"><td style="vertical-align: middle padding-bottom: 5px font-size: 18px;font-weight: bold;">Categoria</td><td style="vertical-align: middle padding-bottom: 5px font-size: 16px;font-weight: bold;">Start</td><td style="vertical-align: middle padding-bottom: 5px font-size: 18px;font-weight: bold;">Stream</td><td style="vertical-align: middle padding-bottom: 5px font-size: 18px;font-weight: bold;">Device</td></tr>"""
    email_footer = """
    </table></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" class="" style="width:600px;" width="600" bgcolor="#FC5000" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#FC5000;background-color:#FC5000;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#FC5000;background-color:#FC5000;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:5px;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:590px;" ><![endif]--><div class="mj-column-per-100 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td style="font-size:0px;word-break:break-word;"><div style="height:10px;line-height:10px;">&#8202;</div></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></div></body></html>"""

    # Se junta la plantilla, la tabla de procesos y el pie del correo en un elemento
    full_email = email_html + inicio_mes + ' y ' + fin_mes + email_html2 + rows_code + email_footer

    return full_email



def send_email_basic():
    """Envío de HTML mediante STMP de Gmail
    """

    # Parámetros de remitente para envío
    smtp_server = "smtp.gmail.com"
    sender = "Audiencias RDFMedia <audiencias@rdfmedia.cl>"
    password = 'tnyztthquvkcpcyl'
    port = 465

    # Se extra diccionario de procesos a revisar y se genera el HTML del correo
    data = Variable.get(
        "correo_catalogo_comercial", deserialize_json=True)
    querys = gen_query()

    df_total = descarga_data(querys[0], querys[1])

    email_html = gen_email(df_total)

    # Se define el mensaje y se agrega el contenido en HTML
    message = MIMEMultipart("multipart")
    part2 = MIMEText(email_html, "html")
    message.attach(part2)

    # Se definen asunto, remitente y destinatarios del correo
    message["Subject"] = "{0} - {1}".format(
        data["subject"], date.today())
    message["From"] = sender
    message['To'] = ', '.join(data["dest"])

    # Se inicia la conexión al servidor y se envía el correo
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login("audiencias@rdfmedia.cl", password)
        server.sendmail(sender, data["dest"], message.as_string())

def init_email():
    """Función de envío de correo
    """
    # Se extraen los feriados a fin de realizar envíos sólo en días hábiles
    feriados_dict = Variable.get("feriados", deserialize_json=True)
    if date.today().strftime("%Y-%m-%d") not in feriados_dict["feriados"]:
        send_email_basic()
    else:
        print("Disfruten del feriado")


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
dag = DAG(
    "email_comercial_catalogo",
    default_args=args,
    description="",
    schedule_interval="30 9 4 * *",
    catchup=False,
    start_date=datetime(2022, 10, 28, tzinfo=local_tz),
    tags=["audio", "platform", "on demand"],
)

# Se definen los pasos del dag según funciones anteriores
t1 = PythonOperator(
    task_id='enviar_correo',
    python_callable=init_email,
    dag=dag
)

# Configuración de la ejecución del DAG.
t1

