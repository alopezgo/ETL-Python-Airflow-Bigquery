""" Audiencias y Consumo - Email Seguimiento

Este script tiene como objetivo informar el estado de la cantidad de devices 
actualizado a la fecha de consulta para los medias de RatPack Domingo.

"""

#Importar librerías
import smtplib
import ssl
from datetime import date, datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
local_tz = pendulum.timezone("America/Santiago")
_google_key = "/home/airflow/airflow/dags/servacc_bigquery.json"


def gen_email(data: dict) -> str:
    """Generación del HTML para correo de seguimiento

    Args:
        data (dict): diccionario de procesos para envío

    Returns:
        str: correo HTML
    """

    query_list = []
    for content in data["content_id"]:
        query_list.append(f'"{content}"')

    str_list = ",".join(query_list)
    query = f"""
    SELECT COUNT(DISTINCT consumo.request_ip||consumo.user_agent) AS devices
    FROM `conexion-datos-rdf.consumo.consumo_detalle` AS consumo
    WHERE consumo.start_date >= "2022-03-13 03:00:00"
    AND consumo.content_id IN ({str_list})
    """

    client = bigquery.Client.from_service_account_json(_google_key)
    result = client.query(query)
    df = result.to_dataframe()
    devices = str(df.iloc[0,0])

    fecha = date.today().strftime("%d-%m-%Y")
    html_1 = """<!doctype html><html xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office"><head><title></title><!--[if !mso]><!--><meta http-equiv="X-UA-Compatible" content="IE=edge"><!--<![endif]--><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style type="text/css">#outlook a { padding:0; }
            body { margin:0;padding:0;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%; }
            table, td { border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt; }
            img { border:0;height:auto;line-height:100%; outline:none;text-decoration:none;-ms-interpolation-mode:bicubic; }
            p { display:block;margin:13px 0; }</style><!--[if mso]>
            <noscript>
            <xml><o:OfficeDocumentSettings><o:AllowPNG/><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml>
            </noscript><![endif]--><!--[if lte mso 11]>
            <style type="text/css">
            .mj-outlook-group-fix { width:100% !important; }
            </style>
            <![endif]--><!--[if !mso]><!--><link href="https://fonts.googleapis.com/css?family=Ubuntu:300,400,500,700" rel="stylesheet" type="text/css"><style type="text/css">@import url(https://fonts.googleapis.com/css?family=Ubuntu:300,400,500,700);</style><!--<![endif]--><style type="text/css">@media only screen and (min-width:480px) {
            .mj-column-per-100 { width:100% !important; max-width: 100%; }
    .mj-column-per-90 { width:90% !important; max-width: 90%; }
        }</style><style media="screen and (min-width:480px)">.moz-text-html .mj-column-per-100 { width:100% !important; max-width: 100%; }
    .moz-text-html .mj-column-per-90 { width:90% !important; max-width: 90%; }</style>
            <style type="text/css"></style></head>
            <body style="word-spacing:normal;"><div><!--[if mso | IE]>
            <table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" bgcolor="#FC5000" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#FC5000;background-color:#FC5000;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#FC5000;background-color:#FC5000;width:100%;"><tbody>
            <tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:600px;" ><![endif]--><div class="mj-column-per-100 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td align="left" style="font-size:0px;padding:10px 25px;word-break:break-word;"><table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:22px;table-layout:auto;width:100%;border:none;"><tr><td style="color:white; vertical-align: middle;"><span style="font-size: 12px;">Audiencias y Consumo</span><br><span style="font-size: 16px;font-weight: bold;">Devices Rat Pack Domingo</span></td><td style="width:40px; vertical-align: middle;"><img src="https://icon-library.com/images/refresh-icon-white/refresh-icon-white-1.jpg" width="30px"></td></tr></table></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" bgcolor="#ffe6d2" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#ffe6d2;background-color:#ffe6d2;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#ffe6d2;background-color:#ffe6d2;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" width="600px" ><table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:540px;" ><![endif]--><div class="mj-column-per-90 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td align="left" style="font-size:0px;padding:10px 25px;padding-top:0px;word-break:break-word;"><div style="font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:1;text-align:left;color:#000000;">
            La cantidad de devices a día de hoy ("""
    html_2 = """) es de:</div></td></tr><tr><td align="center" style="font-size:0px;padding:10px 25px;word-break:break-word;"><table cellpadding="0" cellspacing="0" width="90%" border="0" style="color:#000000;font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:22px;table-layout:auto;width:90%;border:none;"><tr style="border: 1px;"><td style="width:50px; vertical-align: middle; padding-bottom: 5px;"><img src="https://storage.googleapis.com/audiencias-rdf/images/notifications/dispositivos.png" width="30px"></td><td style="vertical-align: middle;padding-bottom: 5px;"><span style="font-size: 16px;font-weight: bold;">"""
    html_3 = """</span></td></tr></table></td></tr><tr><td align="left" style="font-size:0px;padding:10px 25px;word-break:break-word;"><div style="font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:1;text-align:left;color:#000000;">Saludos!</div></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" bgcolor="#FC5000" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#FC5000;background-color:#FC5000;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#FC5000;background-color:#FC5000;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:5px;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:590px;" ><![endif]--><div class="mj-column-per-100 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td style="font-size:0px;word-break:break-word;"><div style="height:10px;line-height:10px;">&#8202;</div></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></div></body>
    </html>"""

    full_email = html_1 + fecha + html_2 + devices + html_3

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
    email_data = Variable.get("correo_seguimiento_ratpack", deserialize_json=True)
    email_html = gen_email(email_data)

    # Se define el mensaje y se agrega el contenido en HTML
    message = MIMEMultipart("multipart")
    part2 = MIMEText(email_html, "html")
    message.attach(part2)

    # Se definen asunto, remitente y destinatarios del correo
    message["Subject"] = "{0} - {1}".format(
        email_data["subject"], date.today())
    message["From"] = sender
    message['To'] = ', '.join(email_data["dest"])

    # Se inicia la conexión al servidor y se envía el correo
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login("audiencias@rdfmedia.cl", password)
        server.sendmail(sender, email_data["dest"], message.as_string())


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
    "email_ratpack",
    default_args=args,
    description="",
    schedule_interval="0 9 * * 3",
    catchup=False,
    start_date=datetime(2022, 10, 3, tzinfo=local_tz),
    tags=["audio", "platform", "diario", "live", "on demand"],
)

# Se definen los pasos del dag según funciones anteriores
t1 = PythonOperator(
    task_id='enviar_correo',
    python_callable=init_email,
    dag=dag
)

# Configuración de la ejecución del DAG.
t1
