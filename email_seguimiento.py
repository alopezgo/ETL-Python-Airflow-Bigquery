""" Audiencias y Consumo - Email Seguimiento

Este script tiene como objetivo informar al Equipo de Audiencias mediante correo 
sobre la actualización diaria de fuentes de datos de consumo realizada de forma 
automática en Apache Airflow, y ser programadas para el envío periódico 
en la plataforma Apache Airflow.

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

# Configuraciones de fecha y zonas horarias. Estas son necesarias para
# ejecutar correctamente el script y realizar las conversiones de formatos
# necesarias
local_tz = pendulum.timezone("America/Santiago")


def gen_email(data: dict) -> str:
    """Generación del HTML para correo de seguimiento

    Args:
        data (dict): diccionario de procesos para envío

    Returns:
        str: correo HTML
    """

    # Por cada proceso de fuente de datos...
    rows_code = ""
    for data_source, values in data["data_sources"].items():
        # Se verifica el valor de la variable en el entorno de Airflow
        ok = Variable.get("correo_{0}".format(data_source))
        icon = "error_icon"
        if ok: icon = "ok_icon"

        # Se genera una nueva fila con el HTML correspondiente
        row_code = """<tr style="border: 1px;">
                <td style="width:50px; vertical-align: middle; padding-bottom: 5px;">
                    <img src="https://storage.googleapis.com/audiencias-rdf/images/notifications/{0}.png" width="30px">
                </td>
                <td style="vertical-align: middle;padding-bottom: 5px;">
                    <span style="font-size: 12px;">{1}</span><br>
                    <span style="font-size: 16px;font-weight: bold;">{2}</span>
                </td>
            </tr>
            """.format(icon, values["tipo"], values["titulo"])

        # Se anexa al final de la cadena HTML de la tabla de procesos
        rows_code = rows_code + row_code

    # Se utiliza la plantilla del correo añadiendo
    email_html = """<!doctype html><html xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office"><head><title></title><!--[if !mso]><!--><meta http-equiv="X-UA-Compatible" content="IE=edge"><!--<![endif]--><meta http-equiv="Content-Type" content="text/html; charset=UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style type="text/css">\outlook a { padding:0; }
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
    .moz-text-html .mj-column-per-90 { width:90% !important; max-width: 90%; }</style><style type="text/css">@media only screen and (max-width:480px) {
        table.mj-full-width-mobile { width: 100% !important; }
        td.mj-full-width-mobile { width: auto !important; }
        }</style></head><body style="word-spacing:normal;"><div><!--[if mso | IE]><table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" bgcolor="white" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:white;background-color:white;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:white;background-color:white;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:600px;" ><![endif]--><div class="mj-column-per-100 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td align="center" style="font-size:0px;padding:10px 25px;word-break:break-word;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="border-collapse:collapse;border-spacing:0px;"><tbody><tr><td style="width:200px;"><img height="auto" src="https://storage.googleapis.com/audiencias-rdf/images/notifications/rdf_logo.png" style="border:0;display:block;outline:none;text-decoration:none;height:auto;width:100%;font-size:13px;" width="200"></td></tr></tbody></table></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" bgcolor="#FC5000" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#FC5000;background-color:#FC5000;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#FC5000;background-color:#FC5000;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:600px;" ><![endif]--><div class="mj-column-per-100 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td align="left" style="font-size:0px;padding:10px 25px;word-break:break-word;"><table cellpadding="0" cellspacing="0" width="100%" border="0" style="color:#000000;font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:22px;table-layout:auto;width:100%;border:none;"><tr><td style="color:white; vertical-align: middle;"><span style="font-size: 12px;">Audiencias y Consumo</span><br><span style="font-size: 16px;font-weight: bold;">Informe de Actualización de Datos</span></td><td style="width:40px; vertical-align: middle;"><img src="https://storage.googleapis.com/audiencias-rdf/images/notifications/update_icon.png" width="40px"></td></tr></table></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" bgcolor="#ffe6d2" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#ffe6d2;background-color:#ffe6d2;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#ffe6d2;background-color:#ffe6d2;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" width="600px" ><table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:20px 0;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:540px;" ><![endif]--><div class="mj-column-per-90 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td align="left" style="font-size:0px;padding:10px 25px;padding-top:0px;word-break:break-word;"><div style="font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:1;text-align:left;color:#000000;">Este es el estado de la ejecución de los distintos procesos de extracción y actualización de datos del día de hoy:</div></td></tr><tr><td align="center" style="font-size:0px;padding:10px 25px;word-break:break-word;"><table cellpadding="0" cellspacing="0" width="90%" border="0" style="color:#000000;font-family:Ubuntu, Helvetica, Arial, sans-serif;font-size:13px;line-height:22px;table-layout:auto;width:90%;border:none;">
        """
    email_footer = """
        </table>
        </td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><table align="center" border="0" cellpadding="0" cellspacing="0" class="" role="presentation" style="width:600px;" width="600" bgcolor="#FC5000" ><tr><td style="line-height:0px;font-size:0px;mso-line-height-rule:exactly;"><![endif]--><div style="background:#FC5000;background-color:#FC5000;margin:0px auto;max-width:600px;"><table align="center" border="0" cellpadding="0" cellspacing="0" role="presentation" style="background:#FC5000;background-color:#FC5000;width:100%;"><tbody><tr><td style="direction:ltr;font-size:0px;padding:5px;text-align:center;"><!--[if mso | IE]><table role="presentation" border="0" cellpadding="0" cellspacing="0"><tr><td class="" style="vertical-align:top;width:590px;" ><![endif]--><div class="mj-column-per-100 mj-outlook-group-fix" style="font-size:0px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;"><table border="0" cellpadding="0" cellspacing="0" role="presentation" style="vertical-align:top;" width="100%"><tbody><tr><td style="font-size:0px;word-break:break-word;"><div style="height:5px;line-height:5px;">&#8202;</div></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></td></tr></tbody></table></div><!--[if mso | IE]></td></tr></table><![endif]--></div></body></html>
            """

    # Se junta la plantilla, la tabla de procesos y el pie del correo en un elemento
    full_email = email_html + rows_code + email_footer

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
    email_data = Variable.get("correo_seguimiento_diario", deserialize_json=True)
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
    "email_seguimiento",
    default_args=args,
    description="",
    schedule_interval="0 8 * * 1-5",
    catchup=False,
    start_date=datetime(2022, 4, 21, tzinfo=local_tz),
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
