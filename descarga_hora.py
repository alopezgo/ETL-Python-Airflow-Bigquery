from datetime import datetime

import pendulum
import pytz
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

local_tz = pendulum.timezone("America/Santiago")
cl = pytz.timezone("America/Santiago")
utc = pytz.utc
fmt = '%Y-%m-%dT%H:%M:%SZ'

def dummy():
    print("Hola, no hago nada ;)")

with DAG(
    dag_id='descarga_hora',
    schedule_interval="30 8-18 * * 1-5",
    start_date=datetime(2022, 9, 10, tzinfo=local_tz),
    catchup=False,
    tags=["audio","hora","live","on demand","platform"],
) as dag:
    consumo_detalle = TriggerDagRunOperator(
        task_id="consumo_detalle_hora",
        trigger_dag_id="consumo_detalle_hora",
        wait_for_completion=True,
        conf={
            "update": "hora"
        }
    )

    consumo_usuarios = TriggerDagRunOperator(
        task_id="consumo_usuarios_hora",
        trigger_dag_id="consumo_usuarios_hora",
        wait_for_completion=True,
        conf={
            "update": "hora"
        }
    )

    audio_digital = TriggerDagRunOperator(
        task_id="audio_digital_hora",
        trigger_dag_id="audio_digital_hora",
        wait_for_completion=True,
        conf={
            "update": "hora"
        }
    )

    consumo_bloques = TriggerDagRunOperator(
        task_id="consumo_bloques_hora",
        trigger_dag_id="consumo_bloques_hora",
        wait_for_completion=True,
        conf={
            "update": "hora"
        }
    )

    consumo_detalle >> consumo_usuarios >> audio_digital >> consumo_bloques