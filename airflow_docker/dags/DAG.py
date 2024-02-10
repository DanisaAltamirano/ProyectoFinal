from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import requests
import pandas as pd
from os import environ as env
import psycopg2
import os 
from airflow.operators.email_operator import EmailOperator
import smtplib

# Definir argumentos predeterminados del DAG
default_args = {
    'owner': 'Danisa_Altamirano',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crear el objeto DAG
dag = DAG(
    'Danisa_Altamirano_DAG',
    default_args=default_args,
    description='DAG para ejecutar entregable.py, validar datos y enviar notificación por correo',
    schedule_interval=timedelta(days=1),
)

def extraer_insertar_datos(**kwargs):
    response = requests.get("https://api.frankfurter.app/latest")

    if response.status_code == 200:
        data = response.json()
        rates_data = data.get("rates", {})
        
        df = pd.DataFrame([rates_data], index=[data["base"]])
        df["LastUpdated"] = pd.to_datetime(data["date"])
        df = df.T.reset_index()
        df.columns = ["Currency", "Exchange"]
        df["LastUpdated"] = pd.to_datetime(data["date"])
        start_date = pd.to_datetime('today') - pd.DateOffset(days=10)
        filtered_df = df[df["LastUpdated"] >= start_date]
        df["ID"] = range(1, len(df) + 1)
        df = df.iloc[:-1]
        df = df[["ID", "Currency", "Exchange", "LastUpdated"]]
        
        print(df)

        # Convertir Timestamp a cadena antes de guardarlo en XCom
        lastupdated_str = df['LastUpdated'].max().strftime('%Y-%m-%d %H:%M:%S')

        # Guardar la última fecha actualizada como una cadena en el contexto de Airflow
        kwargs['ti'].xcom_push(key='lastupdated', value=lastupdated_str)

        # Establecer conexión a Redshift
        REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST")
        REDSHIFT_PORT = os.environ.get("REDSHIFT_PORT")
        REDSHIFT_DB = os.environ.get("REDSHIFT_DB")
        REDSHIFT_USER = os.environ.get("REDSHIFT_USER")
        REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD")
        REDSHIFT_SCHEMA = os.environ.get("REDSHIFT_SCHEMA")
        cadena_conexion = f"host={REDSHIFT_HOST} port={REDSHIFT_PORT} dbname={REDSHIFT_DB} user={REDSHIFT_USER} password={REDSHIFT_PASSWORD} options='-c search_path={REDSHIFT_SCHEMA}'"
        
        try:
            conexion = psycopg2.connect(cadena_conexion)
            cursor = conexion.cursor()
            print("Conexión a Redshift establecida correctamente.")
            cursor.close()
            conexion.close()
        except Exception as e:
            print(f"Error al conectar a Redshift: {e}")
            return None

        # Crear tabla en Redshift
        tabla_sql = f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.CurrencyExchange(
                ID INT PRIMARY KEY,
                Currency VARCHAR(50) NOT NULL,
                exchange DECIMAL(10, 2) NOT NULL,
                LastUpdated DATETIME
            );
        """   

        try:
            conexion = psycopg2.connect(cadena_conexion)
            cursor = conexion.cursor()
            cursor.execute(tabla_sql)
            conexion.commit()
            print("Tabla creada o ya existente correctamente.")
            cursor.close()
            conexion.close()
        except Exception as e:
            print(f"Error al conectar a Redshift o al crear la tabla: {e}")
            return None

        # Insertar datos en Redshift
        nombre_tabla = 'currencyexchange'

        try:
            conexion = psycopg2.connect(cadena_conexion)
            cursor = conexion.cursor()
            df['LastUpdated'] = pd.to_datetime(df['LastUpdated']).dt.strftime('%Y-%m-%d %H:%M:%S')
            columns = ",".join(df.columns)
            values_template = ",".join(["%s"] * len(df.columns))
            query = f"INSERT INTO {REDSHIFT_SCHEMA}.{nombre_tabla} ({columns}) VALUES ({values_template})"
            
            for _, row in df.iterrows():
                cursor.execute(query, tuple(row))
            
            conexion.commit()
            print(f'Datos insertados en la tabla {REDSHIFT_SCHEMA}.{nombre_tabla} correctamente.')
        except Exception as e:
            print(f"Error al conectar a Redshift o al insertar datos en la tabla: {e}")
        finally:
            if conexion:
                conexion.close()
    else:
        print(f"Error en la solicitud. Código de estado: {response.status_code}")
        return None

# Tarea para extraer e insertar datos
extraer_insertar_datos_task = PythonOperator(
    task_id='extraer_insertar_datos_task',
    python_callable=extraer_insertar_datos,
    provide_context=True,  # Necesario para acceder al contexto de Airflow
    dag=dag,
)

# Tarea para validar datos
def validar_datos(**kwargs):
    # Recuperar df_resultado del contexto de Airflow
    ti = kwargs['ti']
    df_resultado = ti.xcom_pull(task_ids='extraer_insertar_datos_task', key='df_resultado')

    # Verificar si df_resultado es None o manejarlo según su tipo
    if df_resultado is None:
        # Manejo cuando df_resultado no está disponible
        return None

    # Verificar si hay datos nulos en el DataFrame
    if df_resultado.isnull().values.any():
        # Acciones a tomar si hay datos nulos
        print("Se encontraron datos nulos. Ejecutar acciones de manejo de nulos aquí.")
        # A futuro, podrías incluirlo en el correo a enviar
    else:
        # Acciones a tomar si no hay datos nulos
        print("No se encontraron datos nulos. Continuar con el flujo normal.")

# Tarea para validar datos
validar_datos_task = PythonOperator(
    task_id='validar_datos_task',
    python_callable=validar_datos,
    provide_context=True,  # Necesario para acceder al contexto de Airflow
    dag=dag,
)

# Obtener las variables de entorno
email = os.environ.get("EMAIL")
password = os.environ.get("PASSWORD_EMAIL")
destinatario_email = os.environ.get("DESTINATARIO_EMAIL")

 # Obtener la conexión a la base de datos
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST")
REDSHIFT_PORT = os.environ.get("REDSHIFT_PORT")
REDSHIFT_DB = os.environ.get("REDSHIFT_DB")
REDSHIFT_USER = os.environ.get("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA = os.environ.get("REDSHIFT_SCHEMA")

cadena_conexion = f"host={REDSHIFT_HOST} port={REDSHIFT_PORT} dbname={REDSHIFT_DB} user={REDSHIFT_USER} password={REDSHIFT_PASSWORD} options='-c search_path={REDSHIFT_SCHEMA}'"

def enviar_correo_exito():
    try:
        # Establecer conexión a la base de datos
        conexion = psycopg2.connect(cadena_conexion)
        cursor = conexion.cursor()

        # Consultar la fecha más reciente de la columna LastUpdated y obtener el resultado
        query = f"SELECT MAX(LastUpdated) FROM {REDSHIFT_SCHEMA}.CurrencyExchange;"
        cursor.execute(query)
        last_updated = cursor.fetchone()[0]

        # Convertir la cadena de fecha obtenida a objeto datetime
        last_updated_date = datetime.strptime(last_updated, '%Y-%m-%d %H:%M:%S').date()

        # Obtener la fecha de hoy
        today = datetime.now().date()

        # Comparar la última fecha actualizada con la fecha de hoy
        if last_updated_date == today:
            # Definir el contenido del correo electrónico para éxito
            html_content = f'<p>La extracción de datos fue exitosa y la última actualización ocurrió el {last_updated}.</p>'
            
   
    # Establecer conexión SMTP y enviar correo electrónico
            with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
                smtp.starttls()
                smtp.login(email, password)
                msg = f'Subject: Extracción de datos exitosa\n\n{html_content}'
                smtp.sendmail(email, destinatario_email, msg)
                print('Correo electrónico enviado exitosamente.')
        else:
            print('No se enviaron correos electrónicos porque la fecha más reciente no coincide con la fecha de hoy.')
    except Exception as e:
        print(f'Error al enviar correo electrónico: {e}')

enviar_correo_exito_task = PythonOperator(
    task_id='enviar_correo_exito_task',
    python_callable=enviar_correo_exito,
    dag=dag
)


extraer_insertar_datos_task >> validar_datos_task >> enviar_correo_exito_task 