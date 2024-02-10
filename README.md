# Proyecto de Extracción y Envío de Cambios de Divisas

Este proyecto realiza la extracción de tipos de cambio de divisas de una API externa, inserta estos datos en una base de datos y envía notificaciones por correo electrónico en caso de éxito.
Tener las divisas actualizadas es fundamental para garantizar la precisión, la transparencia y la eficacia en las operaciones financieras y la toma de decisiones en el ámbito empresarial y de inversiones.

## Estructura del Proyecto

El proyecto se organiza de la siguiente manera:

- **`dags/`**: Carpeta que contiene el código del DAG.
  - `extraccion_envio_cambios_divisas.py`: Archivo Python que define el DAG para Airflow.
- **`requirements.txt`**: Archivo que lista todas las dependencias del proyecto.
- **`README.md`**: Este archivo. Proporciona una descripción general del proyecto y su estructura.


## Configuración

* Antes de ejecutar el DAG, asegúrate de configurar correctamente las variables de entorno necesarias, como las credenciales de la base de datos y del correo electrónico.
* Algunos servicios de email como Gmail requieren que habilites el acceso a aplicaciones con una contraseña de aplicación. 

## Ejecución

Una vez configuradas las variables de entorno, puedes ejecutar el DAG utilizando Airflow. Asegúrate de que el servicio de Airflow esté en funcionamiento y coloca el archivo del DAG en la carpeta dags/.
El archivo .env deberá tener el siguiente formato:
    # .env
    POSTGRES_USER=ejemplo-user
    POSTGRES_PASSWORD=ejemplo-password
    POSTGRES_DB=ejemplo-db
    POSTGRES_HOST=ejemplo-host
    POSTGRES_PORT=5432
    EMAIL=ejemplo-email
    PASSWORD_EMAIL=ejemplo-password
    DESTINATARIO_EMAIL=ejemplo-destinatario-email

## Flujo de Trabajo

El flujo de trabajo del proyecto es el siguiente:

El DAG se ejecuta diariamente.
La tarea extraer_insertar_datos_task extrae los datos de tipos de cambio de una API externa y los inserta en la base de datos.
La tarea validar_datos_task verifica si los datos extraídos son válidos.
La tarea enviar_correo_exito_task compara la última fecha de extracción con la fecha actual y envía un correo electrónico notificando si se obtuvieron datos actualizados o no.
