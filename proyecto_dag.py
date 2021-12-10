import os
import pandas as pd
import airflow
from airflow import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from structlog import get_logger

logger = get_logger()

#creo el diccionario de las columnas para las 3 tablas que cargaremos con la estructura ya creada en MySQL
COLUMNS1 = {
    "Province/State": "Province_State",
    "Country/Region": "Country_Region",
    "Lat": "Lat",
    "Long": "Lon",
    "Date": "Date",
    "Confirmed": "confirmed",
}

COLUMNS2 = {
    "Province/State": "Province_State",
    "Country/Region": "Country_Region",
    "Lat": "Lat",
    "Long": "Lon",
    "Date": "Date",
    "Deaths": "Deaths",
}

COLUMNS3 = {
    "Province/State": "Province_State",
    "Country/Region": "Country_Region",
    "Lat": "Lat",
    "Long": "Lon",
    "Date": "Date",
    "Recovered": "Recovered",
}
#creacion del DAG
dag = DAG('dag_proyecto', description='primer Dag del proyecto',
          default_args={
              'owner': 'conchita.rainier',
              'depends_on_past': False,
              'max_active_runs': 1,
              'start_date': days_ago(5)
          },
          schedule_interval='0 1 * * *',
          catchup=False)

#procesamiento de los archivos, creo los paths y cargo en dataframes la informacion
def process_file(**kwargs):
    connection = MySqlHook('mysql_default').get_sqlalchemy_engine()
    file_path_confirmed = f"{FSHook('fs_default').get_path()}/time_series_covid19_confirmed_global.csv"
    df_confirmed = pd.read_csv(file_path_confirmed)
    file_path_deaths = f"{FSHook('fs_default').get_path()}/time_series_covid19_deaths_global.csv"
    df_deaths = pd.read_csv(file_path_deaths)
    file_path_recovered = f"{FSHook('fs_default').get_path()}/time_series_covid19_recovered_global.csv"
    df_recovered = pd.read_csv(file_path_recovered)
    #logger.info(df_recovered)

#creo las fechas
    dates = df_confirmed.columns[4:]

#realizo el melt para realizar el transpose de las fechas
    confirmed_df_long = df_confirmed.melt(
        id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'],
        value_vars=dates,
        var_name='Date',
        value_name='Confirmed'
    )
    deaths_df_long = df_deaths.melt(
        id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'],
        value_vars=dates,
        var_name='Date',
        value_name='Deaths'
    )
    recovered_df_long = df_recovered.melt(
        id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'],
        value_vars=dates,
        var_name='Date',
        value_name='Recovered'
    )
    logger.info(f'termino los melts')
    logger.info(confirmed_df_long.tail())

    #limpio los valores nulos por ceros
    recovered_df_long['Recovered'] = recovered_df_long['Recovered'].fillna(0)
    logger.info(f'primer na')
    deaths_df_long['Deaths'] = deaths_df_long['Deaths'].fillna(0)
    logger.info(f'segundo na')
    confirmed_df_long['Confirmed'] = confirmed_df_long['Confirmed'].fillna(0)
    logger.info(f'tercer na')
    #df_confirmed['Date'] = pd.to_datetime(df_confirmed['Date'], errors='coerce')

    #renombro las columnas con la estructura de la base de datos
    confirmed_df_long = confirmed_df_long.rename(columns=COLUMNS1)
    recovered_df_long = recovered_df_long.rename(columns=COLUMNS1)
    deaths_df_long = deaths_df_long.rename(columns=COLUMNS1)
    #logs para debuggear
    logger.info(f'renombro columnas')
    logger.info(confirmed_df_long.tail())
    logger.info(f'insert')

    #Creo la primer conexion a base de datos donde elimino las tablas de confirmados e inserto el DF
    with connection.begin() as transaction:
        transaction.execute('DELETE FROM test.confirmed where 1=1')
        transaction.execute('DELETE FROM test.resumen_confirmed where 1=1')
        confirmed_df_long.to_sql('confirmed', con=transaction, schema='test', if_exists='append', index=False)

    logger.info(f'Records inserted {len(confirmed_df_long.index)}')

    # Creo la segunda conexion a base de datos donde elimino las tablas de recuperados e inserto el DF
    with connection.begin() as transaction:
        transaction.execute('DELETE FROM test.recovered where 1=1')
        transaction.execute('DELETE FROM test.resumen_recovered where 1=1')
        recovered_df_long.to_sql('recovered', con=transaction, schema='test', if_exists='append', index=False)

    logger.info(f'Records inserted {len(recovered_df_long.index)}')

    # Creo la segunda conexion a base de datos donde elimino las tablas de las meurtes e inserto el DF
    with connection.begin() as transaction:
        transaction.execute('DELETE FROM test.deaths where 1=1')
        transaction.execute('DELETE FROM test.resumen_deaths where 1=1')
        deaths_df_long.to_sql('deaths', con=transaction, schema='test', if_exists='append', index=False)

    logger.info(f'Records inserted {len(deaths_df_long.index)}')

    #por comodidad y reducir el tiempo de desarrollo se realizo un Storeprocedure en la base de datos en el que se realizan el resto de transformaciones
    #lo mandamos a llamar desde aqui.
    with connection.begin() as transaction:
        transaction.execute('CALL TRANSFORMACION()')



operador = PythonOperator(task_id='process_file',
                          dag=dag,
                          python_callable=process_file,
                          provide_context=True)
#configuro los sensores de los archivos
sensor1 = FileSensor(task_id="file_sensor_covid_confirmed",
                     dag=dag,
                     filepath='time_series_covid19_confirmed_global.csv',
                     fs_conn_id='fs_default',
                     poke_interval=5,
                     timeout=60)

sensor2 = FileSensor(task_id="file_sensor_covid_deaths",
                     dag=dag,
                     filepath='time_series_covid19_deaths_global.csv',
                     fs_conn_id='fs_default',
                     poke_interval=5,
                     timeout=60)

sensor3 = FileSensor(task_id="file_sensor_covid_recovered",
                     dag=dag,
                     filepath='time_series_covid19_recovered_global.csv',
                     fs_conn_id='fs_default',
                     poke_interval=5,
                     timeout=60)
#realizo el arbol del dag, donde indico que los 3 archivos son los que deben finalizar para mandar a llamar al operador.
[sensor1, sensor2, sensor3] >> operador
