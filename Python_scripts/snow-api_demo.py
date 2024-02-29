"""
Este script se encarga de la extracción de datos desde la API de Socrata del gobierno de Seattle, así como su carga en una tabla de la base de datos Snowflake.

El proceso consta de dos partes principales:

1. Extracción de datos desde la API de Socrata:
   - Se establece la comunicación con la API utilizando un token de acceso proporcionado.
   - Se desarrollan scripts en Python para interactuar con la API y extraer los datos necesarios año por año.
   - Cada conjunto de datos anual se almacena en un DataFrame de Pandas para su posterior procesamiento.

2. Conexión y carga de datos en Snowflake:
   - Se configuran los parámetros de conexión con la base de datos Snowflake.
   - Se realiza la conexión a Snowflake utilizando la biblioteca snowflake-connector-python.
   - Se verifica si la tabla ya existe en Snowflake y se procede a su creación si no es así.
   - Se comparan las columnas del DataFrame con las existentes en la tabla de Snowflake y se agregan las nuevas columnas si es necesario.
   - Se insertan los datos en la tabla correspondiente en Snowflake, asegurando la coherencia entre las estructuras de datos.
"""

#pip install snowflake-connector-python
#Librería sodapy para Python
#pip install sodapy

#Librerías
import snowflake.connector
import pandas as pd
from sodapy import Socrata

# EXTRACCIÓN DE DATOS DESDE API SOCRATA
# Token de la aplicación
app_token = "1hm0BsBO9WQQkqPoWSHPWqRNR"

# Establecer los parámetros de autenticación y la URL de la API
client = Socrata("data.seattle.gov", app_token)

# Identificador del dataset de Socrata para Building_Energy_Benchmarking
dataset_id = "5sxi-iyiy" # Dataset de 2022
#dataset_id = "bfsh-nrm6"  # Dataset de 2021
#dataset_id = "auez-gz8p"  # Dataset de 2020
#dataset_id = "3th6-ticf"  # Dataset de 2019
#dataset_id = "qxjw-iwsh"  # Dataset de 2017
#dataset_id = "2bpz-gwpy"  # Dataset de 2016

# Realizar una consulta a la API y recuperar los datos
results = client.get(dataset_id, limit=4000)

# Convertirlos a un DataFrame de pandas
Building_Energy_Benchmarking_df = pd.DataFrame.from_records(results)

# Reemplazar NaN con None en el DataFrame
Building_Energy_Benchmarking_df = Building_Energy_Benchmarking_df.where(pd.notnull(Building_Energy_Benchmarking_df), None)

# Seleccionar solo los primeros 10 registros del DataFrame para la demo
Building_Energy_Benchmarking_df_subset = Building_Energy_Benchmarking_df.head(10)

#CONEXIÓN A SNOWFLAKE
# Configuración de parámetros de conexión en Snowflake
snowflake_config = {
    'user': 'JULIARI',
    'password': '*****',
    'account': 'yfuthya-ig52821',
    'warehouse': 'COMPUTE_WH',
    'database': 'BRONZE_DB',
    'schema': 'API_SOCRATA',
    'role': 'ACCOUNTADMIN'
}

# Conexión a Snowflake
conn = snowflake.connector.connect(**snowflake_config)

#INGESTA DE DATOS DESDE API A SNOWFLAKE
# Definir el nombre de la tabla
table_name = 'Building_Energy_Benchmarking_demo'

# Verificar si la tabla ya existe en Snowflake
cursor = conn.cursor()
cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
if cursor.fetchone():
    # La tabla ya existe en Snowflake
    cursor.execute(f"SHOW COLUMNS IN {table_name}")
    existing_columns = [col[2].lower() for col in cursor.fetchall()]

    # Comparar las columnas del DataFrame con las columnas existentes en la tabla de Snowflake
    new_columns = [col for col in Building_Energy_Benchmarking_df_subset.columns if col.lower() not in existing_columns]

    if new_columns:
        # Si hay nuevas columnas en el DataFrame, se agregan a la tabla de Snowflake
        for new_column in new_columns:
            dtype = Building_Energy_Benchmarking_df_subset[new_column].dtype
            if dtype == 'object':
                data_type = 'VARCHAR(100)'  # Para columnas de tipo objeto (cadenas de caracteres)
            else:
                data_type = 'NUMBER'  # Para columnas numéricas
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {new_column} {data_type}")
            # Insertar registros correspondientes a las nuevas columnas
            for index, row in Building_Energy_Benchmarking_df_subset.iterrows():
                insert_data_sql = f"INSERT INTO {table_name} ({', '.join(Building_Energy_Benchmarking_df_subset.columns)}) VALUES ({', '.join(['%s'] * len(Building_Energy_Benchmarking_df_subset.columns))})"
                cursor.execute(insert_data_sql, tuple(row))
    else:
        for index, row in Building_Energy_Benchmarking_df_subset.iterrows():
            insert_data_sql = f"INSERT INTO {table_name} ({', '.join(Building_Energy_Benchmarking_df_subset.columns)}) VALUES ({', '.join(['%s'] * len(Building_Energy_Benchmarking_df_subset.columns))})"
            cursor.execute(insert_data_sql, tuple(row))

        conn.commit()  # Confirmar la inserción de datos
else:
    # La tabla no existe, crearla basada en las columnas del DataFrame
    create_table_sql = f"CREATE TABLE {table_name} ("
    for column in Building_Energy_Benchmarking_df_subset.columns:
        # Determinar el tipo de datos de la columna en función del tipo en el DataFrame
        dtype = Building_Energy_Benchmarking_df_subset[column].dtype
        if dtype == 'object':
            data_type = 'VARCHAR(100)'  # Para columnas de tipo objeto (cadenas de caracteres)
        else:
            data_type = 'NUMBER'  # Para columnas numéricas
        create_table_sql += f"{column} {data_type}, "
    create_table_sql = create_table_sql.rstrip(', ') + ")"
    cursor.execute(create_table_sql)
    # Insertar datos en la nueva tabla
    for index, row in Building_Energy_Benchmarking_df_subset.iterrows():
        insert_data_sql = f"INSERT INTO {table_name} ({', '.join(Building_Energy_Benchmarking_df_subset.columns)}) VALUES ({', '.join(['%s'] * len(Building_Energy_Benchmarking_df_subset.columns))})"
        cursor.execute(insert_data_sql, tuple(row))

    conn.commit()  # Confirmar la inserción de datos

# Cerrar la conexión
conn.close()
