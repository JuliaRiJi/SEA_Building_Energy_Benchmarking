"""
PONER DESCRIPCIÓN SCRIPT
"""

#pip install snowflake-connector-python
#Librería sodapy para Python
#pip install sodapy

#Librerías
import snowflake.connector
import pandas as pd
from sodapy import Socrata
import json

# EXTRACCIÓN DE DATOS DESDE API SOCRATA
# Token de la aplicación
app_token = "1hm0BsBO9WQQkqPoWSHPWqRNR"

# Establecer los parámetros de autenticación y la URL de la API
client = Socrata("data.seattle.gov", app_token)

# Identificador del dataset de Socrata para Building_Energy_Benchmarking
dataset_id = "h7rm-fz6m"  # Dataset de 2015

# Realizar una consulta a la API y recuperar los datos
results = client.get(dataset_id, limit=4000)

# Convertirlos a un DataFrame de pandas
Building_Energy_Benchmarking_df = pd.DataFrame.from_records(results)

# Renombrar las columnas del DataFrame de 2018 para que coincidan con las de la tabla de Snowflake
# Renombrar las columnas del DataFrame para que coincidan con las columnas existentes en la tabla de Snowflake
Building_Energy_Benchmarking_df.rename(columns={
    'seattlebuildingid': 'osebuildingid',
    'ghgemissions_metrictonsco2e': 'totalghgemissions',
    'ghgemissionsintensity_kgco2e_ft2': 'ghgemissionsintensity',
    'secondlargestpropertyusetypegfa': 'secondlargestpropertyuse'
}, inplace=True)

# Reemplazar NaN con None en el DataFrame
Building_Energy_Benchmarking_df = Building_Energy_Benchmarking_df.where(pd.notnull(Building_Energy_Benchmarking_df), None)

# Suponiendo que 'df' es tu DataFrame de Pandas
Building_Energy_Benchmarking_df['location'] = Building_Energy_Benchmarking_df['location'].apply(lambda x: json.dumps(x))

#CONEXIÓN A SNOWFLAKE
# Configuración de parámetros de conexión en Snowflake
snowflake_config = {
    'user': 'JULIARI',
    'password': 'Mipelusa2012!',
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
table_name = 'Building_Energy_Benchmarking'

# Verificar si la tabla ya existe en Snowflake
cursor = conn.cursor()
cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
if cursor.fetchone():
    # La tabla ya existe en Snowflake
    cursor.execute(f"SHOW COLUMNS IN {table_name}")
    existing_columns = [col[2].lower() for col in cursor.fetchall()]
    print(existing_columns)

    # Comparar las columnas del DataFrame con las columnas existentes en la tabla de Snowflake
    new_columns = [col for col in Building_Energy_Benchmarking_df.columns if col.lower() not in existing_columns]
    print(new_columns)

    if new_columns:
        # Si hay nuevas columnas en el DataFrame, se agregan a la tabla de Snowflake
        for new_column in new_columns:
            dtype = Building_Energy_Benchmarking_df[new_column].dtype
            if dtype == 'object':
                data_type = 'VARCHAR(100)'  # Para columnas de tipo objeto (cadenas de caracteres)
            else:
                data_type = 'NUMBER'  # Para columnas numéricas
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {new_column} {data_type}")
            # Insertar registros correspondientes a las nuevas columnas
            for index, row in Building_Energy_Benchmarking_df.iterrows():
                insert_data_sql = f"INSERT INTO {table_name} ({', '.join(Building_Energy_Benchmarking_df.columns)}) VALUES ({', '.join(['%s'] * len(Building_Energy_Benchmarking_df.columns))})"
                cursor.execute(insert_data_sql, tuple(row))
    else:
        for index, row in Building_Energy_Benchmarking_df.iterrows():
            insert_data_sql = f"INSERT INTO {table_name} ({', '.join(Building_Energy_Benchmarking_df.columns)}) VALUES ({', '.join(['%s'] * len(Building_Energy_Benchmarking_df.columns))})"
            cursor.execute(insert_data_sql, tuple(row))

        conn.commit()  # Confirmar la inserción de datos
else:
    # La tabla no existe, crearla basada en las columnas del DataFrame
    create_table_sql = f"CREATE TABLE {table_name} ("
    for column in Building_Energy_Benchmarking_df.columns:
        # Determinar el tipo de datos de la columna en función del tipo en el DataFrame
        dtype = Building_Energy_Benchmarking_df[column].dtype
        if dtype == 'object':
            data_type = 'VARCHAR(100)'  # Para columnas de tipo objeto (cadenas de caracteres)
        else:
            data_type = 'NUMBER'  # Para columnas numéricas
        create_table_sql += f"{column} {data_type}, "
    create_table_sql = create_table_sql.rstrip(', ') + ")"
    cursor.execute(create_table_sql)
    # Insertar datos en la nueva tabla
    for index, row in Building_Energy_Benchmarking_df.iterrows():
        insert_data_sql = f"INSERT INTO {table_name} ({', '.join(Building_Energy_Benchmarking_df.columns)}) VALUES ({', '.join(['%s'] * len(Building_Energy_Benchmarking_df.columns))})"
        cursor.execute(insert_data_sql, tuple(row))

    conn.commit()  # Confirmar la inserción de datos

# Cerrar la conexión
conn.close()