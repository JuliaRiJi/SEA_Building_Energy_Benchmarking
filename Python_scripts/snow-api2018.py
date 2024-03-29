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
dataset_id = "ypch-zswb"  # Dataset de 2018

# Realizar una consulta a la API y recuperar los datos
results = client.get(dataset_id, limit=4000)

# Convertirlos a un DataFrame de pandas
Building_Energy_Benchmarking_df = pd.DataFrame.from_records(results)

# Renombrar las columnas del DataFrame de 2018 para que coincidan con las de la tabla de Snowflake
Building_Energy_Benchmarking_df.rename(columns={
    'ose_building_id': 'osebuildingid',
    'data_year': 'datayear',
    'building_name': 'buildingname',
    'building_type': 'buildingtype',
    'zip_code': 'zipcode',
    'council_district_code': 'councildistrictcode',
    'year_built': 'yearbuilt',
    'numberof_floors': 'numberoffloors',
    'property_gfa_total': 'propertygfatotal',
    'property_gfa_parking': 'propertygfaparking',
    'primary_property_type': 'epapropertytype',
    'energystar_score': 'energystarscore',
    'largest_property_use_type': 'largestpropertyusetype',
    'largest_property_use_type_1': 'largestpropertyusetypegfa',
    'compliance_issue': 'complianceissue',
    'compliance_status': 'compliancestatus',
    'ghg_emissions_intensity': 'ghgemissionsintensity',
    'total_ghg_emissions': 'totalghgemissions',
    'second_largest_property_use': 'secondlargestpropertyusetype',
    'second_largest_property_use_1': 'secondlargestpropertyuse',
    'third_largest_property_use': 'thirdlargestpropertyusetype',
    'third_largest_property_use_1': 'thirdlargestpropertyusetypegfa'
}, inplace=True)

# Reemplazar NaN con None en el DataFrame
Building_Energy_Benchmarking_df = Building_Energy_Benchmarking_df.where(pd.notnull(Building_Energy_Benchmarking_df), None)

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
table_name = 'Building_Energy_Benchmarking'

# Verificar si la tabla ya existe en Snowflake
cursor = conn.cursor()
cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
if cursor.fetchone():
    # La tabla ya existe en Snowflake
    cursor.execute(f"SHOW COLUMNS IN {table_name}")
    existing_columns = [col[2].lower() for col in cursor.fetchall()]

    # Comparar las columnas del DataFrame con las columnas existentes en la tabla de Snowflake
    new_columns = [col for col in Building_Energy_Benchmarking_df.columns if col.lower() not in existing_columns]

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
