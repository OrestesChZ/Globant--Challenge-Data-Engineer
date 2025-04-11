from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import APIKeyHeader
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from azure.core.exceptions import ResourceNotFoundError
from io import StringIO, BytesIO
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime, text, ForeignKey, ForeignKeyConstraint, inspect
import urllib
import fastavro

app = FastAPI()

API_KEY = "mi_clave_api_unica_12345"
api_key_header = APIKeyHeader(name="X-API-KEY")

def verify_api_key(api_key: str = Depends(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Clave API no válida")

AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=storageglobantorestes;AccountKey=UfcoWhoLzquWPzpT/aP3eksWYFOxz3sUFkxqYkOzogeO/gq3iAP4Ln4dfN7SDKUV//BsP78LDXst+ASttnEIaQ==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_name = "csv-files"

def get_db_connection():
    server = 'orestes-sqlserver.database.windows.net'
    database = 'globantdb'
    username = 'adminochz'
    password = 'ores20tes00@'
    driver = '{ODBC Driver 18 for SQL Server}'
    connection_string = f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    return connection_string

def execute_query(query: str):
    conn = pyodbc.connect(get_db_connection())
    result = pd.read_sql(query, conn)
    conn.close()
    return result

@app.get("/")
def read_root():
    return {"message": "API para el desafío de Data Engineer en Globant. Usa /docs para probar los endpoints."}


##############################

# Función para verificar si una tabla existe
def check_if_table_exists(engine, table_name):
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(get_db_connection())}")
    inspector = inspect(engine)
    return table_name in inspector.get_table_names()

#####################################################

@app.post("/create-tables", dependencies=[Depends(verify_api_key)])
def create_all_tables():
    try:
        messages = []

        if create_table_departments():
            messages.append("Tabla 'Departments' creada.")
        else:
            messages.append("Tabla 'Departments' ya existía.")

        if create_table_jobs():
            messages.append("Tabla 'Jobs' creada.")
        else:
            messages.append("Tabla 'Jobs' ya existía.")

        if create_table_employees():
            messages.append("Tabla 'Employees' creada.")
        else:
            messages.append("Tabla 'Employees' ya existía.")

        return {"message": messages}
    
    except Exception as e:
        return {"error": f"Ocurrió un error: {str(e)}"}

####

metadata = MetaData()

def create_table_departments():
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(get_db_connection())}")
    if check_if_table_exists(engine, "Departments"):
        print("La tabla 'Departments' ya existe. No se creará nuevamente.")
        return False
    table = Table("Departments", metadata,
        Column('Id', Integer, primary_key=True, autoincrement=False),
        Column('Department', String(100), nullable=False)
    )
    metadata.create_all(engine)
    return True

####

def create_table_jobs():
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(get_db_connection())}")
    if check_if_table_exists(engine, "Jobs"):
        print("La tabla 'Jobs' ya existe. No se creará nuevamente.")
        return False
    table = Table("Jobs", metadata,
        Column('Id', Integer, primary_key=True, autoincrement=False),
        Column('Job', String(100), nullable=False)
    )
    metadata.create_all(engine)
    return True

###

def create_table_employees():
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(get_db_connection())}")
    if check_if_table_exists(engine, "Employees"):
        print("La tabla 'Employees' ya existe. No se creará nuevamente.")
        return False
    table = Table("Employees", metadata,
        Column('Id', Integer, primary_key=True, autoincrement=False),
        Column('Name', String(250), nullable=False),
        Column('DateTime', DateTime, nullable=True),
        Column('Department_Id', Integer, ForeignKey('Departments.Id'), nullable=False),
        Column('Job_Id', Integer, ForeignKey('Jobs.Id'), nullable=True),
        Column('DateCreate', DateTime, nullable=False, server_default=text('GETDATE()')),
        Column('DateUpdate', DateTime, nullable=True)
    )
    metadata.create_all(engine)
    return True

#################################################

@app.post("/upload-csv", dependencies=[Depends(verify_api_key)]) 
def upload_csv_to_sql(
    blob_name: str,
    table_name: str,
    container_name: str = "csv-files"
):
    try:
        # Recortar espacios del blob_name
        blob_name = blob_name.strip()

        # Cliente de Blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{blob_name}.csv")

        # Verificar existencia del blob
        try:
            blob_client.get_blob_properties()
        except ResourceNotFoundError:
            raise HTTPException(status_code=404, detail=f"Blob '{blob_name}.csv' no encontrado en el contenedor '{container_name}'")

        # Leer CSV sin encabezado
        stream = StringIO(blob_client.download_blob().readall().decode('utf-8'))

        # Asignar columnas manualmente
        if blob_name == "jobs":
            column_names = ["Id", "Job"]
        elif blob_name == "departments":
            column_names = ["Id", "Department"]
        elif blob_name == "hired_employees":
            column_names = ["Id", "Name", "DateTime", "Department_Id", "Job_Id"]
        else:
            raise HTTPException(status_code=400, detail=f"Archivo {blob_name} no reconocido.")

        df = pd.read_csv(stream, header=None, names=column_names)
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='any', inplace=True)

        # Convertir DateTime si existe
        if "DateTime" in df.columns:
            df["DateTime"] = pd.to_datetime(df["DateTime"], errors='coerce')
            df.dropna(subset=["DateTime"], inplace=True)

        with pyodbc.connect(get_db_connection()) as conn:
            cursor = conn.cursor()

            # Verificar si la tabla ya existe
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?
            """, (table_name,))
            table_exists = cursor.fetchone()[0] == 1

            # Si la tabla no existe, lanzar error
            if not table_exists:
                raise HTTPException(status_code=400, detail=f"La tabla '{table_name}' no existe. Debes crearla primero usando el endpoint /create-tables.")

            # Obtener columnas reales de la tabla
            cursor.execute("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?
            """, (table_name,))
            existing_columns = {row[0] for row in cursor.fetchall()}

            # Preparar columnas
            cols_to_insert = [col for col in df.columns if col in existing_columns]
            cols_to_update = [col for col in cols_to_insert if col.lower() != "id"]

            use_date_create = "DateCreate" in existing_columns
            use_date_update = "DateUpdate" in existing_columns

            for _, row in df.iterrows():
                insert_cols = cols_to_insert.copy()
                update_set = [f"target.{col} = source.{col}" for col in cols_to_update]

                if use_date_create and use_date_update:
                    insert_cols += ["DateCreate", "DateUpdate"]
                    update_set.append("target.DateUpdate = GETDATE()")

                source_placeholders = ', '.join(['?' for _ in insert_cols])
                source_cols = ', '.join(insert_cols)
                update_clause = ', '.join(update_set)

                merge_sql = f"""
                    MERGE INTO {table_name} AS target
                    USING (VALUES ({source_placeholders})) AS source ({source_cols})
                    ON target.Id = source.Id
                    WHEN MATCHED THEN UPDATE SET {update_clause}
                    WHEN NOT MATCHED THEN INSERT ({source_cols}) VALUES ({source_placeholders});
                """

                params = [row[col] for col in cols_to_insert]
                if use_date_create and use_date_update:
                    params += [datetime.now(), datetime.now()]
                cursor.execute(merge_sql, params + params)

            conn.commit()
        return {"message": f"Datos de {blob_name}.csv cargados correctamente en '{table_name}'."}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


##############################################


@app.get("/employees-by-job-and-department")
def employees_by_job_and_department():
    query = """
        SELECT Job_Id, Department_Id,
               SUM(CASE WHEN DateTime BETWEEN '2021-01-01' AND '2021-03-31' THEN 1 ELSE 0 END) AS Q1,
               SUM(CASE WHEN DateTime BETWEEN '2021-04-01' AND '2021-06-30' THEN 1 ELSE 0 END) AS Q2,
               SUM(CASE WHEN DateTime BETWEEN '2021-07-01' AND '2021-09-30' THEN 1 ELSE 0 END) AS Q3,
               SUM(CASE WHEN DateTime BETWEEN '2021-10-01' AND '2021-12-31' THEN 1 ELSE 0 END) AS Q4
        FROM Employees
        WHERE DateTime BETWEEN '2021-01-01' AND '2021-12-31'
        GROUP BY Job_Id, Department_Id
        ORDER BY Job_Id, Department_Id
    """
    result = execute_query(query)
    return {"data": result.to_dict(orient='records')}

@app.get("/departments-above-average")
def departments_above_average():
    query = """
        WITH DepartmentHiring AS (
            SELECT Department_Id, COUNT(*) AS EmployeesHired
            FROM Employees
            WHERE DateTime BETWEEN '2021-01-01' AND '2021-12-31'
            GROUP BY Department_Id
        ),
        AverageHiring AS (
            SELECT AVG(EmployeesHired) AS AvgHiring
            FROM DepartmentHiring
        )
        SELECT d.Id AS Department_Id, d.Department, dh.EmployeesHired
        FROM DepartmentHiring dh
        JOIN Departments d ON dh.Department_Id = d.Id
        WHERE dh.EmployeesHired > (SELECT AvgHiring FROM AverageHiring)
        ORDER BY dh.EmployeesHired DESC
    """
    result = execute_query(query)
    return {"data": result.to_dict(orient='records')}


def read_csv_from_blob(blob_name: str) -> pd.DataFrame:
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container_name)

    try:
        blob_client = container_client.get_blob_client(blob_name)
        download_stream = blob_client.download_blob()
        csv_data = download_stream.readall().decode("utf-8")
    except ResourceNotFoundError:
        raise HTTPException(status_code=404, detail=f"El archivo '{blob_name}' no existe en el contenedor.")

    # Asignar nombres de columnas según el archivo
    if blob_name == "jobs.csv":
        column_names = ["Id", "Job"]
    elif blob_name == "departments.csv":
        column_names = ["Id", "Department"]
    elif blob_name == "hired_employees.csv":
        column_names = ["Id", "Name", "DateTime", "Department_Id", "Job_Id"]
    else:
        raise HTTPException(status_code=400, detail=f"Archivo {blob_name} no reconocido.")

    # Leer CSV sin encabezados y asignar nombres de columnas
    df = pd.read_csv(StringIO(csv_data), header=None, names=column_names)

    # Si existe la columna DateTime, procesarla
    if "DateTime" in df.columns:
        df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')  # Convertir a datetime
        df.dropna(subset=['DateTime'], inplace=True)  # Eliminar filas inválidas

    return df

#########

def backup_table(query, avro_schema, avro_file):
    conn = pyodbc.connect(get_db_connection())
    df = pd.read_sql(query, conn)
    conn.close()

    records = df.to_dict(orient='records')
    avro_buffer = BytesIO()
    fastavro.writer(avro_buffer, avro_schema, records)
    avro_buffer.seek(0)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=avro_file)
    blob_client.upload_blob(avro_buffer, overwrite=True)

    return f'Archivo {avro_file} subido exitosamente al contenedor {container_name}'

######










