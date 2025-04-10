from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import APIKeyHeader
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from io import StringIO, BytesIO
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime, text
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

    if blob_name == "jobs.csv":
        column_names = ["Id", "Job"]
    elif blob_name == "departments.csv":
        column_names = ["Id", "Department"]
    elif blob_name == "hired_employees.csv":
        column_names = ["Id", "Name", "DateTime", "Department_Id", "Job_Id"]
    else:
        raise HTTPException(status_code=400, detail=f"Archivo {blob_name} no reconocido.")

    df = pd.read_csv(StringIO(csv_data), header=None, names=column_names)

    # Limpiar las columnas DateTime y convertir a formato adecuado
    df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')  # Asegurarse de que sea datetime
    df.dropna(subset=['DateTime'], inplace=True)  # Eliminar filas con valores NaT (invalidados)

    return df

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

def create_table_employees():
    metadata = MetaData()
    table = Table("Employees", metadata,
        Column('Id', Integer, primary_key=True),
        Column('Name', String(250), nullable=False),
        Column('DateTime', DateTime, nullable=True),  # Cambiado a DATETIME
        Column('Department_Id', Integer, nullable=False),
        Column('Job_Id', Integer, nullable=True),
        Column('DateCreate', DateTime, nullable=False, server_default=text('GETDATE()')),
        Column('DateUpdate', DateTime, nullable=True)
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(get_db_connection())}")
    metadata.drop_all(engine, [table])
    metadata.create_all(engine)

def create_table_departments():
    metadata = MetaData()
    table = Table("Departments", metadata,
        Column('Id', Integer, primary_key=True),
        Column('Department', String(100), nullable=False)
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(get_db_connection())}")
    metadata.drop_all(engine, [table])
    metadata.create_all(engine)

def create_table_jobs():
    metadata = MetaData()
    table = Table("Jobs", metadata,
        Column('Id', Integer, primary_key=True),
        Column('Job', String(100), nullable=False)
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(get_db_connection())}")
    metadata.drop_all(engine, [table])
    metadata.create_all(engine)

@app.post("/create-tables", dependencies=[Depends(verify_api_key)])
def create_all_tables():
    create_table_employees()
    create_table_departments()
    create_table_jobs()
    return {"message": "Todas las tablas fueron creadas correctamente."}

@app.post("/upload-csv", dependencies=[Depends(verify_api_key)])
def upload_csv_to_sql(
    blob_name: str,
    table_name: str,
    new: bool = False,
    container_name: str = "csv-files"  # Valor por defecto
):
    try:
        # Obtener el cliente del blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{blob_name}.csv")

        # Verificar si el blob existe
        try:
            blob_client.get_blob_properties()  # Esto verifica si el blob existe
        except ResourceNotFoundError:
            raise HTTPException(status_code=404, detail=f"Blob '{blob_name}.csv' no encontrado en el contenedor '{container_name}'")

        # Leer el CSV desde el blob sin encabezados
        stream = StringIO(blob_client.download_blob().readall().decode('utf-8'))
        df = pd.read_csv(stream, header=None)  # Sin encabezados

        # Limpiar comas vacías y eliminar las columnas vacías
        df = df.dropna(axis=1, how='all')  # Elimina las columnas vacías (si existen)
        df = df.dropna(axis=0, how='any')  # Elimina las filas con valores nulos en cualquier columna

        # Asignar los nombres de las columnas manualmente (según el archivo hired_employees.csv)
        df.columns = ["Id", "Name", "DateTime", "Department_Id", "Job_Id"]

        # Limpiar los valores de las columnas numéricas y garantizar que todos los datos sean válidos
        for column in df.columns:
            if df[column].dtype == 'float64' or df[column].dtype == 'int64':
                # Reemplazar valores no numéricos o NaN por un valor por defecto (0 o NULL dependiendo de lo que desees)
                df[column] = pd.to_numeric(df[column], errors='coerce')  # Convierte a numérico, reemplazando errores por NaN
                df[column] = df[column].fillna(0)  # O usa otro valor como 0 o NULL

        # Asegurarse de que las columnas de fecha sean válidas
        df['DateTime'] = pd.to_datetime(df['DateTime'], errors='coerce')  # Convertir la columna DateTime
        df = df.dropna(subset=['DateTime'])  # Eliminar filas con valores NaT

        # Conectar a la base de datos usando get_db_connection
        with pyodbc.connect(get_db_connection()) as conn:
            cursor = conn.cursor()

            # Crear tabla si es necesario
            if new:
                columns = ", ".join([f"{col} NVARCHAR(MAX)" for col in df.columns])
                columns += ", DateCreate DATETIME, DateUpdate DATETIME"
                cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
                cursor.execute(f"CREATE TABLE {table_name} ({columns});")
                if 'Id' in df.columns:
                    cursor.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT pk_{table_name} PRIMARY KEY (Id);")

            # Construir y ejecutar la consulta MERGE para evitar duplicados
            for _, row in df.iterrows():
                col_names = ', '.join(df.columns)
                param_placeholders = ', '.join(['?' for _ in df.columns])
                params = list(row)

                # Usar un alias único y evitar conflictos con los nombres de las etiquetas
                merge_sql = f"""
                    MERGE INTO {table_name} AS target
                    USING (VALUES ({', '.join(['?' for _ in df.columns])})) AS source ({', '.join(df.columns)})
                    ON target.Id = source.Id
                    WHEN MATCHED THEN
                        UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in df.columns if col.lower() != 'id'])},
                            target.DateUpdate = GETDATE()
                    WHEN NOT MATCHED THEN
                        INSERT ({col_names}, DateCreate, DateUpdate)
                        VALUES ({', '.join(['?' for _ in df.columns])}, GETDATE(), GETDATE());
                """
                cursor.execute(merge_sql, tuple(params + params))  # Duplicar los params porque se usan dos veces

            conn.commit()
        return {"message": f"Datos de {blob_name}.csv cargados en {table_name} correctamente."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


