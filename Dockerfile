# Usa la imagen base con tu versión de Python
FROM python:3.11

# Instala dependencias del sistema necesarias para ODBC y agrega bash para SSH
RUN apt-get update && apt-get install -y \
    unixodbc \
    unixodbc-dev \
    libodbc1 \
    build-essential \
    curl \
    gnupg2 \
    bash

# Agrega el repositorio de Microsoft
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Instala el driver de Microsoft ODBC
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Limpieza de cache
RUN apt-get clean

# Establece el directorio de trabajo
WORKDIR /app

# Copia todos los archivos del proyecto al contenedor
COPY . .

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto que usará FastAPI
EXPOSE 8000

# Ejecuta la aplicación FastAPI
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
