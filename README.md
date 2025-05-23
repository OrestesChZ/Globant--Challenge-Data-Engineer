# Data Engineering Challenge - Globant

This project involves building a REST API to handle data migration from CSV files into an SQL database, performing batch transactions, and providing endpoints to explore and analyze the data.

## Project Structure

- `app.py`: The main FastAPI application where the API logic is implemented.
- `Dockerfile`: Used to containerize the application for deployment.
- `requirements.txt`: Lists the Python dependencies required to run the application.
- `apiTest.py`: A script for testing the API endpoints.
- `architecture.puml`: Contains the architecture diagram for the solution.
- `README.md`: This file.

# Azure Web App
The application is deployed to Azure App Service. You can access the API at the following URL:

[API URL](https://fastapiorestes7-hjarejg6gpdggdbh.brazilsouth-01.azurewebsites.net/docs)

# Power BI reporting
Stakeholder can access this link to see the results of the querys: https://app.powerbi.com/links/3hXa-R8gY1?ctid=edef029c-489f-4136-8f10-fe213603f574&pbi_source=linkShare

## Setup

### Clone the Repository

To get started, clone the repository to your local machine:

```bash
git clone https://github.com/your-username/fastapi-globant.git
cd fastapi-globant


## Prerequisites

- Python 3.11 or higher
- Docker (if containerizing the application)
- Azure SQL Database (or another SQL database of your choice)

## Install Dependencies

#To install the necessary dependencies, run:

pip install -r requirements.txt


##Running the Application Locally

#To run the FastAPI application locally without Docker, simply run:

uvicorn app:app --reload


## Docker Setup

#If you prefer to run the application in a Docker container, #build the Docker image using:

docker build -t fastapi-globant .

docker run -p 8000:8000 fastapi-globant

```
## 🧱 Arquitectura del Proyecto

A continuación, se muestra el diagrama de arquitectura de la solución:

![Diagrama de Arquitectura](/arquitecturaChallenge.png)

![image](https://github.com/user-attachments/assets/9ba5088a-b375-4090-b870-a05f7d268a89)

![image](https://github.com/user-attachments/assets/1127e364-6a4d-4db1-820d-0a380539d3f9)






