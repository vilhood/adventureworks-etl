# 📊 Proyecto ETL – Data Warehouse con Python y PostgreSQL

Este proyecto implementa un proceso **ETL (Extract, Transform, Load)** en **Python**, cuyo objetivo es cargar una **Bodega de Datos (Data Warehouse)** a partir de una base de datos **OLTP AdventureWorks** usando **PostgreSQL**.

El resultado final es un Data Warehouse poblado con **dimensiones** y **dos datamarts de ventas**:
- Ventas por Internet
- Ventas a Revendedores

---

## 🧠 Arquitectura General

PostgreSQL (OLTP)
↓
EXTRACT
↓
TRANSFORM
↓
PostgreSQL (DW)


Todo el proceso es **automatizado** y se ejecuta con un solo comando.

---

## 🗂️ Estructura del Proyecto

ETL/
│
├── src/
│ ├── extract.py # Extracción de datos desde OLTP
│ ├── transform.py # Transformaciones y modelo dimensional
│ ├── load.py # Carga de datos en el Data Warehouse
│ ├── utils.py # Conexión a PostgreSQL
│
├── config/
│ └── config.ini # Configuración de bases de datos (NO se sube a Git)
│
├── main.py # Orquestador del proceso ETL
├── requirements.txt # Dependencias del proyecto
├── .gitignore
└── README.md

---

## ⚙️ Requisitos

- Python 3.9 o superior
- PostgreSQL
- pgAdmin (recomendado)
- Git

---

## 🔧 Instalación

### 1️ Clonar el repositorio
```bash
git clone https://github.com/vilhood/adventureworks-etl
cd ETL
```
### 2️ Crear entorno virtual
```bash
python -m venv venv
```
### 3️ Activar entorno virtual
```bash 
.\venv\Scripts\Activate.ps1
```
### 4️ Instalar dependencias
```bash 
pip install -r requirements.txt
```

### Configuracion:

Crear el archivo:

config/config.ini


Con el siguiente contenido (ajustar credenciales):

[SOURCE_DB]
host=localhost
port=5432
database=adventureworksOLTP
username=postgres
password=tu contraseña

[DESTINATION_DB]
host=localhost
port=5432
database=nombre de la base de datos destino
username=postgres
password=tu constraseña