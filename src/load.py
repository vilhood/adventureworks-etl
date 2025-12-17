import pandas as pd
from sqlalchemy import types

"""
Módulo de carga (Load) del proceso ETL.

Este script se encarga de cargar DataFrames transformados en tablas del
Data Warehouse (PostgreSQL), utilizando SQLAlchemy y pandas. Incluye
control opcional de tipos de datos para dimensiones específicas como DimDate.
"""


def load_data(df, table_name, engine, if_exists='append', schema='public'):
    """
Carga un DataFrame en una tabla de la base de datos destino (PostgreSQL).

Args:
    df (pd.DataFrame): DataFrame a cargar.
    table_name (str): Nombre de la tabla destino.
    engine (sqlalchemy.engine.Engine): Engine de conexión al Data Warehouse.
    if_exists (str, optional): Acción si la tabla existe ('fail', 'replace', 'append').
    schema (str, optional): Esquema destino. Por defecto 'public'.

Raises:
    Exception: Si ocurre un error durante la carga.
"""

    print(f"Cargando datos en la tabla: {schema}.{table_name}...")

    dtype_map = {}


    if table_name.lower() == 'dimdate':
        dtype_map = {
            'datekey': types.Integer(),
            'fulldate': types.Date(),
            'daynumberofweek': types.SmallInteger(),
            'englishdaynameofweek': types.String(10),
            'daynumberofmonth': types.SmallInteger(),
            'daynumberofyear': types.SmallInteger(),
            'weeknumberofyear': types.SmallInteger(),
            'englishmonthname': types.String(10),
            'monthnumberofyear': types.SmallInteger(),
            'calendarquarter': types.SmallInteger(),
            'calendaryear': types.SmallInteger()
        }

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=1000,
            dtype=dtype_map if dtype_map else None
        )
        print(f"Se cargaron exitosamente {len(df)} filas en {schema}.{table_name}.")
    except Exception as e:
        print(f"Error al cargar datos en {schema}.{table_name}: {e}")


if __name__ == '__main__':
    print("Script de carga de datos listo para PostgreSQL.")
