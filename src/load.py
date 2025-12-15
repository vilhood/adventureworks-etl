import pandas as pd
from sqlalchemy import types


def load_data(df, table_name, engine, if_exists='append', schema='public'):
    """
    Carga un DataFrame en una tabla específica en la base de datos de destino (PostgreSQL).

    Argumentos:
        df (pd.DataFrame): DataFrame a cargar
        table_name (str): Nombre de la tabla destino
        engine (sqlalchemy.engine.Engine): Motor SQLAlchemy del DW
        if_exists (str): 'fail', 'replace' o 'append'
        schema (str): Esquema destino (PostgreSQL usa 'public' por defecto)
    """
    print(f"Cargando datos en la tabla: {schema}.{table_name}...")

    dtype_map = {}

    # Blueprint SOLO para DimDate (opcional pero recomendado)
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
