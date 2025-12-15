import configparser
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import os


def get_db_engine(db_type='SOURCE_DB'):
    """
    Crea y retorna un motor de SQLAlchemy para PostgreSQL.
    db_type puede ser: SOURCE_DB o DESTINATION_DB
    """
    config = configparser.ConfigParser()
    config_path = os.path.join(
        os.path.dirname(__file__), '..', 'config', 'config.ini'
    )
    config.read(config_path)

    if db_type not in config:
        raise ValueError(f"No existe la configuración para {db_type}")

    db_config = config[db_type]

    host = db_config['host']
    port = db_config['port']
    database = db_config['database']
    user = db_config['user']
    password = db_config['password']

    try:
        connection_url = URL.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
        )

        engine = create_engine(connection_url)

        # Probar conexión
        with engine.connect() as connection:
            print(f"Conectado exitosamente a {db_type}: {database}")

        return engine

    except Exception as e:
        print(f"Error al conectar a la base de datos {db_type}: {e}")
        return None


if __name__ == '__main__':
    print("Probando conexiones a PostgreSQL...")
    source_engine = get_db_engine('SOURCE_DB')
    dest_engine = get_db_engine('DESTINATION_DB')
    print("Prueba de conexión completada.")
