import cx_Oracle
from config.settings import Config

# Inicialização do Oracle Instant Client
cx_Oracle.init_oracle_client(lib_dir="/Users/ewertongoncalves/Downloads/instantclient_23_3")

def get_oracle_connection():
    """Estabelece conexão com o banco Oracle."""
    # Constrói o DSN com SERVICE_NAME
    dsn = cx_Oracle.makedsn(
        host=Config.ORACLE_HOST,
        port=Config.ORACLE_PORT,
        service_name=Config.ORACLE_SERVICE_NAME
    )

    return cx_Oracle.connect(
        user=Config.ORACLE_USER,
        password=Config.ORACLE_PASSWORD,
        dsn=dsn
    )
