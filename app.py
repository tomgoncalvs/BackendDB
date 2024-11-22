from flask import Flask
from app.routes import routes
from config.settings import Config
from app.dboracle import get_oracle_connection
from app.mongodb import get_mongo_client

def test_database_connections():
    """Testa a conectividade com os bancos de dados Oracle e MongoDB."""
    try:
        # Testar conexão com o Oracle
        print("Testando conexão com o Oracle...")
        with get_oracle_connection() as oracle_conn:
            cursor = oracle_conn.cursor()
            cursor.execute("SELECT 1 FROM dual")  # Consulta simples para validar conexão
            print("Conexão com o Oracle: OK")
    
        # Testar conexão com o MongoDB
        print("Testando conexão com o MongoDB...")
        mongo_client = get_mongo_client()
        mongo_client.list_collection_names()  # Tenta listar coleções para validar conexão
        print("Conexão com o MongoDB: OK")

    except Exception as e:
        print(f"Erro ao conectar aos bancos de dados: {e}")
        raise  # Levanta o erro para impedir a inicialização da aplicação

def create_app():
    app = Flask(__name__)
    app.config["ENV"] = Config.FLASK_ENV
    app.config["DEBUG"] = Config.FLASK_DEBUG

    # Registrar rotas
    app.register_blueprint(routes)

    return app

if __name__ == "__main__":
    try:
        # Testar conexões antes de iniciar a API
        test_database_connections()

        # Criar e iniciar a aplicação Flask
        app = create_app()
        app.run()
    except Exception as e:
        print(f"Falha ao iniciar a aplicação: {e}")
