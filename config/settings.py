from dotenv import load_dotenv
import os

# Carregar variáveis do .env
load_dotenv()

class Config:
    # Configurações do Oracle
    ORACLE_HOST = os.getenv("ORACLE_HOST")
    ORACLE_PORT = os.getenv("ORACLE_PORT", "1521")  # Porta padrão como fallback
    ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME")
    ORACLE_USER = os.getenv("ORACLE_USER")
    ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")


    # Configurações do MongoDB
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB = os.getenv("MONGO_DB")

    # Configurações do Flask
    FLASK_ENV = os.getenv("FLASK_ENV")
    FLASK_DEBUG = os.getenv("FLASK_DEBUG", "0") == "1"
