from pymongo import MongoClient
from config.settings import Config

def get_mongo_client():
    client = MongoClient(Config.MONGO_URI)
    return client[Config.MONGO_DB]
