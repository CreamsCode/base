from pymongo import MongoClient
from pymongo.collection import Collection

class MongoDBConnectionManager:
    def __init__(self, uri: str = "mongodb://localhost:27017/", db_name: str = "word_analysis"):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None

    def connect(self):
        """
        Establece la conexión con MongoDB y selecciona la base de datos.
        """
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        print(f"Connected to MongoDB at {self.uri}")

    def get_collection(self, collection_name: str) -> Collection:
        """
        Retorna una colección específica dentro de la base de datos.
        """
        if not self.db:
            raise Exception("Database connection not initialized. Call connect() first.")
        return self.db[collection_name]

    def close(self):
        """
        Cierra la conexión con MongoDB.
        """
        if self.client:
            self.client.close()
            print("MongoDB connection closed.")
