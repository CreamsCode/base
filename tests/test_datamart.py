import logging
from src.datamart import DataMart, MongoDataLoader
from src.datalake.connection import MongoDBConnectionManager

# Configuración de MongoDB y Hazelcast
MONGO_URI = "mongodb://root:example@localhost:27017/"
DB_NAME = "graph_words_db"
HAZELCAST_CONFIG = {
    "cluster_members": ["127.0.0.1:5701"],
    "connection_timeout": 10,
}

# Configurar el logger para capturar mensajes del test
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_datamart():
    logger.info("Starting DataMart test...")
    connection_manager = MongoDBConnectionManager(uri=MONGO_URI, db_name=DB_NAME)

    try:
        logger.info("Connecting to MongoDB...")
        connection_manager.connect()

        # Crear el cargador de datos desde MongoDB
        mongo_loader = MongoDataLoader(connection_manager)

        # Crear la instancia de DataMart
        datamart = DataMart(mongo_loader, HAZELCAST_CONFIG)

        # Construir el datamart
        logger.info("Building the DataMart...")
        datamart.build_datamart()

        # Verificar los datos en Hazelcast
        logger.info("Words Map in Hazelcast:")
        for word, data in datamart.words_map.entry_set():
            logger.info(f"{word}: {data}")

        logger.info("\nGraph Map in Hazelcast:")
        for word, connections in datamart.graph_map.entry_set():
            logger.info(f"{word}: {connections}")

    finally:
        logger.info("Closing MongoDB connection...")
        connection_manager.close()
        logger.info("Test completed.")

if __name__ == "__main__":
    test_datamart()
