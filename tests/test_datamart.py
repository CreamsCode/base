from src.datamart import DataMart, MongoDataLoader
from src.datalake.connection import MongoDBConnectionManager

# Configuración de MongoDB y Hazelcast
MONGO_URI = "mongodb://root:example@localhost:27017/"
DB_NAME = "graph_words_db"
HAZELCAST_CONFIG = {}

connection_manager = MongoDBConnectionManager(uri=MONGO_URI, db_name=DB_NAME)

try:

    connection_manager.connect()

    mongo_loader = MongoDataLoader(connection_manager)
    datamart = DataMart(mongo_loader, HAZELCAST_CONFIG)


    datamart.build_datamart()

    print("Words Map in Hazelcast:")
    for word, data in datamart.words_map.entry_set():
        print(f"{word}: {data}")

    print("\nGraph Map in Hazelcast:")
    for word, connections in datamart.graph_map.entry_set():
        print(f"{word}: {connections}")

finally:

    connection_manager.close()
