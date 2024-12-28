from src.datalake.connection import MongoDBConnectionManager
from src.datalake.ingestor import DataIngestor
from src.collector import WebScraper

# Configuración de MongoDB
connection_manager = MongoDBConnectionManager(uri="mongodb://localhost:27017/", db_name="word_analysis")
connection_manager.connect()

# Obtener colecciones
words_collection = connection_manager.get_collection("words")
usage_collection = connection_manager.get_collection("word_usage")

# Procesar datos
scraper = WebScraper(number=2)
processed_words = scraper.start()

# Ingestar datos
data_ingestor = DataIngestor(words_collection, usage_collection)
data_ingestor.process_and_insert_words(processed_words)

# Cerrar conexión
connection_manager.close()
