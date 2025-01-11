from src.datalake.connection import MongoDBConnectionManager
from src.datalake.ingestor import DataIngestor
from src.collector import WebScraper

# Configuración de MongoDB
connection_manager = MongoDBConnectionManager(uri= "mongodb://root:example@localhost:27017/"
, db_name="graph_words_db")
connection_manager.connect()

# Inicializar DataIngestor
data_ingestor = DataIngestor(connection_manager)

# Procesar datos
scraper = WebScraper(number=1)
processed_words = scraper.start()

# Ingestar datos
data_ingestor.process_and_insert_words(processed_words)

# Cerrar conexión
connection_manager.close()

