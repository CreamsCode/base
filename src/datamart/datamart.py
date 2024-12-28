import logging
from hazelcast import HazelcastClient
from .mongodataloader import MongoDataLoader

# Configurar el logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataMart:
    def __init__(self, mongo_loader: MongoDataLoader, hazelcast_config: dict):
        """
        Inicializa el DataMart con un cargador de MongoDB y la configuración de Hazelcast.
        """
        self.mongo_loader = mongo_loader
        try:
            self.hazelcast_client = HazelcastClient(**hazelcast_config)
            self.words_map = self.hazelcast_client.get_map("words_map").blocking()
            self.graph_map = self.hazelcast_client.get_map("graph_map").blocking()
            logger.info("Hazelcast client initialized successfully.")
        except Exception as e:
            logger.error("Failed to initialize Hazelcast client: %s", e)
            raise

    def build_datamart(self):
        """
        Coordina la carga y procesamiento de datos:
        1. Carga los datos desde MongoDB al mapa `words_map`.
        2. Procesa los datos en `words_map` y genera conexiones en `graph_map`.
        """
        logger.info("Starting to build the DataMart...")

        # Paso 1: Cargar datos desde MongoDB
        logger.info("Loading data from MongoDB into Hazelcast `words_map`...")
        word_data = self.mongo_loader.load_words_with_usage()
        for word, data in word_data.items():
            self.words_map.put(word, data)
            logger.debug(f"Added word to words_map: {word}")

        logger.info("Data loaded into Hazelcast `words_map`.")

        # Paso 2: Procesar datos para el grafo
        logger.info("Processing words_map to generate graph_map...")
        for word in self.words_map.key_set():
            connected_words = self.find_connected_words(word)
            self.graph_map.put(word, connected_words)
            logger.debug(f"Processed word '{word}' with connections: {connected_words}")

        logger.info("Graph data processed and stored in Hazelcast `graph_map`.")

    def find_connected_words(self, word):
        """
        Encuentra palabras conectadas en `words_map` (difieren en una sola letra).
        """
        logger.debug(f"Finding connected words for: {word}")
        connected = []
        for candidate_word in self.words_map.key_set():
            if self.is_one_letter_different(word, candidate_word):
                connected.append(candidate_word)
        return connected

    @staticmethod
    def is_one_letter_different(word1, word2):
        """
        Verifica si dos palabras difieren en solo una letra.
        """
        if len(word1) != len(word2):
            return False
        diff_count = sum(1 for a, b in zip(word1, word2) if a != b)
        return diff_count == 1
