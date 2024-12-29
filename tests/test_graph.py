import logging
from src.graph.neo4j_loader import Neo4JLoader
from hazelcast import HazelcastClient

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar conexiones
neo4j_loader = Neo4JLoader(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="password",
    hazelcast_host="127.0.0.1:5701"
)

# Conexión a Hazelcast
hazelcast_client = HazelcastClient(cluster_members=["127.0.0.1:5701"])
words_map = hazelcast_client.get_map("words_map").blocking()
graph_map = hazelcast_client.get_map("graph_map").blocking()

class TestGraph:
    @staticmethod
    def run():
        logger.info("Starting Graph test...")

        try:
            # Transferir palabras con longitud de 3 letras desde el Datamart a Neo4J
            logger.info("Transferring 3-letter words to Neo4J...")
            for word, data in words_map.entry_set():
                if data["length"] == 3:  # Filtro de longitud
                    logger.info(f"Processing word: {word} -> {data}")
                    neo4j_loader.transfer_words()  # Inserta nodos de palabras

            logger.info("3-letter words successfully transferred to Neo4J.")

        except Exception as e:
            logger.error(f"An error occurred during the graph test: {e}")

        finally:
            # Cerrar conexiones
            logger.info("Closing Neo4J connection...")
            neo4j_loader.close()
            hazelcast_client.shutdown()
            logger.info("Test completed.")


# Ejecutar la prueba
if __name__ == "__main__":
    TestGraph.run()
