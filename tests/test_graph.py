import logging
from src.graph.neo4j_loader import Neo4JLoader
from hazelcast import HazelcastClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

neo4j_loader = Neo4JLoader(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="password",
    hazelcast_host="127.0.0.1:5701"
)

hazelcast_client = HazelcastClient(cluster_members=["127.0.0.1:5701"])
words_map = hazelcast_client.get_map("words_map").blocking()
graph_map = hazelcast_client.get_map("graph_map").blocking()

class TestGraph:
    @staticmethod
    def run():
        logger.info("Starting Graph test...")

        try:
            logger.info("Transferring words to Neo4J...")
            
            neo4j_loader.process_graph_map()

            logger.info("Words successfully transferred to Neo4J.")

        except Exception as e:
            logger.error(f"An error occurred during the graph test: {e}")

        finally:
            logger.info("Closing Neo4J connection...")
            neo4j_loader.close()
            hazelcast_client.shutdown()
            logger.info("Test completed.")

if __name__ == "__main__":
    TestGraph.run()
