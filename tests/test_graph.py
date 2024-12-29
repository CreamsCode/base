import unittest
from src.graph.neo4j_loader import Neo4JLoader
from src.datamart import MongoDataLoader

class TestNeo4JLoader(unittest.TestCase):
    def setUp(self):
        """
        Configura las conexiones necesarias para MongoDB y Neo4J.
        """
        self.mongo_loader = MongoDataLoader("mongodb://localhost:27017/", "word_analysis")
        self.neo4j_loader = Neo4JLoader(
            uri="bolt://localhost:7687",
            user="neo4j",
            password="password",
            mongo_loader=self.mongo_loader
        )

    def test_transfer_words(self):
        """Prueba la transferencia de palabras desde MongoDB a Neo4J."""
        self.neo4j_loader.transfer_words()

    def test_transfer_word_usage(self):
        """Prueba la transferencia de relaciones de uso de palabras desde MongoDB a Neo4J."""
        self.neo4j_loader.transfer_word_usage()

    def tearDown(self):
        """Cierra las conexiones después de las pruebas."""
        self.neo4j_loader.neo4j_conn.close()
