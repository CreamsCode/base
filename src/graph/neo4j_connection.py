from neo4j import GraphDatabase
import logging

class Neo4JConnection:
    def __init__(self, uri, username, password):
        """Inicializa una conexión a Neo4j usando la URI, usuario y contraseña."""
        self.logger = logging.getLogger(__name__)
        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            self.logger.info("Connected to Neo4J at %s", uri)
        except Exception as e:
            self.logger.error(f"Failed to create a Neo4J driver: {e}")
            raise e

    def close(self):
        """Cierra la conexión al controlador de Neo4j."""
        if self.driver:
            self.driver.close()
            self.logger.info("Neo4J connection closed.")

    def query(self, cypher_query, parameters=None):
        """Ejecuta una consulta Cypher en Neo4J."""
        with self.driver.session() as session:
            return session.run(cypher_query, parameters).data()