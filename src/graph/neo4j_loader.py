from src.graph.neo4j_connection import Neo4JConnection
from hazelcast import HazelcastClient
from src.graph.data_parser import DataParser


class Neo4JLoader:
    def __init__(self, uri, user, password, hazelcast_host="127.0.0.1:5701"):
        """
        Inicializa el cargador Neo4J con conexión a Neo4J y Hazelcast.
        :param uri: URI para conectar a Neo4J
        :param user: Usuario para autenticar en Neo4J
        :param password: Contraseña para autenticar en Neo4J
        :param hazelcast_host: Dirección del cluster de Hazelcast
        """
        # Conexión a Neo4J
        self.neo4j_conn = Neo4JConnection(uri, user, password)

        # Conexión a Hazelcast
        self.hazelcast_client = HazelcastClient(cluster_members=[hazelcast_host])
        self.words_map = self.hazelcast_client.get_map("words_map").blocking()
        self.graph_map = self.hazelcast_client.get_map("graph_map").blocking()

    def close(self):
        """
        Cierra las conexiones abiertas.
        """
        self.neo4j_conn.close()
        self.hazelcast_client.shutdown()

    def process_graph_map(self):
        """
        Procesa las claves y listas del `graph_map` de Hazelcast.
        Asegura que los nodos existen y crea conexiones entre ellos.
        """
        for key, value in self.graph_map.entry_set():
            # `key` es el nodo principal
            # `value` es una lista de nodos relacionados
            with self.neo4j_conn.driver.session() as session:
                # Asegurarse de que el nodo principal está creado
                session.execute_write(self.ensure_node_exists, {"word": key})

                for related_node in value:
                    # Asegurarse de que cada nodo relacionado está creado
                    session.execute_write(self.ensure_node_exists, {"word": related_node})

                    # Crear una relación entre el nodo principal y el nodo relacionado
                    session.execute_write(self.create_relationship, {"source": key, "target": related_node})

    @staticmethod
    def ensure_node_exists(tx, doc):
        """
        Crea un nodo si no existe.
        :param tx: Transacción de Neo4J
        :param doc: Diccionario con los datos del nodo (ej. {"word": "example"})
        """
        query = """
        MERGE (w:Word {word: $word})
        """
        tx.run(query, word=doc["word"])

    @staticmethod
    def create_relationship(tx, doc):
        """
        Crea una relación entre dos nodos.
        :param tx: Transacción de Neo4J
        :param doc: Diccionario con los datos de la relación (ej. {"source": "node1", "target": "node2"})
        """
        query = """
        MATCH (source:Word {word: $source})
        MATCH (target:Word {word: $target})
        MERGE (source)-[:RELATED_TO]->(target)
        """
        tx.run(query, source=doc["source"], target=doc["target"])
