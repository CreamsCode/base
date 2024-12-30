from Levenshtein import distance as levenshtein_distance
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
        self.neo4j_conn = Neo4JConnection(uri, user, password)

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
        Asegura que los nodos existen y crea conexiones entre ellos, con pesos basados en la frecuencia promedio.
        """
        for key, value in self.graph_map.entry_set():
            with self.neo4j_conn.driver.session() as session:
                session.execute_write(self.ensure_node_exists, {"word": key})

                for related_node in value:
                    session.execute_write(self.ensure_node_exists, {"word": related_node})

                    weight = self.calculate_frequency_weight(key, related_node)

                    session.execute_write(self.create_relationship, {"source": key, "target": related_node, "weight": weight})

    @staticmethod
    def ensure_node_exists(tx, doc):
        """
        Crea un nodo si no existe.
        :param tx: Transacción de Neo4J
        :param doc: Diccionario con los datos del nodo (ej. {"word": "example"}).
        """
        query = """
        MERGE (w:Word {word: $word})
        """
        tx.run(query, word=doc["word"])

    @staticmethod
    def create_relationship(tx, doc):
        """
        Crea una relación única entre nodos en Neo4J, asegurándose de que la relación sea no direccional y con peso.
        :param tx: Transacción de Neo4J.
        :param doc: Diccionario con los datos de la relación (ej. {"source": "word1", "target": "word2", "weight": 0.5}).
        """
        source, target = sorted([doc["source"], doc["target"]])

        query = """
        MATCH (source:Word {word: $source})
        MATCH (target:Word {word: $target})
        MERGE (source)-[r:RELATED_TO]->(target)
        ON CREATE SET r.weight = $weight
        """
        tx.run(query, source=source, target=target, weight=doc["weight"])

    def calculate_frequency_weight(self, word1, word2):
        """
        Calcula el peso basado en la frecuencia promedio de dos palabras.
        :param word1: Primera palabra.
        :param word2: Segunda palabra.
        :return: Peso basado en la frecuencia promedio.
        """
        freq1 = self.get_word_frequency(word1)
        freq2 = self.get_word_frequency(word2)

        return (freq1 + freq2) / 2

    def get_word_frequency(self, word):
        """
        Obtiene la frecuencia de una palabra desde el mapa `words_map`.
        :param word: La palabra para la que se obtendrá la frecuencia.
        :return: Frecuencia total de la palabra en todos los usos, o 0 si no se encuentra.
        """
        for key, value in self.words_map.entry_set():
            if key == word: 
                if "usages" in value:
                    return sum([usage.get("frequency", 0) for usage in value.get("usages", [])])
                else:
                    return value.get("frequency", 0)  
        return 0