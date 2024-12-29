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

    def transfer_words(self):
        """
        Transfiere palabras desde Hazelcast a Neo4J.
        """
        for word, data in self.words_map.entry_set():
            parsed_word = DataParser.parse_word(word,data)
            with self.neo4j_conn.driver.session() as session:
                session.execute_write(self.create_word, parsed_word)

    def transfer_word_usage(self):
        """
        Transfiere relaciones de uso de palabras desde Hazelcast a Neo4J.
        """
        for relationship_key, relationship_data in self.graph_map.entry_set():
            parsed_usage = DataParser.parse_word_usage(relationship_data)
            with self.neo4j_conn.driver.session() as session:
                session.execute_write(self.create_word_usage_relationship, parsed_usage)

    @staticmethod
    def create_word(tx, doc):
        """
        Crea un nodo de palabra en Neo4J, solo si la longitud de la palabra es igual a 3.
        :param tx: Transacción de Neo4J
        :param doc: Diccionario con los datos de la palabra
        """
        if doc["length"] == 3:  # Filtrar solo palabras de longitud 3
            query = """
            MERGE (w:Word {word: $word})
            ON CREATE SET w.length = $length
            """
            tx.run(query, word=doc["word"], length=doc["length"])


    @staticmethod
    def create_word_usage_relationship(tx, doc):
        """
        Crea relaciones de uso de palabra entre Word y Book en Neo4J.
        """
        query = """
        MATCH (w:Word {word: $word})
        MERGE (b:Book {name: $book, author: $author})
        MERGE (w)-[:USED_IN {frequency: $frequency}]->(b)
        """
        tx.run(query, word=doc["word_id"], book=doc["book"], author=doc["author"], frequency=doc["frequency"])
