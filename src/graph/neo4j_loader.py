from graph.neo4j_connection import Neo4JConnection
from graph.data_parser import DataParser

class Neo4JLoader:
    def __init__(self, uri, user, password, mongo_loader):
        """
        Inicializa el cargador Neo4J con conexión a MongoDB y Neo4J.
        """
        self.neo4j_conn = Neo4JConnection(uri, user, password)
        self.mongo_loader = mongo_loader

    def transfer_words(self):
        """Transfiere palabras desde MongoDB a Neo4J."""
        word_docs = self.mongo_loader.load_words()
        with self.neo4j_conn.driver.session() as session:
            for doc in word_docs:
                parsed_doc = DataParser.parse_word(doc)
                session.write_transaction(self.create_word, parsed_doc)

    def transfer_word_usage(self):
        """Transfiere relaciones de uso de palabras desde MongoDB a Neo4J."""
        word_usage_docs = self.mongo_loader.load_word_usage()
        with self.neo4j_conn.driver.session() as session:
            for doc in word_usage_docs:
                parsed_doc = DataParser.parse_word_usage(doc)
                session.write_transaction(self.create_word_usage_relationship, parsed_doc)

    @staticmethod
    def create_word(tx, doc):
        """Crea un nodo de palabra en Neo4J."""
        query = """
        MERGE (w:Word {word: $word})
        SET w.length = $length
        """
        tx.run(query, word=doc["word"], length=doc["length"])

    @staticmethod
    def create_word_usage_relationship(tx, doc):
        """Crea relaciones de uso de palabra entre Word y Book en Neo4J."""
        query = """
        MATCH (w:Word {word: $word})
        MERGE (b:Book {name: $book, author: $author})
        MERGE (w)-[:USED_IN {frequency: $frequency}]->(b)
        """
        tx.run(query, word=doc["word_id"], book=doc["book"], author=doc["author"], frequency=doc["frequency"])
