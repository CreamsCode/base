class DataParser:
    @staticmethod
    def parse_word(word,data):
        """
        Transforma un elemento del mapa de palabras de Hazelcast para Neo4J.
        :param data: Diccionario con datos de la palabra
        :return: Diccionario transformado
        """
        return {
            "word": data.get("word", word),
            "length": data.get("length", 0)
        }

    @staticmethod
    def parse_word_usage(data):
        """
        Transforma un elemento del mapa de relaciones de Hazelcast para Neo4J.
        :param data: Diccionario con datos de la relación
        :return: Diccionario transformado
        """
        return {
            "word_id": data.get("word_id", ""),
            "book": data.get("book", ""),
            "author": data.get("author", ""),
            "frequency": data.get("frequency", 0)
        }
