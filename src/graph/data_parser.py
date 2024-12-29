class DataParser:
    @staticmethod
    def parse_word(doc):
        """Transforma un documento de palabra de MongoDB para ser compatible con Neo4J."""
        return {
            "word": doc.get("word", ""),
            "length": doc.get("length", 0)
        }

    @staticmethod
    def parse_word_usage(doc):
        """Transforma un documento de uso de palabra de MongoDB para ser compatible con Neo4J."""
        return {
            "word_id": doc.get("word_id", ""),
            "book": doc.get("book", ""),
            "author": doc.get("author", ""),
            "frequency": doc.get("frequency", 0)
        }
    