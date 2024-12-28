from src.datalake.connection import MongoDBConnectionManager

class MongoDataLoader:
    def __init__(self, connection_manager: MongoDBConnectionManager):
        """
        Inicializa el cargador de datos con un administrador de conexiones.
        """
        self.connection_manager = connection_manager

    def load_words_with_usage(self):
        """
        Combina las colecciones `words` y `word_usage` en una estructura optimizada.
        """
        words_collection = self.connection_manager.get_or_create_collection("words")
        usage_collection = self.connection_manager.get_or_create_collection("word_usage")

        word_data = {}
        for word in words_collection.find():
            word_id = word["_id"]
            word_data[word["word"]] = {
                "length": word["length"],
                "usages": []
            }

            # Agregar los usos (word_usage) relacionados
            for usage in usage_collection.find({"word_id": word_id}):
                word_data[word["word"]]["usages"].append({
                    "book": usage["book"],
                    "author": usage["author"],
                    "frequency": usage["frequency"]
                })

        return word_data
