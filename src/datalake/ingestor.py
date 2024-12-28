from .models import Word, WordUsage

def insert_word(collection, word):
    """
    Inserta una palabra en la colección `words` si no existe.
    Retorna el ID del documento.
    """
    existing_word = collection.find_one({"word": word.word})
    if existing_word:
        return existing_word["_id"]  # Si ya existe, retorna su ID
    result = collection.insert_one(word.to_dict())
    return result.inserted_id

def insert_word_usage(collection, usage):
    """
    Inserta un uso de palabra en la colección `word_usage`.
    """
    collection.insert_one(usage.to_dict())

def process_and_insert_words(processed_words, words_collection, usage_collection):
    """
    Procesa y guarda las palabras y su uso en las colecciones MongoDB.

    Args:
        processed_words (list): Lista de palabras procesadas con su información.
        words_collection (Collection): Colección de palabras únicas (words).
        usage_collection (Collection): Colección de usos de palabras (word_usage).
    """
    for entry in processed_words:
        # Crear objeto Word
        word_obj = Word(entry["word"], entry["length"])
        # Insertar palabra única o recuperar su ID
        word_id = insert_word(words_collection, word_obj)

        # Crear objeto WordUsage
        usage_obj = WordUsage(
            word_id=word_id,
            book=entry["book"],
            author=entry["author"],
            frequency=entry["frequency"]
        )
        # Insertar el uso de la palabra
        insert_word_usage(usage_collection, usage_obj)

