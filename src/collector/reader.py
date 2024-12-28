import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import re

class Reader:
    def __init__(self):
        nltk.download('stopwords', quiet=True)
        nltk.download('punkt', quiet=True)
        self.stopwords_eng = set(stopwords.words('english'))

    def preprocessing(self, texto):
        """
        Procesa el texto de un libro:
        - Limpia caracteres especiales.
        - Tokeniza el texto en palabras.
        - Elimina stopwords y palabras no alfabéticas.
        Retorna una lista de palabras procesadas.
        """
        texto = re.sub(r'[^\w\s]', ' ', texto)  # Limpieza de caracteres especiales
        texto = texto.lower()

        tokens = word_tokenize(texto)
        words = [word for word in tokens if word.isalpha() and word not in self.stopwords_eng]
        return words

    def process_book_data(self, title, author, word_frequencies):
        """
        Crea un JSON estructurado con palabras, frecuencias y metadatos.
        """
        return [
            {
                "word": word,
                "length": len(word),
                "frequency": freq,
                "book": title.strip(),
                "author": author.strip()
            }
            for word, freq in word_frequencies.items()
        ]