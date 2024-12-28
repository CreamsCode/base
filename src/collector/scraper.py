import requests
import re
import random
import logging
from collections import Counter
from .reader import Reader


class WebScraper:
    def __init__(self, number):
        self.n = number
        self.reader = Reader()
        logging.basicConfig(level=logging.INFO)

    def extract_metadata(self, content):
        """
        Extrae los metadatos del contenido del libro.
        """
        metadata_pattern = r"(.+?)\*\*\* START OF (THE|THIS) PROJECT GUTENBERG EBOOK"
        metadata_match = re.search(metadata_pattern, content, re.DOTALL)

        if metadata_match:
            metadata = metadata_match.group(1).strip()
            title_match = re.search(r"Title:\s*(.+)", metadata)
            author_match = re.search(r"Author:\s*(.+)", metadata)
            language_match = re.search(r"Language:\s*(.+)", metadata)

            title = title_match.group(1).strip() if title_match else f"Unknown Title"
            author = author_match.group(1).strip() if author_match else "Unknown Author"
            language = language_match.group(1).strip() if language_match else "Unknown Language"

            return title, author, language
        return None, None, None

    def extract_content(self, content):
        """
        Extrae el contenido del libro desde el marcador de inicio.
        """
        content_pattern = r"\*\*\* START OF (THE|THIS) PROJECT GUTENBERG EBOOK(.*)"
        content_match = re.search(content_pattern, content, re.DOTALL)
        return content_match.group(2).strip() if content_match else None

    def start(self):
        """
        Realiza el scraping de libros, procesa las palabras y retorna una lista de objetos JSON
        con las palabras, frecuencias y metadatos del libro.
        """
        logging.info("Starting WebScraper...")
        i = 0
        all_word_data = []

        while i < self.n:
            # Generar una URL aleatoria
            num_str = str(random.randint(1, 70000))
            url = f"https://www.gutenberg.org/cache/epub/{num_str}/pg{num_str}.txt"

            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()

                content = response.text

                # Extraer metadatos
                title, author, language = self.extract_metadata(content)

                if language and language.lower() == "english":
                    # Extraer contenido
                    ebook_content = self.extract_content(content)

                    if ebook_content:
                        # Procesar palabras
                        processed_words = self.reader.preprocessing(ebook_content)

                        # Calcular frecuencias
                        word_frequencies = Counter(processed_words)

                        # Crear datos con metadatos
                        book_data = self.reader.process_book_data(
                            title=title, author=author, word_frequencies=word_frequencies
                        )

                        all_word_data.extend(book_data)
                        logging.info(
                            f"Book '{title}' processed successfully with {len(book_data)} unique words."
                        )
                        i += 1
                    else:
                        logging.warning(f"Content missing for book '{title}'. Skipping.")
                else:
                    logging.warning(f"Book skipped: Not in English or missing metadata. URL: {url}")

            except requests.RequestException as e:
                logging.error(f"Error fetching URL {url}: {e}")

        logging.info("Scraping completed.")
        return all_word_data
