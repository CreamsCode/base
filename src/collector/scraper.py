import requests
import re
import random
import logging
from collections import Counter
from .reader import Reader


class WebScraper:
    def __init__(self):
        self.reader = Reader()
        logging.basicConfig(level=logging.INFO)

    def extract_metadata(self, content):
        metadata_pattern = r"(.+?)\*\*\* START OF (THE|THIS) PROJECT GUTENBERG EBOOK"
        metadata_match = re.search(metadata_pattern, content, re.DOTALL)

        if metadata_match:
            metadata = metadata_match.group(1).strip()
            title_match = re.search(r"Title:\s*(.+)", metadata)
            author_match = re.search(r"Author:\s*(.+)", metadata)
            language_match = re.search(r"Language:\s*(.+)", metadata)

            title = title_match.group(1).strip() if title_match else "Unknown Title"
            author = author_match.group(1).strip() if author_match else "Unknown Author"
            language = language_match.group(1).strip() if language_match else "Unknown Language"

            return title, author, language
        return None, None, None

    def extract_content(self, content):
        content_pattern = r"\*\*\* START OF (THE|THIS) PROJECT GUTENBERG EBOOK(.*)"
        content_match = re.search(content_pattern, content, re.DOTALL)
        return content_match.group(2).strip() if content_match else None

    def start(self):
        logging.info("Starting WebScraper...")

        num_str = str(random.randint(1, 70000))
        url = f"https://www.gutenberg.org/cache/epub/{num_str}/pg{num_str}.txt"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            content = response.text

            title, author, language = self.extract_metadata(content)

            if language and language.lower() == "english":
                ebook_content = self.extract_content(content)

                if ebook_content:
                    processed_words = self.reader.preprocessing(ebook_content)

                    book_data = self.reader.process_book_data(title, author, Counter(processed_words))

                    word_info = {
                        "book": title,
                        "author": author,
                        "words": book_data
                    }

                    logging.info(
                        f"Book '{title}' processed successfully with {len(book_data)} unique words."
                    )
                else:
                    logging.warning(f"Content missing for book '{title}'. Skipping.")
            else:
                logging.warning(f"Book skipped: Not in English or missing metadata. URL: {url}")
                return None

        except requests.RequestException as e:
            logging.error(f"Error fetching URL {url}: {e}")

        logging.info("Scraping completed.")
        return word_info
