from src.collector import WebScraper
import json

scraper = WebScraper(number=2)

books_data = scraper.start()

print(json.dumps(books_data, indent=4, ensure_ascii=False))

print(books_data)
