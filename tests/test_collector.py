from src.collector import WebScraper
import json

# Inicializar el scraper para 2 libros
scraper = WebScraper(number=2)

# Obtener palabras procesadas
books_data = scraper.start()

# Mostrar el JSON resultante en un formato legible
print(json.dumps(books_data, indent=4, ensure_ascii=False))

print(books_data)
