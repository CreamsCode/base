from src.collector import Reader, WebScraper


# Inicializar el scraper para 2 libros
scraper = WebScraper(number=2)

# Obtener palabras procesadas
words = scraper.start()

# Mostrar un resumen
print(f"Total words processed: {len(words)}")
print(f"Sample data: {words[:5]}")