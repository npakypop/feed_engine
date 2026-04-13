import xml.etree.ElementTree as ET
import requests

def extract_unique_params(url):
    unique_params = set()

    try:
        print(f"📡 Завантажую фід...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Парсимо XML
        root = ET.fromstring(response.content)

        # 1. Знаходимо всі оффери
        offers = root.findall('.//offer')
        print(f"📦 Знайдено товарів для аналізу: {len(offers)}")

        # 2. Перебираємо кожен товар окремо
        for offer in offers:
            # Усередині кожного товару шукаємо всі параметри
            params = offer.findall('param')
            for p in params:
                name = p.get('name')
                if name:
                    unique_params.add(name.strip())
        
        # Сортуємо для зручності
        sorted_params = sorted(list(unique_params))

        # 3. Зберігаємо результат
        with open('unique_params.txt', 'w', encoding='utf8') as f:
            for item in sorted_params:
                f.write(f"{item}\n")
        
        print(f"✅ Готово! Знайдено унікальних характеристик: {len(sorted_params)}")
        print(f"📁 Файл 'unique_params.txt' створено.")
        
        if sorted_params:
            print("\nСписок знайдених характеристик:")
            for p in sorted_params:
                print(f" - {p}")
                
    except Exception as e:
        print(f"❌ Помилка: {e}")

# Твоє посилання
FEED_URL = "https://www.vitberry.com.ua/content/export/63d47af4d1381987197968a23866d482.xml"

# Запуск
extract_unique_params(FEED_URL)