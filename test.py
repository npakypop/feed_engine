import pandas as pd  # Бібліотека для аналізу даних. Ми використовуємо її для створення таблиць (DataFrame).
from lxml import etree  # Потужна бібліотека для роботи з XML. etree допомагає створювати та читати теги.
import requests  # Бібліотека для відправки запитів в інтернет (скачування файлів).
from datetime import datetime  # Модуль для роботи з часом (нам потрібна дата генерації фіда).
import re  # Модуль регулярних виразів. Потрібен для складного пошуку та заміни тексту.
import translators as ts


translation_cache = {} #cache для збереження вже перекладених текстів, щоб не звертатися до API повторно.
def translate_text(text):
    if not text or str(text).strip() == "" or str(text).isdigit():
        return text
    
    if text in translation_cache:
        return translation_cache[text]
    
    try:
        # Використовуємо Google через сервер 'google'
        # Або можна замінити на 'bing' чи 'alibaba'
        translated = ts.translate_text(text, from_language='ru', to_language='uk', translator='google')
        translation_cache[text] = translated
        return translated
    except Exception as e:
        print(f"⚠️ Помилка: {e}")
        return text

# --- КРОК 1: Функція парсингу (перетворення XML у таблицю) ---
def get_master_data(xml_source, is_url=True):
    print("🔄 Починаю повний розбір майстер-фіда...")
    try:
        if is_url:
            # requests.get: завантажує вміст за посиланням. timeout=30 — чекати відповіді не більше 30 сек.
            response = requests.get(xml_source, timeout=30)
            # .raise_for_status(): якщо сайт видасть помилку (наприклад 404), код зупиниться тут і викличе помилку.
            response.raise_for_status()
            xml_content = response.content # Отримуємо "сирі" байти завантаженого файлу.
        else:
            # open: відкриває файл локально. 'rb' — читання в бінарному режимі.
            with open(xml_source, 'rb') as f:
                xml_content = f.read()

        # etree.fromstring: перетворює текст/байти XML у дерево об'єктів, з якими Python може працювати.
        root = etree.fromstring(xml_content)

        # Створюємо порожній словник для категорій { "id": "Назва" }
        categories = {}
        # root.xpath: метод пошуку в XML. "//category" знайде всі теги <category> у будь-якому місці файлу.
        for cat in root.xpath("//category"):
            # .get('id'): отримує значення атрибута id="...". .text.strip(): бере назву категорії та видаляє пробіли.
            categories[cat.get('id')] = cat.text.strip() if cat.text else ""

        offers_data = [] # Порожній список, куди ми будемо складати словники з даними товарів.
        offers = root.xpath("//offer") # Знаходимо всі теги <offer> (товари).
        print(f"📦 Знайдено товарів: {len(offers)}")

        for offer in offers:
            # Створюємо словник для одного товару.
            item = {
                'id': offer.get('id'), # Беремо id з атрибута тегу <offer>.
                'group_id': offer.get('group_id'),
                'available': offer.get('available', 'true'), # Другий аргумент 'true' — значення за замовчуванням.
                'in_stock': offer.get('in_stock', 'true'),
            }

            # Список тегів, які ми хочемо знайти всередині кожного товару.
            tags = [
                'url', 'price', 'currencyId', 'categoryId', 
                'quantity_in_stock', 'vendorCode', 'vendor', 
                'name', 'name_ua', 'description', 'description_ua', 'old_price'
            ]
            
            for tag in tags:
                # .findtext(tag): шукає текст всередині вказаного тегу. Якщо тегу немає — поверне None.
                value = offer.findtext(tag)
                # .strip(): видаляє зайві пробіли на початку та в кінці тексту.
                item[tag] = value.strip() if value else ""

            # categories.get: шукаємо назву категорії за її ID у нашому словнику категорій.
            item['category_name'] = categories.get(item['categoryId'], "Невідома категорія")

            # xpath("picture"): знайде всі теги <picture> всередині поточного товару.
            pictures = [p.text for p in offer.xpath("picture") if p.text]
            # ", ".join(pictures): з'єднує список посилань у один рядок через кому.
            item['all_pictures'] = ", ".join(pictures)

            # Шукаємо всі теги <param>.
            for p in offer.xpath("param"):
                # .get('name'): бере назву параметра (наприклад "Вага").
                p_name = p.get('name').strip()
                # Створюємо в словнику ключ типу 'param_Вага' і записуємо туди значення.
                item[f"param_{p_name}"] = p.text.strip() if p.text else ""

            offers_data.append(item) # Додаємо готовий словник товару в загальний список.

        # pd.DataFrame: перетворює список словників на зручну таблицю.
        df = pd.DataFrame(offers_data)
        # pd.to_numeric: перетворює текстові ціни ("64.00") на числа для математичних операцій.
        # errors='coerce': якщо замість ціни буде текст, він перетвориться на NaN (порожнечу), а не викличе помилку.
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        return df # Функція повертає готову таблицю.

    except Exception as e:
        # try...except: якщо в блоці try станеться помилка, ми не "впадемо", а виведемо її текст.
        print(f"❌ Помилка при парсингу: {e}")
        return None

# --- ДОПОМІЖНА ФУНКЦІЯ ---
def clean_html(text):
    """Видаляє всі HTML теги з тексту за допомогою регулярних виразів"""
    if not isinstance(text, str): return "" # Якщо прийшло не слово — повертаємо пустий рядок.
    # re.sub: шукає шаблон <...> (все що в гострих дужках) і замінює на порожнечу.
    return re.sub(r'<[^>]+>', '', text).strip()

# --- КРОК 2: Генерація XML для MAUDAU ---
def generate_maudau_xml(df, output_filename="maudau_feed_test.xml"):
    print(f"🛠 Починаю збірку XML для MAUDAU...")
    
    # etree.Element: створює самий верхній (кореневий) тег XML.
    # datetime.now().strftime: бере поточний час і перетворює його у формат "РРРР-ММ-ДД ГГ:ХХ".
    root = etree.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    # etree.SubElement: створює вкладений тег всередині батьківського.
    shop = etree.SubElement(root, "shop")

    # 1. Категорії
    categories_node = etree.SubElement(shop, "categories")
    # .drop_duplicates(): залишає в таблиці тільки унікальні пари ID та Назва категорії.
    unique_cats = df[['categoryId', 'category_name']].drop_duplicates()
    
    # .iterrows(): цикл, який перебирає таблицю рядок за рядком.
    for _, row in unique_cats.iterrows():
        cat_id = str(row['categoryId'])
        cat_name = str(row['category_name'])
        cat_elem = etree.SubElement(categories_node, "category", id=cat_id)
        
        # Перевіряємо, чи є в таблиці стовпчик з портальними ID МАУДАУ.
        if 'maudau_portal_id' in df.columns:
            # .loc: знаходить рядок, де категорія збігається, і бере значення portal_id.
            # .iloc[0]: бере перше знайдене значення.
            val = df.loc[df['categoryId'] == cat_id, 'maudau_portal_id'].iloc[0]
            # pd.notna(val): перевіряє, чи не є комірка порожньою.
            if pd.notna(val):
                # .set: додає атрибут (наприклад portal_id="562") всередину тегу.
                cat_elem.set("portal_id", str(val))
        
        cat_elem.text = cat_name # Записуємо назву категорії всередину тегу <category>.

    # 2. Товари
    offers_node = etree.SubElement(shop, "offers")

    for _, row in df.iterrows():
        offer = etree.SubElement(offers_node, "offer", id=str(row['id']))
        offer.set("available", str(row['available']).lower())

        # Записуємо дані товару. clean_html видаляє теги з назв.
        etree.SubElement(offer, "name_ua").text = clean_html(row.get('name_ua', ''))
        etree.SubElement(offer, "name_ru").text = clean_html(row.get('name', ''))
        # Описи залишаємо як є, str() гарантує, що дані стануть рядком.
        etree.SubElement(offer, "description_ua").text = str(row.get('description_ua', ''))
        etree.SubElement(offer, "description_ru").text = str(row.get('description', ''))

        etree.SubElement(offer, "price").text = str(row['price'])
        
        # Якщо є стара ціна — додаємо її.
        if 'old_price' in row and pd.notna(row['old_price']) and str(row['old_price']) != "":
            etree.SubElement(offer, "old_price").text = str(row['old_price'])

        etree.SubElement(offer, "categoryId").text = str(row['categoryId'])
        etree.SubElement(offer, "vendor").text = str(row.get('vendor', ''))

        # Фото
        if row['all_pictures']:
            # .split(", "): розбиває рядок з посиланнями назад у список.
            pics = str(row['all_pictures']).split(", ")
            for p in pics[:12]: # Обрізаємо список до 12 штук (вимога МАУДАУ).
                etree.SubElement(offer, "picture").text = p.strip()

        # Параметри (Характеристики)
        # Шукаємо назви всіх колонок, які ми раніше назвали 'param_...'
        param_cols = [c for c in df.columns if c.startswith('param_')]
        for col in param_cols:
            val = row[col]
            if pd.notna(val) and str(val).strip() != "":
                # .replace: видаляє префікс 'param_', щоб залишити чисту назву (наприклад "Вага").
                p_name = col.replace('param_', '')
                param = etree.SubElement(offer, "param", name=p_name)
                param.text = str(val).strip()

    # Збереження результату у файл.
    # etree.ElementTree: обгортка, яка перетворює набір тегів у повноцінний файл.
    tree = etree.ElementTree(root)
    # .write: записує дані на диск. xml_declaration додає перший рядок <?xml...?>.
    # pretty_print=True: робить файл красивим (з відступами), а не в один рядок.
    tree.write(output_filename, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    print(f"🚀 Готово! Файл збережено: {output_filename}")



# --- ГОЛОВНИЙ ЗАПУСК ---
if __name__ == "__main__":
    # Спеціальна умова Python: цей код виконається тільки якщо ти запустиш саме цей файл.
    XML_URL = "https://www.vitberry.com.ua/content/export/32a59534f1502626737630acf454425c.xml"
    
    # Викликаємо першу функцію.
    df_master = get_master_data(XML_URL)
    
    if df_master is not None:
        # Додаємо в таблицю колонку 'maudau_portal_id' і заповнюємо її значенням "562".
        # Це те саме, що ти робив в Excel руками.
        df_master['maudau_portal_id'] = "562" 
        
        # Викликаємо другу функцію, передаючи їй нашу таблицю.
        generate_maudau_xml(df_master)