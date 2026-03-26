import pandas as pd
from lxml import etree
import requests
from datetime import datetime
import re
import os
import html

XML_URL = os.getenv("MASTER_FEED_URL")

# --- КРОК 1: Функція парсингу ---
def get_master_data(xml_source, is_url=True):
    print("🔄 Починаю повний розбір майстер-фіда...")
    try:
        if is_url:
            response = requests.get(xml_source, timeout=30)
            response.raise_for_status()
            xml_content = response.content
        else:
            with open(xml_source, 'rb') as f:
                xml_content = f.read()

        root = etree.fromstring(xml_content)

        # Збираємо Категорії {id: "Name"}
        categories = {}
        for cat in root.xpath("//category"):
            categories[cat.get('id')] = cat.text.strip() if cat.text else ""

        offers_data = []
        offers = root.xpath("//offer")
        print(f"📦 Знайдено товарів: {len(offers)}")

        for offer in offers:
            item = {
                'id': offer.get('id'),
                'group_id': offer.get('group_id'),
                'available': offer.get('available', 'true'),
                'in_stock': offer.get('in_stock', 'true'),
            }

            # Стандартні теги
            tags = [
                'url', 'price', 'currencyId', 'categoryId', 
                'quantity_in_stock', 'vendorCode', 'vendor', 
                'name', 'name_ua', 'description', 'description_ua', 'old_price'
            ]
            
            for tag in tags:
                value = offer.findtext(tag)
                item[tag] = value.strip() if value else ""

            item['category_name'] = categories.get(item['categoryId'], "Невідома категорія")

            # Робота з фото
            pictures = [p.text for p in offer.xpath("picture") if p.text]
            item['all_pictures'] = ", ".join(pictures)

            # Параметри <param>
            for p in offer.xpath("param"):
                p_name = p.get('name').strip()
                item[f"param_{p_name}"] = p.text.strip() if p.text else ""

            offers_data.append(item)

        # Створення DataFrame (Виправлено назву!)
        df = pd.DataFrame(offers_data)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        return df # Повертаємо саме df

    except Exception as e:
        print(f"❌ Помилка при парсингу: {e}")
        return None

# --- ДОПОМІЖНА ФУНКЦІЯ ---
# def clean_html(text):
#     """Видаляє всі HTML теги з тексту (для назв)"""
#     if not isinstance(text, str): return ""
#     return re.sub(r'<[^>]+>', '', text).strip()

# # --- КРОК 2: Генерація XML для MAUDAU ---
# def generate_maudau_xml(df, output_filename="maudau_feed.xml"):
#     print(f"🛠 Починаю збірку XML для MAUDAU...")
    
#     root = etree.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
#     shop = etree.SubElement(root, "shop")

#     # 1. Категорії
#     categories_node = etree.SubElement(shop, "categories")
#     unique_cats = df[['categoryId', 'category_name']].drop_duplicates()
    
#     for _, row in unique_cats.iterrows():
#         cat_id = str(row['categoryId'])
#         cat_name = str(row['category_name'])
#         cat_elem = etree.SubElement(categories_node, "category", id=cat_id)
        
#         # Надійний пошук portal_id
#         if 'maudau_portal_id' in df.columns:
#             # Шукаємо перше значення portal_id для цієї категорії
#             val = df.loc[df['categoryId'] == cat_id, 'maudau_portal_id'].iloc[0]
#             if pd.notna(val):
#                 cat_elem.set("portal_id", str(val))
        
#         cat_elem.text = cat_name

#     # 2. Товари
#     offers_node = etree.SubElement(shop, "offers")

#     for _, row in df.iterrows():
#         offer = etree.SubElement(offers_node, "offer", id=str(row['vendorCode']))
#         offer.set("available", str(row['available']).lower())

#         # Назви та описи (МАУДАУ: без CDATA, назви без HTML)
#         etree.SubElement(offer, "name_ua").text = clean_html(row.get('name_ua', ''))
#         etree.SubElement(offer, "name_ru").text = clean_html(row.get('name', ''))
#         etree.SubElement(offer, "description_ua").text = str(row.get('description_ua', ''))
#         etree.SubElement(offer, "description_ru").text = str(row.get('description', ''))

#         etree.SubElement(offer, "price").text = str(row['price'])
        
#         if 'old_price' in row and pd.notna(row['old_price']) and str(row['old_price']) != "":
#             etree.SubElement(offer, "old_price").text = str(row['old_price'])

#         etree.SubElement(offer, "categoryId").text = str(row['categoryId'])
#         etree.SubElement(offer, "vendor").text = str(row.get('vendor', ''))

#         # Фото
#         if row['all_pictures']:
#             pics = str(row['all_pictures']).split(", ")
#             for p in pics[:12]:
#                 etree.SubElement(offer, "picture").text = p.strip()

#         # Параметри
#         param_cols = [c for c in df.columns if c.startswith('param_')]
#         for col in param_cols:
#             val = row[col]
#             if pd.notna(val) and str(val).strip() != "":
#                 p_name = col.replace('param_', '')
#                 param = etree.SubElement(offer, "param", name=p_name)
#                 param.text = str(val).strip()

#     # Збереження
#     tree = etree.ElementTree(root)
#     tree.write(output_filename, encoding="UTF-8", xml_declaration=True, pretty_print=True)
#     print(f"🚀 Готово! Файл збережено: {output_filename}")


def final_clean_text(text, is_description=False):
    """Мега-чистка тексту від усіх можливих проблем"""
    if not isinstance(text, str) or text == "":
        return ""
    
    # 1. Виправляємо подвійне екранування (наприклад, &amp;mdash; перетворюємо на нормальне тире —)
    # Ми робимо це два рази про всяк випадок
    text = html.unescape(text)
    text = html.unescape(text)
    
    # 2. Видаляємо невидимі технічні символи (ASCII 0-31), які заборонені в XML
    text = "".join(ch for ch in text if ord(ch) >= 32 or ch in "\n\r\t")
    
    if not is_description:
        # Для назв: видаляємо ВСІ теги
        text = re.sub(r'<[^>]+>', '', text)
    else:
        # Для описів: видаляємо CDATA, якщо вони десь вижили
        text = text.replace('<![CDATA[', '').replace(']]>', '')
        # Видаляємо стилі (style="...") та класи (class="..."), залишаючи чисті теги <div>, <p>
        text = re.sub(r'\s+(style|class|id|onclick|target)\s*=\s*("[^"]*"|\'[^\']*\')', '', text, flags=re.IGNORECASE)
    
    return text.strip()

def generate_maudau_xml(df, output_filename="maudau_feed.xml"):
    print(f"🚀 Починаю генерацію фіда...")
    
    # Створюємо корінь з правильною датою
    root = etree.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = etree.SubElement(root, "shop")

    # --- КАТЕГОРИИ ---
    categories_node = etree.SubElement(shop, "categories")
    unique_cats = df[['categoryId', 'category_name']].drop_duplicates()
    
    for _, row in unique_cats.iterrows():
        cat_id = str(row['categoryId'])
        # Створюємо тег категорії. id — обов'язковий.
        cat_elem = etree.SubElement(categories_node, "category", id=cat_id)
        
        # Перевіряємо portal_id (має бути числом)
        if 'maudau_portal_id' in df.columns:
            p_id = row.get('maudau_portal_id')
            if pd.notna(p_id) and str(p_id).strip() != "":
                cat_elem.set("portal_id", str(int(float(p_id)))) # Перетворюємо на чисте число
        
        cat_elem.text = final_clean_text(row['category_name'])

    # --- ТОВАРЫ ---
    offers_node = etree.SubElement(shop, "offers")

    for _, row in df.iterrows():
        # ID товару: MAUDAU любить прості ID. Якщо є дефіс - це ок, але пустим бути не може.
        offer_id = str(row['id']).strip()
        if not offer_id: continue # Пропускаємо товар без ID

        # Available: Тільки true або false, ніяких порожніх лапок!
        avail_val = str(row.get('available', 'true')).lower()
        is_available = "true" if avail_val in ['true', '1', 'yes'] else "false"

        offer = etree.SubElement(offers_node, "offer", id=offer_id)
        offer.set("available", is_available)

        # Назви та Описи проганяємо через нашу мега-чистку
        etree.SubElement(offer, "name_ua").text = final_clean_text(row.get('name_ua', ''), is_description=False)
        etree.SubElement(offer, "name_ru").text = final_clean_text(row.get('name', ''), is_description=False)
        etree.SubElement(offer, "description_ua").text = final_clean_text(row.get('description_ua', ''), is_description=True)
        etree.SubElement(offer, "description_ru").text = final_clean_text(row.get('description', ''), is_description=True)

        # Ціна має бути числом
        price = str(int(float(row['price']))) if pd.notna(row['price']) else "0"
        etree.SubElement(offer, "price").text = price
        
        # Категорія товару
        etree.SubElement(offer, "categoryId").text = str(row['categoryId'])
        
        # Бренд (Виробник)
        etree.SubElement(offer, "vendor").text = final_clean_text(row.get('vendor', 'Vitberry'))

        # Картинки (не більше 12)
        if 'all_pictures' in row and row['all_pictures']:
            pics = str(row['all_pictures']).split(",")
            for p in pics[:12]:
                url = p.strip()
                if url.startswith("http"): # Тільки валідні посилання
                    etree.SubElement(offer, "picture").text = url
    # Збереження
    tree = etree.ElementTree(root)
    # Використовуємо метод, який автоматично замінить < на &lt; і т.д.
    tree.write(output_filename, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    print(f"✅ Готово! Файл {output_filename} створено та очищено.")



# --- ГОЛОВНИЙ ЗАПУСК ---
if __name__ == "__main__":
    # XML_URL = ""
    
    df_master = get_master_data(XML_URL)
    
    if df_master is not None:
        # Тимчасовий мапінг для тесту
        df_master['maudau_portal_id'] = "562" 
        
        generate_maudau_xml(df_master)
