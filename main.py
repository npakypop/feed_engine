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

            tags = [
                'url', 'price', 'currencyId', 'categoryId', 
                'quantity_in_stock', 'vendorCode', 'vendor', 
                'name', 'name_ua', 'description', 'description_ua', 'old_price'
            ]
            
            for tag in tags:
                value = offer.findtext(tag)
                item[tag] = value.strip() if value else ""

            item['category_name'] = categories.get(item['categoryId'], "Невідома категорія")

            pictures = [p.text for p in offer.xpath("picture") if p.text]
            item['all_pictures'] = ", ".join(pictures)

            for p in offer.xpath("param"):
                p_name = p.get('name').strip()
                item[f"param_{p_name}"] = p.text.strip() if p.text else ""

            offers_data.append(item)

        df = pd.DataFrame(offers_data)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        return df 

    except Exception as e:
        print(f"❌ Помилка при парсингу: {e}")
        return None


def final_clean_text(text, is_description=False):
    if not isinstance(text, str) or text == "":
        return ""
    
    text = html.unescape(text)
    text = html.unescape(text)
    
    text = "".join(ch for ch in text if ord(ch) >= 32 or ch in "\n\r\t")
    
    if not is_description:
        text = re.sub(r'<[^>]+>', '', text)
    else:
        text = text.replace('<![CDATA[', '').replace(']]>', '')
        text = re.sub(r'\s+(style|class|id|onclick|target)\s*=\s*("[^"]*"|\'[^\']*\')', '', text, flags=re.IGNORECASE)
    
    return text.strip()

def generate_maudau_xml(df, output_filename="maudau_feed.xml"):
    print(f"🚀 Починаю генерацію фіда...")
    
    root = etree.Element("yml_catalog", date=datetime.now().strftime("%Y-%m-%d %H:%M"))
    shop = etree.SubElement(root, "shop")

    categories_node = etree.SubElement(shop, "categories")
    unique_cats = df[['categoryId', 'category_name']].drop_duplicates()
    
    for _, row in unique_cats.iterrows():
        cat_id = str(row['categoryId'])
        cat_elem = etree.SubElement(categories_node, "category", id=cat_id)
        
        if 'maudau_portal_id' in df.columns:
            p_id = row.get('maudau_portal_id')
            if pd.notna(p_id) and str(p_id).strip() != "":
                cat_elem.set("portal_id", str(int(float(p_id)))) 
        
        cat_elem.text = final_clean_text(row['category_name'])

    offers_node = etree.SubElement(shop, "offers")

    for _, row in df.iterrows():
        # --- ЗМІНА ТУТ: Беремо vendorCode замість системного id ---
        # 1. Отримуємо артикул. Якщо він порожній, залишаємо старий id (як запасний план)
        offer_id = str(row.get('vendorCode', '')).strip()
        if not offer_id:
            offer_id = str(row['id']).strip()

        # 2. Очищуємо артикул від зайвих знаків, але залишаємо дефіс та підкреслення
        # Це важливо для стабільної роботи XML та Maudau
        offer_id = re.sub(r'[^a-zA-Z0-9_-]', '', offer_id)

        if not offer_id: continue 

        avail_val = str(row.get('available', 'true')).lower()
        is_available = "true" if avail_val in ['true', '1', 'yes'] else "false"

        # Записуємо очищений vendorCode в атрибут id
        offer = etree.SubElement(offers_node, "offer", id=offer_id)
        offer.set("available", is_available)

        etree.SubElement(offer, "name_ua").text = final_clean_text(row.get('name_ua', ''), is_description=False)
        etree.SubElement(offer, "name_ru").text = final_clean_text(row.get('name', ''), is_description=False)
        etree.SubElement(offer, "description_ua").text = final_clean_text(row.get('description_ua', ''), is_description=True)
        etree.SubElement(offer, "description_ru").text = final_clean_text(row.get('description', ''), is_description=True)

        price = str(int(float(row['price']))) if pd.notna(row['price']) else "0"
        etree.SubElement(offer, "price").text = price
        
        etree.SubElement(offer, "categoryId").text = str(row['categoryId'])
        etree.SubElement(offer, "vendor").text = final_clean_text(row.get('vendor', 'Vitberry'))

        if 'all_pictures' in row and row['all_pictures']:
            pics = str(row['all_pictures']).split(",")
            for p in pics[:12]:
                url = p.strip()
                if url.startswith("http"): 
                    etree.SubElement(offer, "picture").text = url

    tree = etree.ElementTree(root)
    tree.write(output_filename, encoding="UTF-8", xml_declaration=True, pretty_print=True)
    print(f"✅ Готово! Файл {output_filename} створено. Використано VendorCode як ID.")


# --- ГОЛОВНИЙ ЗАПУСК ---
if __name__ == "__main__":
    df_master = get_master_data(XML_URL)
    
    if df_master is not None:
        df_master['maudau_portal_id'] = "562" 
        generate_maudau_xml(df_master)
