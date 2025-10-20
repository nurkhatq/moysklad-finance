import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from collections import defaultdict

class MoySkladAPI:
    """Класс для работы с API МойСклад"""
    
    def __init__(self, token):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }
    
    def _make_request(self, endpoint, params=None, timeout=10):
        """Базовый метод для запросов"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"⚠️  Ошибка запроса {endpoint}: {e}")
            return None
    
    def get_all_orders(self, date_from=None, date_to=None):
        """Получить ВСЕ заказы с позициями"""
        all_orders = []
        offset = 0
        limit = 100
        
        while True:
            print(f"Загружаем заказы: offset {offset}...")
            
            endpoint = "/entity/customerorder"
            params = {
                "limit": limit,
                "offset": offset,
                "expand": "positions.assortment,agent,organization,state,attributes"
            }
            
            filters = []
            if date_from:
                filters.append(f"moment>={date_from}")
            if date_to:
                filters.append(f"moment<={date_to}")
            
            if filters:
                params["filter"] = ";".join(filters)
            
            data = self._make_request(endpoint, params, timeout=30)
            
            if not data or 'rows' not in data:
                break
            
            orders = data['rows']
            all_orders.extend(orders)
            
            if len(orders) < limit:
                break
            
            offset += limit
            time.sleep(0.2)
        
        print(f"✅ Загружено заказов: {len(all_orders)}")
        return all_orders
    
    def get_products_batch(self, product_ids):
        """Получить товары пакетом"""
        if not product_ids:
            return {}
        
        products = {}
        batch_size = 100
        
        for i in range(0, len(product_ids), batch_size):
            batch = list(product_ids)[i:i+batch_size]
            
            # Формируем фильтр
            filter_parts = [f"id={pid}" for pid in batch]
            filter_str = ";".join(filter_parts)
            
            endpoint = "/entity/product"
            params = {
                "filter": filter_str,
                "limit": 100
            }
            
            data = self._make_request(endpoint, params)
            
            if data and 'rows' in data:
                for product in data['rows']:
                    products[product['id']] = product
            
            time.sleep(0.1)
        
        return products


class OrderProcessor:
    """Класс для обработки заказов"""
    
    def __init__(self):
        self.product_cache = {}
    
    def extract_commission_and_delivery(self, order):
        """Извлечь комиссию и доставку из дополнительных полей"""
        
        commission_percent = 0
        commission_sum = 0
        delivery_cost = 0
        
        attributes = order.get("attributes", [])
        
        for attr in attributes:
            attr_name = attr.get("name", "")
            attr_value = attr.get("value")
            
            # Пропускаем пустые значения
            if attr_value is None or attr_value == "":
                continue
            
            # Преобразуем в число
            try:
                attr_value = float(attr_value)
            except:
                continue
            
            attr_name_lower = attr_name.lower()
            
            # Комиссия по товарам
            if "комисс" in attr_name_lower and "товар" in attr_name_lower:
                commission_sum += attr_value
            
            # Комиссия за доставку
            elif "комисс" in attr_name_lower and "доставк" in attr_name_lower:
                commission_sum += attr_value
            
            # Общая комиссия
            elif "комисс" in attr_name_lower:
                if "%" in attr_name:
                    commission_percent = attr_value
                elif "сумма" in attr_name_lower:
                    commission_sum += attr_value
            
            # Доставка
            if "доставк" in attr_name_lower or "delivery" in attr_name_lower:
                # Исключаем комиссию за доставку (уже учли выше)
                if "комисс" not in attr_name_lower:
                    if any(word in attr_name_lower for word in ["стоимость", "сумма", "цена", "cost"]):
                        delivery_cost += attr_value
        
        # Если есть процент, но нет суммы - рассчитываем
        order_sum = order.get("sum", 0) / 100
        if commission_percent > 0 and commission_sum == 0:
            commission_sum = order_sum * (commission_percent / 100)
        
        return {
            'commission_percent': commission_percent,
            'commission_sum': commission_sum,
            'delivery_cost': delivery_cost
        }



    def get_buy_price_for_item(self, item, item_type, products_cache):
        """Получить цену закупки"""
        
        buy_price = 0
        source = "не найдена"
        
        # Для обычного товара
        if item_type == 'product':
            buy_price_obj = item.get('buyPrice', {})
            if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                buy_price = buy_price_obj.get('value', 0)
                source = "товар"
        
        # Для комплекта - ищем по артикулу
        elif item_type == 'bundle':
            article = item.get('article')
            if article and article in products_cache:
                base_product = products_cache[article]
                buy_price_obj = base_product.get('buyPrice', {})
                if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                    buy_price = buy_price_obj.get('value', 0)
                    source = f"основной товар (арт. {article})"
        
        # Для модификации
        elif item_type == 'variant':
            buy_price_obj = item.get('buyPrice', {})
            if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                buy_price = buy_price_obj.get('value', 0)
                source = "модификация"
        
        return {
            'value': buy_price,
            'source': source
        }
    
    def process_orders(self, orders, products_cache):
        """Обработать все заказы"""
        
        orders_data = []
        positions_data = []
        summaries = []
        
        print("\n📊 Обработка данных...")
        
        for idx, order in enumerate(orders):
            if (idx + 1) % 200 == 0:
                print(f"  Обработано: {idx + 1}/{len(orders)}")
            
            # 1. Общая информация о заказе
            order_data = self.extract_order_data(order)
            orders_data.append(order_data)
            
            # 2. Комиссия и доставка
            costs = self.extract_commission_and_delivery(order)
            
            # 3. Позиции
            positions = order.get('positions', {}).get('rows', [])
            total_positions = len(positions)
            
            # Распределяем доставку и комиссию на позиции
            order_sum = order.get("sum", 0) / 100
            
            order_cost = 0
            
            for pos in positions:
                assortment = pos.get('assortment', {})
                item_type = assortment.get('meta', {}).get('type', 'product')
                
                # Цена закупки
                buy_price_info = self.get_buy_price_for_item(assortment, item_type, products_cache)
                cost_per_unit = buy_price_info['value'] / 100 if buy_price_info['value'] > 0 else 0
                
                quantity = pos.get("quantity", 0)
                price = pos.get("price", 0) / 100
                position_sum = price * quantity
                
                # Доля позиции в заказе
                position_share = (position_sum / order_sum) if order_sum > 0 else (1 / total_positions if total_positions > 0 else 0)
                
                # Доставка на позицию
                delivery_per_position = costs['delivery_cost'] * position_share
                
                # Комиссия на позицию
                commission_per_position = costs['commission_sum'] * position_share
                
                # Себестоимость товара
                product_cost = cost_per_unit * quantity
                
                # ПОЛНАЯ себестоимость
                full_cost = product_cost + delivery_per_position + commission_per_position
                
                order_cost += full_cost
                
                # Прибыль
                profit = position_sum - full_cost
                margin_percent = (profit / position_sum * 100) if position_sum > 0 else 0
                
                position_data = {
                    "Номер заказа": order.get("name", ""),
                    "Дата заказа": order.get("moment", ""),
                    "Товар": assortment.get("name", ""),
                    "Артикул": assortment.get("article", ""),
                    "Код": assortment.get("code", ""),
                    "Тип": item_type,
                    "Количество": quantity,
                    "Цена": round(price, 2),
                    "Скидка %": pos.get("discount", 0),
                    "НДС %": pos.get("vat", 0),
                    "Сумма": round(position_sum, 2),
                    "Себестоимость товара ед.": round(cost_per_unit, 2),
                    "Себестоимость товара общая": round(product_cost, 2),
                    "Доставка на позицию": round(delivery_per_position, 2),
                    "Комиссия на позицию": round(commission_per_position, 2),
                    "ПОЛНАЯ себестоимость": round(full_cost, 2),
                    "Прибыль": round(profit, 2),
                    "Маржа %": round(margin_percent, 2),
                    "Источник себестоимости": buy_price_info['source']
                }
                
                positions_data.append(position_data)
            
            # 4. Сводка
            vat_sum = order.get("vatSum", 0) / 100
            net_profit = order_sum - order_cost - vat_sum
            margin_percent = (net_profit / order_sum * 100) if order_sum > 0 else 0
            
            summary = {
                "Номер заказа": order.get("name", ""),
                "Выручка": round(order_sum, 2),
                "Себестоимость товаров": round(sum(p["Себестоимость товара общая"] for p in positions_data if p["Номер заказа"] == order.get("name", "")), 2),
                "Доставка": round(costs['delivery_cost'], 2),
                "Комиссия МП": round(costs['commission_sum'], 2),
                "ПОЛНАЯ себестоимость": round(order_cost, 2),
                "НДС": round(vat_sum, 2),
                "Чистая прибыль": round(net_profit, 2),
                "Рентабельность %": round(margin_percent, 2),
                "Количество товаров": total_positions,
                "Себестоимость доступна": "Да" if order_cost > 0 else "Нет"
            }
            
            summaries.append(summary)
        
        print(f"  Обработано: {len(orders)}/{len(orders)}")
        
        return orders_data, positions_data, summaries
    
    @staticmethod
    def extract_order_data(order):
        """Извлечь данные из заказа"""
        
        order_data = {
            "Номер заказа": order.get("name", ""),
            "Дата": order.get("moment", ""),
            "Контрагент": order.get("agent", {}).get("name", "") if order.get("agent") else "",
            "Организация": order.get("organization", {}).get("name", "") if order.get("organization") else "",
            "Статус": order.get("state", {}).get("name", "") if order.get("state") else "",
            "Применён": "Да" if order.get("applicable") else "Нет",
            "Сумма заказа": order.get("sum", 0) / 100,
            "Оплачено": order.get("payedSum", 0) / 100,
            "Отгружено": order.get("shippedSum", 0) / 100,
            "Зарезервировано": order.get("reservedSum", 0) / 100,
            "НДС": order.get("vatSum", 0) / 100,
            "НДС включён": "Да" if order.get("vatIncluded") else "Нет",
        }
        
        # Дополнительные поля
        attributes = order.get("attributes", [])
        for attr in attributes:
            attr_name = attr.get("name", "")
            attr_value = attr.get("value", "")
            order_data[attr_name] = attr_value
        
        if order.get("shipmentAddress"):
            order_data["Адрес доставки"] = order.get("shipmentAddress")
        
        return order_data


class GoogleSheetsUploader:
    """Класс для работы с Google Sheets"""
    
    def __init__(self, credentials_file, spreadsheet_name):
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open(spreadsheet_name)
    
    def upload_dataframe(self, df, worksheet_name, clear=True):
        """Загрузить DataFrame в лист таблицы"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=50)
        
        if clear:
            worksheet.clear()
        
        # Очистка данных
        df_clean = df.copy()
        df_clean = df_clean.replace([float('inf'), float('-inf')], 0)
        df_clean = df_clean.fillna('')
        
        # Конвертируем в список
        data = []
        data.append(df_clean.columns.values.tolist())
        
        for row in df_clean.values:
            clean_row = []
            for val in row:
                if isinstance(val, float):
                    if np.isnan(val) or np.isinf(val):
                        clean_row.append(0)
                    else:
                        clean_row.append(round(val, 2))
                else:
                    clean_row.append(val)
            data.append(clean_row)
        
        # Загружаем порциями
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            if i == 0:
                worksheet.update(values=batch, range_name='A1')
            else:
                worksheet.append_rows(batch, value_input_option='USER_ENTERED')
            print(f"  Загружено строк: {min(i+batch_size, len(data))}/{len(data)}")
        
        print(f"✓ Данные загружены в лист '{worksheet_name}': {len(df)} строк")
    
    def format_worksheet(self, worksheet_name):
        """Применить форматирование к листу"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            
            worksheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'horizontalAlignment': 'CENTER',
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            worksheet.freeze(rows=1)
        except Exception as e:
            print(f"⚠️  Ошибка форматирования: {e}")


def main():
    """Основная функция"""
    
    # ===== НАСТРОЙКИ =====
    MOYSKLAD_TOKEN = "105d4f38eb9a02400c3a6428ea71640babe37e98"
    GOOGLE_CREDENTIALS_FILE = "credentials.json"
    SPREADSHEET_NAME = "Финансовый отчёт"
    
    # Период
    date_to = datetime.now()
    date_from = date_to - timedelta(days=30)
    
    DATE_FROM = date_from.strftime("%Y-%m-%d 00:00:00")
    DATE_TO = date_to.strftime("%Y-%m-%d 23:59:59")
    
    print("=" * 60)
    print("ЗАГРУЗКА ДАННЫХ ИЗ МОЙСКЛАД В GOOGLE SHEETS")
    print("(ОПТИМИЗИРОВАННАЯ ВЕРСИЯ)")
    print("=" * 60)
    print(f"Период: {DATE_FROM} - {DATE_TO}\n")
    
    # Инициализация
    api = MoySkladAPI(MOYSKLAD_TOKEN)
    
    # 1. Получаем все заказы с позициями за 1 запрос
    print("📦 Загрузка заказов с позициями...")
    orders = api.get_all_orders(DATE_FROM, DATE_TO)
    
    if not orders:
        print("❌ Заказы не найдены!")
        return
    
    # 2. Собираем уникальные артикулы для поиска основных товаров (для комплектов)
    print("\n🔍 Сбор уникальных артикулов...")
    unique_articles = set()
    
    for order in orders:
        positions = order.get('positions', {}).get('rows', [])
        for pos in positions:
            assortment = pos.get('assortment', {})
            item_type = assortment.get('meta', {}).get('type', 'product')
            article = assortment.get('article')
            
            if article and item_type == 'bundle':
                unique_articles.add(article)
    
    print(f"✅ Найдено уникальных артикулов для комплектов: {len(unique_articles)}")
    
    # 3. Получаем товары по артикулам
    products_cache = {}
    
    if unique_articles:
        print("\n📦 Загрузка основных товаров для комплектов...")
        for article in unique_articles:
            endpoint = "/entity/product"
            params = {
                "filter": f"article={article}",
                "limit": 1
            }
            
            data = api._make_request(endpoint, params)
            if data and data.get('rows'):
                product = data['rows'][0]
                products_cache[article] = product
            
            time.sleep(0.05)
        
        print(f"✅ Загружено товаров: {len(products_cache)}")
    
    # 4. Обработка данных
    processor = OrderProcessor()
    orders_data, positions_data, summaries = processor.process_orders(orders, products_cache)
    
    # 5. Создаём DataFrame
    df_orders = pd.DataFrame(orders_data)
    df_positions = pd.DataFrame(positions_data)
    df_summary = pd.DataFrame(summaries)
    
    # 6. Статистика
    print("\n📊 Создание статистики...")
    
    if not df_summary.empty:
        total_stats = {
            "Метрика": [
                "Общая выручка",
                "Себестоимость товаров",
                "Доставка",
                "Комиссии МП",
                "ПОЛНАЯ себестоимость",
                "НДС",
                "Чистая прибыль",
                "Средняя рентабельность %",
                "Количество заказов",
                "Средний чек"
            ],
            "Значение": [
                round(df_summary["Выручка"].sum(), 2),
                round(df_summary["Себестоимость товаров"].sum(), 2),
                round(df_summary["Доставка"].sum(), 2),
                round(df_summary["Комиссия МП"].sum(), 2),
                round(df_summary["ПОЛНАЯ себестоимость"].sum(), 2),
                round(df_summary["НДС"].sum(), 2),
                round(df_summary["Чистая прибыль"].sum(), 2),
                round(df_summary["Рентабельность %"].mean(), 2),
                len(df_summary),
                round(df_summary["Выручка"].mean(), 2)
            ]
        }
        df_stats = pd.DataFrame(total_stats)
    else:
        df_stats = pd.DataFrame()
    
    # 7. Анализ качества
    print("\n" + "=" * 60)
    print("📊 АНАЛИЗ ДАННЫХ")
    print("=" * 60)
    print(f"\n✅ Заказов обработано: {len(df_orders)}")
    print(f"✅ Позиций обработано: {len(df_positions)}")
    
    if len(df_positions) > 0:
        with_cost = df_positions[df_positions['ПОЛНАЯ себестоимость'] > 0]
        print(f"✅ Позиций с себестоимостью: {len(with_cost)} ({len(with_cost)/len(df_positions)*100:.1f}%)")
    
    # 8. Сохранение CSV
    print("\n💾 Сохранение локальных копий...")
    df_orders.to_csv("orders.csv", index=False, encoding='utf-8-sig')
    df_positions.to_csv("positions.csv", index=False, encoding='utf-8-sig')
    df_summary.to_csv("summary.csv", index=False, encoding='utf-8-sig')
    print("✓ orders.csv\n✓ positions.csv\n✓ summary.csv")
    
    # 9. Загрузка в Google Sheets
    print("\n" + "=" * 60)
    print("ЗАГРУЗКА В GOOGLE SHEETS")
    print("=" * 60)
    
    try:
        uploader = GoogleSheetsUploader(GOOGLE_CREDENTIALS_FILE, SPREADSHEET_NAME)
        
        print("\n📤 Загрузка данных...")
        uploader.upload_dataframe(df_orders, "Заказы общая информация")
        uploader.upload_dataframe(df_positions, "Позиции детально")
        uploader.upload_dataframe(df_summary, "Сводка с прибылью")
        uploader.upload_dataframe(df_stats, "Общая статистика")
        
        print("\n🎨 Форматирование...")
        for sheet_name in ["Заказы общая информация", "Позиции детально", "Сводка с прибылью", "Общая статистика"]:
            uploader.format_worksheet(sheet_name)
        
        print("\n" + "=" * 60)
        print("✅ УСПЕШНО! Данные загружены в Google Sheets")
        print(f"📊 Таблица: {SPREADSHEET_NAME}")
        print("=" * 60)
        
        if not df_stats.empty:
            print("\n📊 КРАТКАЯ СТАТИСТИКА:")
            print(df_stats.to_string(index=False))
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        print("💾 Данные сохранены в CSV файлы")


if __name__ == "__main__":
    main()