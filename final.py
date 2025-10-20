import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json

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
        """Базовый метод для запросов с обработкой ошибок"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(f"⏱️  Таймаут запроса: {endpoint}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка запроса: {e}")
            return None
    
    def get_customer_orders(self, date_from=None, date_to=None, limit=100, offset=0):
        """Получить заказы покупателя"""
        endpoint = "/entity/customerorder"
        
        params = {
            "limit": limit,
            "offset": offset,
        }
        
        filters = []
        if date_from:
            filters.append(f"moment>={date_from}")
        if date_to:
            filters.append(f"moment<={date_to}")
        
        if filters:
            params["filter"] = ";".join(filters)
        
        return self._make_request(endpoint, params)
    
    def get_order_positions(self, order_id):
        """Получить позиции заказа"""
        endpoint = f"/entity/customerorder/{order_id}/positions"
        return self._make_request(endpoint)
    
    def get_all_orders(self, date_from=None, date_to=None):
        """Получить ВСЕ заказы с пагинацией"""
        all_orders = []
        offset = 0
        limit = 100
        
        while True:
            print(f"Загружаем заказы: offset {offset}...")
            data = self.get_customer_orders(date_from, date_to, limit, offset)
            
            if not data or 'rows' not in data:
                break
            
            orders = data['rows']
            all_orders.extend(orders)
            
            if len(orders) < limit:
                break
            
            offset += limit
            time.sleep(0.3)
        
        print(f"Всего загружено заказов: {len(all_orders)}")
        return all_orders
    
    def get_item_by_href(self, href):
        """Получить товар/комплект/модификацию по href"""
        if not href:
            return None
        
        try:
            response = requests.get(href, headers=self.headers, timeout=10)  # Было 5, стало 10
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(f"⏱️  Таймаут получения товара: {href}")
            return None
        except Exception as e:
            print(f"⚠️  Ошибка получения товара по href: {e}")
            return None
    
    def get_product_by_article(self, article):
        """Найти основной товар по артикулу"""
        endpoint = "/entity/product"
        params = {
            "filter": f"article={article}",
            "limit": 1
        }
        
        data = self._make_request(endpoint, params)
        if data and data.get('rows'):
            return data['rows'][0]
        return None
    
    def get_stock_by_store(self, item_id, item_type, moment=None):
        """Получить остатки по всем складам"""
        endpoint = "/report/stock/bystore"
        
        # ДЛЯ BUNDLE НЕЛЬЗЯ ПОЛУЧИТЬ ОСТАТКИ НАПРЯМУЮ!
        # Нужно получать остатки компонентов
        if item_type == 'bundle':
            # Для комплектов возвращаем пустые остатки
            # Остатки комплекта = минимум остатков компонентов
            return None
        
        filter_param = f"{item_type}=https://api.moysklad.ru/api/remap/1.2/entity/{item_type}/{item_id}"
        params = {"filter": filter_param}
        
        if moment:
            params["moment"] = moment
        
        return self._make_request(endpoint, params, timeout=15)


class OrderProcessor:
    """Класс для обработки и анализа заказов"""
    
    def __init__(self, api):
        self.api = api
        self.stock_cache = {}
        self.product_cache = {}
        self.buy_price_cache = {}
    
    def get_buy_price_for_item(self, item, item_type):
        """
        Получить цену закупки для товара/комплекта/модификации
        
        Логика:
        1. Для product - берём buyPrice
        2. Для bundle - ищем основной товар по артикулу и берём его buyPrice
        3. Для variant - сначала пробуем его buyPrice, если нет - берём с базового товара
        """
        
        item_id = item.get('id')
        
        # Проверяем кэш
        if item_id in self.buy_price_cache:
            return self.buy_price_cache[item_id]
        
        buy_price = 0
        source = "не найдена"
        
        # Для обычного товара
        if item_type == 'product':
            buy_price_obj = item.get('buyPrice', {})
            if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                buy_price = buy_price_obj.get('value', 0)
                source = "товар"
        
        # Для комплекта - ищем основной товар по артикулу
        elif item_type == 'bundle':
            article = item.get('article')
            if article:
                # Проверяем кэш продуктов
                if article in self.product_cache:
                    base_product = self.product_cache[article]
                else:
                    base_product = self.api.get_product_by_article(article)
                    if base_product:
                        self.product_cache[article] = base_product
                
                if base_product:
                    buy_price_obj = base_product.get('buyPrice', {})
                    if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                        buy_price = buy_price_obj.get('value', 0)
                        source = f"основной товар (арт. {article})"
        
        # Для модификации
        elif item_type == 'variant':
            # Сначала пробуем buyPrice модификации
            buy_price_obj = item.get('buyPrice', {})
            if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                buy_price = buy_price_obj.get('value', 0)
                source = "модификация"
            else:
                # Если нет - берём с базового товара
                product_meta = item.get('product', {}).get('meta', {})
                if product_meta:
                    product_href = product_meta.get('href')
                    base_product = self.api.get_item_by_href(product_href)
                    
                    if base_product:
                        buy_price_obj = base_product.get('buyPrice', {})
                        if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                            buy_price = buy_price_obj.get('value', 0)
                            source = "базовый товар модификации"
        
        # Сохраняем в кэш
        result = {
            'value': buy_price,
            'source': source
        }
        self.buy_price_cache[item_id] = result
        
        return result
    
    def get_total_stock(self, item_id, item_type, moment=None):
        """Получить общий остаток по всем складам"""
        
        cache_key = f"{item_type}_{item_id}_{moment}"
        if cache_key in self.stock_cache:
            return self.stock_cache[cache_key]
        
        # Для bundle нельзя получить остатки напрямую
        if item_type == 'bundle':
            result = {
                'stock': 0,
                'reserve': 0,
                'available': 0
            }
            self.stock_cache[cache_key] = result
            return result
        
        stock_data = self.api.get_stock_by_store(item_id, item_type, moment)
        
        if not stock_data or 'rows' not in stock_data or len(stock_data['rows']) == 0:
            return {
                'stock': 0,
                'reserve': 0,
                'available': 0
            }
        
        total_stock = 0
        total_reserve = 0
        
        for row in stock_data['rows']:
            stock_by_store = row.get('stockByStore', [])
            for store in stock_by_store:
                total_stock += store.get('stock', 0)
                total_reserve += store.get('reserve', 0)
        
        result = {
            'stock': total_stock,
            'reserve': total_reserve,
            'available': total_stock - total_reserve
        }
        
        self.stock_cache[cache_key] = result
        return result
    
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
        
        # Дополнительные поля (маркетплейсы, комиссии)
        attributes = order.get("attributes", [])
        for attr in attributes:
            attr_name = attr.get("name", "")
            attr_value = attr.get("value", "")
            order_data[attr_name] = attr_value
        
        if order.get("shipmentAddress"):
            order_data["Адрес доставки"] = order.get("shipmentAddress")
        
        return order_data
    
    def extract_positions_data(self, order, order_positions):
        """Извлечь данные о позициях заказа"""
        positions_data = []
        
        order_name = order.get("name", "")
        order_date = order.get("moment", "")
        
        if not order_positions or not order_positions.get('rows'):
            return positions_data
        
        for pos in order_positions['rows']:
            assortment_meta = pos.get('assortment', {}).get('meta', {})
            assortment_href = assortment_meta.get('href', '')
            item_type = assortment_meta.get('type', 'product')
            item_id = assortment_href.split('/')[-1] if assortment_href else None
            
            if not item_id:
                continue
            
            # Получаем полную информацию о товаре
            item = self.api.get_item_by_href(assortment_href)
            
            if not item:
                continue
            
            position_data = {
                "Номер заказа": order_name,
                "Дата заказа": order_date,
                "Товар": item.get("name", ""),
                "Артикул": item.get("article", ""),
                "Код": item.get("code", ""),
                "Тип": item_type,
                "Количество": pos.get("quantity", 0),
                "Цена": pos.get("price", 0) / 100,
                "Скидка %": pos.get("discount", 0),
                "НДС %": pos.get("vat", 0),
                "Сумма": (pos.get("price", 0) * pos.get("quantity", 0)) / 100,
            }
            
            # Получаем цену закупки (себестоимость)
            buy_price_info = self.get_buy_price_for_item(item, item_type)
            cost_per_unit = buy_price_info['value'] / 100 if buy_price_info['value'] > 0 else 0
            
            position_data["Себестоимость ед."] = round(cost_per_unit, 2)
            position_data["Себестоимость общая"] = round(cost_per_unit * pos.get("quantity", 0), 2)
            position_data["Источник себестоимости"] = buy_price_info['source']
            
            # Получаем остатки по всем складам
            stock_info = self.get_total_stock(item_id, item_type, order_date)
            position_data["Остаток (все склады)"] = stock_info.get("stock", 0)
            position_data["Резерв (все склады)"] = stock_info.get("reserve", 0)
            position_data["Доступно (все склады)"] = stock_info.get("available", 0)
            
            # Расчёт маржи
            if cost_per_unit > 0:
                revenue = (pos.get("price", 0) * pos.get("quantity", 0)) / 100
                cost = cost_per_unit * pos.get("quantity", 0)
                profit = revenue - cost
                
                if revenue > 0:
                    margin_percent = (profit / revenue * 100)
                else:
                    margin_percent = 0
                
                position_data["Прибыль"] = round(profit, 2)
                position_data["Маржа %"] = round(margin_percent, 2)
            else:
                position_data["Прибыль"] = 0
                position_data["Маржа %"] = 0
            
            positions_data.append(position_data)
        
        return positions_data
    
    def calculate_order_summary(self, order, order_positions):
        """Рассчитать сводку по заказу"""
        
        total_revenue = order.get("sum", 0) / 100
        total_cost = 0
        total_quantity = 0
        
        order_date = order.get("moment", "")
        
        if not order_positions or not order_positions.get('rows'):
            return {
                "Номер заказа": order.get("name", ""),
                "Выручка": round(total_revenue, 2),
                "Себестоимость": 0,
                "Комиссия МП": 0,
                "НДС": order.get("vatSum", 0) / 100,
                "Чистая прибыль": 0,
                "Рентабельность %": 0,
                "Количество товаров": 0,
                "Себестоимость доступна": "Нет"
            }
        
        for pos in order_positions['rows']:
            assortment_meta = pos.get('assortment', {}).get('meta', {})
            assortment_href = assortment_meta.get('href', '')
            item_type = assortment_meta.get('type', 'product')
            
            # Получаем информацию о товаре
            item = self.api.get_item_by_href(assortment_href)
            
            if item:
                buy_price_info = self.get_buy_price_for_item(item, item_type)
                cost_per_unit = buy_price_info['value'] / 100 if buy_price_info['value'] > 0 else 0
                total_cost += cost_per_unit * pos.get("quantity", 0)
            
            total_quantity += pos.get("quantity", 0)
        
        # Комиссия маркетплейса из дополнительных полей
        marketplace_commission = 0
        commission_percent = 0
        attributes = order.get("attributes", [])
        
        for attr in attributes:
            attr_name = attr.get("name", "").lower()
            if "комисс" in attr_name:
                if "%" in attr_name:
                    commission_percent = float(attr.get("value", 0) or 0)
                elif "сумма" in attr_name:
                    marketplace_commission = float(attr.get("value", 0) or 0)
        
        if commission_percent > 0 and marketplace_commission == 0:
            marketplace_commission = total_revenue * (commission_percent / 100)
        
        vat_sum = order.get("vatSum", 0) / 100
        
        # Чистая прибыль
        net_profit = total_revenue - total_cost - marketplace_commission - vat_sum
        
        # Рентабельность
        if total_revenue > 0:
            margin_percent = (net_profit / total_revenue * 100)
        else:
            margin_percent = 0
        
        summary = {
            "Номер заказа": order.get("name", ""),
            "Выручка": round(total_revenue, 2),
            "Себестоимость": round(total_cost, 2),
            "Комиссия МП": round(marketplace_commission, 2),
            "НДС": round(vat_sum, 2),
            "Чистая прибыль": round(net_profit, 2),
            "Рентабельность %": round(margin_percent, 2),
            "Количество товаров": total_quantity,
            "Себестоимость доступна": "Да" if total_cost > 0 else "Нет"
        }
        
        return summary


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
            
            # Жирный шрифт для заголовков
            worksheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'horizontalAlignment': 'CENTER',
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            worksheet.freeze(rows=1)
        except Exception as e:
            print(f"⚠️  Ошибка форматирования листа {worksheet_name}: {e}")


def analyze_data_quality(df_orders, df_positions, df_summary):
    """Анализ качества данных"""
    
    print("\n" + "=" * 60)
    print("📊 АНАЛИЗ КАЧЕСТВА ДАННЫХ")
    print("=" * 60)
    
    # Анализ заказов
    print("\n1️⃣ ЗАКАЗЫ:")
    print(f"   Всего заказов: {len(df_orders)}")
    if len(df_orders) > 0:
        print(f"   Временной диапазон: {df_orders['Дата'].min()} - {df_orders['Дата'].max()}")
    
    # Анализ позиций
    print("\n2️⃣ ПОЗИЦИИ:")
    print(f"   Всего позиций: {len(df_positions)}")
    
    # Проверка себестоимости
    cost_percentage = 0
    if 'Себестоимость ед.' in df_positions.columns and len(df_positions) > 0:
        positions_with_cost = df_positions[df_positions['Себестоимость ед.'] > 0]
        positions_without_cost = df_positions[df_positions['Себестоимость ед.'] == 0]
        
        cost_percentage = (len(positions_with_cost) / len(df_positions) * 100) if len(df_positions) > 0 else 0
        
        print(f"   ✅ Позиций с себестоимостью: {len(positions_with_cost)} ({cost_percentage:.1f}%)")
        print(f"   ⚠️  Позиций БЕЗ себестоимости: {len(positions_without_cost)} ({100-cost_percentage:.1f}%)")
        
        # Источники себестоимости
        if 'Источник себестоимости' in df_positions.columns:
            print(f"\n   📋 Источники себестоимости:")
            sources = df_positions[df_positions['Себестоимость ед.'] > 0]['Источник себестоимости'].value_counts()
            for source, count in sources.items():
                print(f"      • {source}: {count} позиций")
    
    # Анализ сводки
    print("\n3️⃣ СВОДКА ПО ЗАКАЗАМ:")
    cost_orders_percentage = 0
    if 'Себестоимость доступна' in df_summary.columns and len(df_summary) > 0:
        orders_with_cost = df_summary[df_summary['Себестоимость доступна'] == 'Да']
        orders_without_cost = df_summary[df_summary['Себестоимость доступна'] == 'Нет']
        
        cost_orders_percentage = (len(orders_with_cost) / len(df_summary) * 100) if len(df_summary) > 0 else 0
        
        print(f"   ✅ Заказов с себестоимостью: {len(orders_with_cost)} ({cost_orders_percentage:.1f}%)")
        print(f"   ⚠️  Заказов БЕЗ себестоимости: {len(orders_without_cost)} ({100-cost_orders_percentage:.1f}%)")
    
    # Финансовый анализ
    if len(df_summary) > 0:
        print("\n4️⃣ ФИНАНСОВЫЙ АНАЛИЗ:")
        print(f"   Общая выручка: {df_summary['Выручка'].sum():,.2f} руб.")
        print(f"   Общая себестоимость: {df_summary['Себестоимость'].sum():,.2f} руб.")
        print(f"   Комиссии МП: {df_summary['Комиссия МП'].sum():,.2f} руб.")
        print(f"   НДС: {df_summary['НДС'].sum():,.2f} руб.")
        print(f"   Чистая прибыль: {df_summary['Чистая прибыль'].sum():,.2f} руб.")
        
        avg_margin = df_summary['Рентабельность %'].mean()
        print(f"   Средняя рентабельность: {avg_margin:.2f}%")
        
        # Топ-5 прибыльных заказов
        if len(df_summary) >= 5:
            print("\n5️⃣ ТОП-5 ПРИБЫЛЬНЫХ ЗАКАЗОВ:")
            top_profitable = df_summary.nlargest(5, 'Чистая прибыль')[['Номер заказа', 'Выручка', 'Чистая прибыль', 'Рентабельность %']]
            print(top_profitable.to_string(index=False))
    
    print("\n" + "=" * 60)
    
    return {
        'positions_with_cost_percent': cost_percentage,
        'orders_with_cost_percent': cost_orders_percentage
    }


def main():
    """Основная функция"""
    
    # ===== НАСТРОЙКИ =====
    MOYSKLAD_TOKEN = "105d4f38eb9a02400c3a6428ea71640babe37e98"
    GOOGLE_CREDENTIALS_FILE = "credentials.json"
    SPREADSHEET_NAME = "Финансовый отчёт"
    
    # Период для загрузки (последние 30 дней)
    date_to = datetime.now()
    date_from = date_to - timedelta(days=30)
    
    DATE_FROM = date_from.strftime("%Y-%m-%d 00:00:00")
    DATE_TO = date_to.strftime("%Y-%m-%d 23:59:59")
    
    print("=" * 60)
    print("ЗАГРУЗКА ДАННЫХ ИЗ МОЙСКЛАД В GOOGLE SHEETS")
    print("=" * 60)
    print(f"Период: {DATE_FROM} - {DATE_TO}")
    print()
    
    # Инициализация
    api = MoySkladAPI(MOYSKLAD_TOKEN)
    
    # Получаем заказы
    print("📦 Загрузка заказов...")
    orders = api.get_all_orders(DATE_FROM, DATE_TO)
    
    if not orders:
        print("❌ Заказы не найдены!")
        return
    
    # Обработка данных
    processor = OrderProcessor(api)
    
    # 1. Общая информация о заказах
    print("\n📋 Обработка общей информации о заказах...")
    orders_data = []
    for order in orders:
        orders_data.append(processor.extract_order_data(order))
    df_orders = pd.DataFrame(orders_data)
    
    # 2. Позиции с полной информацией
    print("\n📦 Обработка позиций заказов...")
    print("(Получение остатков по всем складам и цен закупки...)")
    print("(Это может занять некоторое время...)\n")
    
    all_positions = []
    
    for idx, order in enumerate(orders):
        if (idx + 1) % 50 == 0:
            print(f"  Обработано заказов: {idx + 1}/{len(orders)}")
        
        order_id = order.get('id')
        order_positions = api.get_order_positions(order_id)
        
        if order_positions:
            positions = processor.extract_positions_data(order, order_positions)
            all_positions.extend(positions)
    
    print(f"  Обработано заказов: {len(orders)}/{len(orders)}")
    df_positions = pd.DataFrame(all_positions)
    
    # 3. Сводка с прибылью
    print("\n💰 Расчёт сводки по заказам...")
    summaries = []
    
    for order in orders:
        order_id = order.get('id')
        order_positions = api.get_order_positions(order_id)
        summary = processor.calculate_order_summary(order, order_positions)
        summaries.append(summary)
    
    df_summary = pd.DataFrame(summaries)
    
    # 4. Статистика
    print("\n📊 Создание агрегированной статистики...")
    
    if not df_summary.empty:
        total_stats = {
            "Метрика": [
                "Общая выручка",
                "Общая себестоимость",
                "Комиссии МП",
                "НДС",
                "Чистая прибыль",
                "Средняя рентабельность %",
                "Количество заказов",
                "Средний чек"
            ],
            "Значение": [
                round(df_summary["Выручка"].sum(), 2),
                round(df_summary["Себестоимость"].sum(), 2),
                round(df_summary["Комиссия МП"].sum(), 2),
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
    
    # Анализ качества данных
    quality_report = analyze_data_quality(df_orders, df_positions, df_summary)
    
    # Сохранение локальных копий
    print("\n💾 Сохранение локальных копий...")
    df_orders.to_csv("orders.csv", index=False, encoding='utf-8-sig')
    df_positions.to_csv("positions.csv", index=False, encoding='utf-8-sig')
    df_summary.to_csv("summary.csv", index=False, encoding='utf-8-sig')
    print("✓ orders.csv")
    print("✓ positions.csv")
    print("✓ summary.csv")
    
    # Загрузка в Google Sheets
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
        
        print("\n🎨 Применение форматирования...")
        for sheet_name in ["Заказы общая информация", "Позиции детально", "Сводка с прибылью", "Общая статистика"]:
            uploader.format_worksheet(sheet_name)
        
        print("\n" + "=" * 60)
        print("✅ УСПЕШНО! Данные загружены в Google Sheets")
        print(f"📊 Таблица: {SPREADSHEET_NAME}")
        print("=" * 60)
        
        # Итоговая статистика
        if not df_stats.empty:
            print("\n📊 КРАТКАЯ СТАТИСТИКА:")
            print(df_stats.to_string(index=False))
        
    except FileNotFoundError:
        print(f"\n❌ Файл {GOOGLE_CREDENTIALS_FILE} не найден!")
        print("   Создайте credentials.json из Google Cloud Console")
        print("   Данные сохранены в CSV файлы")
        
    except Exception as e:
        print(f"\n❌ Ошибка при загрузке в Google Sheets: {e}")
        print("\n💾 Данные сохранены локально в CSV файлы")


if __name__ == "__main__":
    main()