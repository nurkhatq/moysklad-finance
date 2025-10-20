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
    
    def _make_request(self, endpoint, params=None):
        """Базовый метод для запросов с обработкой ошибок"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса: {e}")
            return None
    
    def get_customer_orders(self, date_from=None, date_to=None, limit=100, offset=0):
        """Получить заказы покупателя с детализацией"""
        endpoint = "/entity/customerorder"
        
        params = {
            "limit": limit,
            "offset": offset,
            "expand": "positions,positions.assortment,agent,organization,state",
        }
        
        # Фильтры по дате
        filters = []
        if date_from:
            filters.append(f"moment>={date_from}")
        if date_to:
            filters.append(f"moment<={date_to}")
        
        if filters:
            params["filter"] = ";".join(filters)
        
        return self._make_request(endpoint, params)
    
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
    
    def get_product_stock_by_store(self, product_id, moment=None):
        """Получить остатки товара по всем складам"""
        endpoint = "/report/stock/bystore"
        params = {
            "filter": f"product=https://api.moysklad.ru/api/remap/1.2/entity/product/{product_id}"
        }
        
        if moment:
            params["moment"] = moment
        
        return self._make_request(endpoint, params)


class OrderProcessor:
    """Класс для обработки и анализа заказов"""
    
    def __init__(self, api):
        self.api = api
        self.stock_cache = {}
    
    def get_total_stock_and_cost(self, product_id, moment=None):
        """Получить общий остаток и среднюю себестоимость по всем складам"""
        
        cache_key = f"{product_id}_{moment}"
        if cache_key in self.stock_cache:
            return self.stock_cache[cache_key]
        
        stock_data = self.api.get_product_stock_by_store(product_id, moment)
        
        if not stock_data or 'rows' not in stock_data or len(stock_data['rows']) == 0:
            return None
        
        total_stock = 0
        total_reserve = 0
        total_cost_weighted = 0
        total_quantity_for_cost = 0
        
        # Суммируем по всем складам
        for row in stock_data['rows']:
            stock_by_store = row.get('stockByStore', [])
            for store_data in stock_by_store:
                stock = store_data.get('stock', 0)
                reserve = store_data.get('reserve', 0)
                cost = store_data.get('cost', 0)
                
                total_stock += stock
                total_reserve += reserve
                
                # Взвешенная себестоимость
                if cost > 0 and stock > 0:
                    total_cost_weighted += cost * stock
                    total_quantity_for_cost += stock
        
        # Средняя себестоимость
        avg_cost = 0
        if total_quantity_for_cost > 0:
            avg_cost = total_cost_weighted / total_quantity_for_cost
        
        result = {
            'stock': total_stock,
            'reserve': total_reserve,
            'available': total_stock - total_reserve,
            'cost': avg_cost
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
        
        # Дополнительные поля
        attributes = order.get("attributes", [])
        for attr in attributes:
            attr_name = attr.get("name", "")
            attr_value = attr.get("value", "")
            order_data[attr_name] = attr_value
        
        # Адрес доставки
        if order.get("shipmentAddress"):
            order_data["Адрес доставки"] = order.get("shipmentAddress")
        
        return order_data
    
    def extract_positions_data(self, order):
        """Извлечь данные о позициях заказа"""
        positions_data = []
        positions = order.get("positions", {}).get("rows", [])
        
        order_name = order.get("name", "")
        order_date = order.get("moment", "")
        
        for pos in positions:
            assortment = pos.get("assortment", {})
            
            # Получаем ID товара
            product_meta = assortment.get("meta", {})
            product_href = product_meta.get("href", "")
            product_id = product_href.split("/")[-1] if product_href else None
            
            position_data = {
                "Номер заказа": order_name,
                "Дата заказа": order_date,
                "Товар": assortment.get("name", ""),
                "Артикул": assortment.get("article", ""),
                "Код": assortment.get("code", ""),
                "Количество": pos.get("quantity", 0),
                "Цена": pos.get("price", 0) / 100,
                "Скидка %": pos.get("discount", 0),
                "НДС %": pos.get("vat", 0),
                "Сумма": (pos.get("price", 0) * pos.get("quantity", 0)) / 100,
            }
            
            # Получаем остатки и себестоимость по всем складам
            if product_id:
                stock_info = self.get_total_stock_and_cost(product_id, order_date)
                
                if stock_info and stock_info.get("cost", 0) > 0:
                    cost_per_unit = stock_info.get("cost", 0) / 100
                    position_data["Себестоимость ед."] = round(cost_per_unit, 2)
                    position_data["Себестоимость общая"] = round(cost_per_unit * pos.get("quantity", 0), 2)
                    position_data["Остаток (все склады)"] = stock_info.get("stock", 0)
                    position_data["Резерв (все склады)"] = stock_info.get("reserve", 0)
                    position_data["Доступно (все склады)"] = stock_info.get("available", 0)
                    
                    # Расчёт маржи
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
                    # Нет данных о себестоимости
                    position_data["Себестоимость ед."] = 0
                    position_data["Себестоимость общая"] = 0
                    position_data["Остаток (все склады)"] = "Нет данных"
                    position_data["Резерв (все склады)"] = "Нет данных"
                    position_data["Доступно (все склады)"] = "Нет данных"
                    position_data["Прибыль"] = 0
                    position_data["Маржа %"] = 0
            
            positions_data.append(position_data)
        
        return positions_data
    
    def calculate_order_summary(self, order):
        """Рассчитать сводку по заказу"""
        positions = order.get("positions", {}).get("rows", [])
        
        total_revenue = order.get("sum", 0) / 100
        total_cost = 0
        total_quantity = 0
        
        order_date = order.get("moment", "")
        
        for pos in positions:
            assortment = pos.get("assortment", {})
            product_meta = assortment.get("meta", {})
            product_href = product_meta.get("href", "")
            product_id = product_href.split("/")[-1] if product_href else None
            
            if product_id:
                stock_info = self.get_total_stock_and_cost(product_id, order_date)
                if stock_info and stock_info.get("cost"):
                    cost_per_unit = stock_info.get("cost", 0) / 100
                    total_cost += cost_per_unit * pos.get("quantity", 0)
            
            total_quantity += pos.get("quantity", 0)
        
        # Комиссия маркетплейса
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
        worksheet = self.spreadsheet.worksheet(worksheet_name)
        
        # Жирный шрифт для заголовков
        worksheet.format('A1:Z1', {
            'textFormat': {'bold': True},
            'horizontalAlignment': 'CENTER',
            'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
        })
        
        worksheet.freeze(rows=1)


def analyze_data_quality(df_orders, df_positions, df_summary):
    """Анализ качества данных"""
    
    print("\n" + "=" * 60)
    print("📊 АНАЛИЗ КАЧЕСТВА ДАННЫХ")
    print("=" * 60)
    
    # Анализ заказов
    print("\n1️⃣ ЗАКАЗЫ:")
    print(f"   Всего заказов: {len(df_orders)}")
    print(f"   Временной диапазон: {df_orders['Дата'].min()} - {df_orders['Дата'].max()}")
    
    # Анализ позиций
    print("\n2️⃣ ПОЗИЦИИ:")
    print(f"   Всего позиций: {len(df_positions)}")
    
    # Проверка себестоимости
    cost_percentage = 0
    if 'Себестоимость ед.' in df_positions.columns:
        positions_with_cost = df_positions[df_positions['Себестоимость ед.'] > 0]
        positions_without_cost = df_positions[df_positions['Себестоимость ед.'] == 0]
        
        cost_percentage = (len(positions_with_cost) / len(df_positions) * 100) if len(df_positions) > 0 else 0
        
        print(f"   ✅ Позиций с себестоимостью: {len(positions_with_cost)} ({cost_percentage:.1f}%)")
        print(f"   ⚠️  Позиций БЕЗ себестоимости: {len(positions_without_cost)} ({100-cost_percentage:.1f}%)")
        
        if len(positions_without_cost) > 0 and len(positions_without_cost) < 20:
            print(f"\n   ⚠️  Товары без себестоимости:")
            no_cost_products = positions_without_cost[['Товар', 'Номер заказа']].head(10)
            print(no_cost_products.to_string(index=False))
    
    # Анализ сводки
    print("\n3️⃣ СВОДКА ПО ЗАКАЗАМ:")
    cost_orders_percentage = 0
    if 'Себестоимость доступна' in df_summary.columns:
        orders_with_cost = df_summary[df_summary['Себестоимость доступна'] == 'Да']
        orders_without_cost = df_summary[df_summary['Себестоимость доступна'] == 'Нет']
        
        cost_orders_percentage = (len(orders_with_cost) / len(df_summary) * 100) if len(df_summary) > 0 else 0
        
        print(f"   ✅ Заказов с себестоимостью: {len(orders_with_cost)} ({cost_orders_percentage:.1f}%)")
        print(f"   ⚠️  Заказов БЕЗ себестоимости: {len(orders_without_cost)} ({100-cost_orders_percentage:.1f}%)")
    
    # Финансовый анализ
    print("\n4️⃣ ФИНАНСОВЫЙ АНАЛИЗ:")
    print(f"   Общая выручка: {df_summary['Выручка'].sum():,.2f} руб.")
    print(f"   Общая себестоимость: {df_summary['Себестоимость'].sum():,.2f} руб.")
    print(f"   Комиссии МП: {df_summary['Комиссия МП'].sum():,.2f} руб.")
    print(f"   НДС: {df_summary['НДС'].sum():,.2f} руб.")
    print(f"   Чистая прибыль: {df_summary['Чистая прибыль'].sum():,.2f} руб.")
    
    avg_margin = df_summary['Рентабельность %'].mean()
    print(f"   Средняя рентабельность: {avg_margin:.2f}%")
    
    # Топ-5 прибыльных заказов
    print("\n5️⃣ ТОП-5 ПРИБЫЛЬНЫХ ЗАКАЗОВ:")
    top_profitable = df_summary.nlargest(5, 'Чистая прибыль')[['Номер заказа', 'Выручка', 'Чистая прибыль', 'Рентабельность %']]
    print(top_profitable.to_string(index=False))
    
    # Топ-5 убыточных
    print("\n6️⃣ ТОП-5 УБЫТОЧНЫХ ЗАКАЗОВ:")
    top_loss = df_summary.nsmallest(5, 'Чистая прибыль')[['Номер заказа', 'Выручка', 'Чистая прибыль', 'Рентабельность %']]
    print(top_loss.to_string(index=False))
    
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
    print("Загрузка заказов...")
    orders = api.get_all_orders(DATE_FROM, DATE_TO)
    
    if not orders:
        print("Заказы не найдены!")
        return
    
    # Обработка данных
    processor = OrderProcessor(api)
    
    # 1. Общая информация о заказах
    print("\nОбработка общей информации о заказах...")
    orders_data = [processor.extract_order_data(order) for order in orders]
    df_orders = pd.DataFrame(orders_data)
    
    # 2. Позиции с остатками по всем складам
    print("Обработка позиций заказов с получением остатков по всем складам...")
    print("(Это может занять некоторое время...)")
    all_positions = []
    for idx, order in enumerate(orders):
        if (idx + 1) % 100 == 0:
            print(f"  Обработано заказов: {idx + 1}/{len(orders)}")
        positions = processor.extract_positions_data(order)
        all_positions.extend(positions)
    df_positions = pd.DataFrame(all_positions)
    
    # 3. Сводка с прибылью
    print("Расчёт сводки по заказам...")
    summaries = [processor.calculate_order_summary(order) for order in orders]
    df_summary = pd.DataFrame(summaries)
    
    # 4. Статистика
    print("Создание агрегированной статистики...")
    
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
    
    # Проверка качества
    if quality_report['positions_with_cost_percent'] < 50:
        print("\n⚠️  ПРЕДУПРЕЖДЕНИЕ: Менее 50% позиций имеют себестоимость!")
        print("    Возможные причины:")
        print("    - Товары не оприходованы на склад")
        print("    - Не указана закупочная цена")
        print("    - Товары находятся на складах без учёта себестоимости")
        
        user_input = input("\n❓ Продолжить загрузку в Google Sheets? (да/нет): ").lower()
        if user_input not in ['да', 'yes', 'y', 'д']:
            print("\nЗагрузка отменена. Данные сохранены в CSV файлы.")
            df_orders.to_csv("orders.csv", index=False, encoding='utf-8-sig')
            df_positions.to_csv("positions.csv", index=False, encoding='utf-8-sig')
            df_summary.to_csv("summary.csv", index=False, encoding='utf-8-sig')
            return
    
    # Загрузка в Google Sheets
    print("\n" + "=" * 60)
    print("ЗАГРУЗКА В GOOGLE SHEETS")
    print("=" * 60)
    
    try:
        uploader = GoogleSheetsUploader(GOOGLE_CREDENTIALS_FILE, SPREADSHEET_NAME)
        
        print("\nЗагрузка данных...")
        uploader.upload_dataframe(df_orders, "Заказы общая информация")
        uploader.upload_dataframe(df_positions, "Позиции детально")
        uploader.upload_dataframe(df_summary, "Сводка с прибылью")
        uploader.upload_dataframe(df_stats, "Общая статистика")
        
        print("\nПрименение форматирования...")
        for sheet_name in ["Заказы общая информация", "Позиции детально", "Сводка с прибылью", "Общая статистика"]:
            uploader.format_worksheet(sheet_name)
        
        print("\n" + "=" * 60)
        print("✅ УСПЕШНО! Данные загружены в Google Sheets")
        print(f"Таблица: {SPREADSHEET_NAME}")
        print("=" * 60)
        
        # Итоговая статистика
        if not df_stats.empty:
            print("\n📊 КРАТКАЯ СТАТИСТИКА:")
            print(df_stats.to_string(index=False))
        
    except Exception as e:
        print(f"\n❌ Ошибка при загрузке в Google Sheets: {e}")
        print("\nДанные сохранены локально:")
        df_orders.to_csv("orders.csv", index=False, encoding='utf-8-sig')
        df_positions.to_csv("positions.csv", index=False, encoding='utf-8-sig')
        df_summary.to_csv("summary.csv", index=False, encoding='utf-8-sig')
        print("✓ orders.csv")
        print("✓ positions.csv")
        print("✓ summary.csv")


if __name__ == "__main__":
    main()