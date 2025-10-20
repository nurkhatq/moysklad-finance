#!/usr/bin/env python3
"""
Скрипт автоматической синхронизации для GitHub Actions
"""

import sys
import json
import os
import logging
from datetime import datetime, timedelta
import requests
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
import time
from collections import defaultdict

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


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
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка запроса {endpoint}: {e}")
            return None
    
    def get_all_orders(self, date_from=None, date_to=None):
        all_orders = []
        offset = 0
        limit = 100
        
        while True:
            logger.info(f"Загрузка заказов: offset {offset}...")
            
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
        
        logger.info(f"✅ Загружено заказов: {len(all_orders)}")
        return all_orders


class OrderProcessor:
    """Класс для обработки заказов"""
    
    def extract_commission_and_delivery(self, order):
        commission_percent = 0
        commission_sum = 0
        delivery_cost = 0
        
        attributes = order.get("attributes", [])
        
        for attr in attributes:
            attr_name = attr.get("name", "")
            attr_value = attr.get("value")
            
            if attr_value is None or attr_value == "":
                continue
            
            try:
                attr_value = float(attr_value)
            except:
                continue
            
            attr_name_lower = attr_name.lower()
            
            if "комисс" in attr_name_lower and "товар" in attr_name_lower:
                commission_sum += attr_value
            elif "комисс" in attr_name_lower and "доставк" in attr_name_lower:
                commission_sum += attr_value
            elif "комисс" in attr_name_lower:
                if "%" in attr_name:
                    commission_percent = attr_value
                elif "сумма" in attr_name_lower:
                    commission_sum += attr_value
            
            if "доставк" in attr_name_lower or "delivery" in attr_name_lower:
                if "комисс" not in attr_name_lower:
                    if any(word in attr_name_lower for word in ["стоимость", "сумма", "цена", "cost"]):
                        delivery_cost += attr_value
        
        order_sum = order.get("sum", 0) / 100
        if commission_percent > 0 and commission_sum == 0:
            commission_sum = order_sum * (commission_percent / 100)
        
        return {
            'commission_percent': commission_percent,
            'commission_sum': commission_sum,
            'delivery_cost': delivery_cost
        }
    
    def get_buy_price_for_item(self, item, item_type, products_cache):
        buy_price = 0
        source = "не найдена"
        
        if item_type == 'product':
            buy_price_obj = item.get('buyPrice', {})
            if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                buy_price = buy_price_obj.get('value', 0)
                source = "товар"
        elif item_type == 'bundle':
            article = item.get('article')
            if article and article in products_cache:
                base_product = products_cache[article]
                buy_price_obj = base_product.get('buyPrice', {})
                if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                    buy_price = buy_price_obj.get('value', 0)
                    source = f"основной товар (арт. {article})"
        elif item_type == 'variant':
            buy_price_obj = item.get('buyPrice', {})
            if buy_price_obj and buy_price_obj.get('value', 0) > 0:
                buy_price = buy_price_obj.get('value', 0)
                source = "модификация"
        
        return {'value': buy_price, 'source': source}
    
    def process_orders(self, orders, products_cache):
        orders_data = []
        positions_data = []
        summaries = []
        
        logger.info(f"Обработка {len(orders)} заказов...")
        
        for idx, order in enumerate(orders):
            if (idx + 1) % 100 == 0:
                logger.info(f"  Обработано: {idx + 1}/{len(orders)}")
            
            order_data = self.extract_order_data(order)
            orders_data.append(order_data)
            
            costs = self.extract_commission_and_delivery(order)
            positions = order.get('positions', {}).get('rows', [])
            total_positions = len(positions)
            order_sum = order.get("sum", 0) / 100
            order_cost = 0
            
            for pos in positions:
                assortment = pos.get('assortment', {})
                item_type = assortment.get('meta', {}).get('type', 'product')
                
                buy_price_info = self.get_buy_price_for_item(assortment, item_type, products_cache)
                cost_per_unit = buy_price_info['value'] / 100 if buy_price_info['value'] > 0 else 0
                
                quantity = pos.get("quantity", 0)
                price = pos.get("price", 0) / 100
                position_sum = price * quantity
                
                position_share = (position_sum / order_sum) if order_sum > 0 else (1 / total_positions if total_positions > 0 else 0)
                
                delivery_per_position = costs['delivery_cost'] * position_share
                commission_per_position = costs['commission_sum'] * position_share
                product_cost = cost_per_unit * quantity
                full_cost = product_cost + delivery_per_position + commission_per_position
                order_cost += full_cost
                
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
        
        logger.info(f"✅ Обработка завершена")
        return orders_data, positions_data, summaries
    
    @staticmethod
    def extract_order_data(order):
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
        
        # ИЗМЕНИТЬ ЭТИ СТРОКИ:
        creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open(spreadsheet_name)
    
    def get_existing_orders(self, worksheet_name):
        """Получить существующие номера заказов"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_values()
            if len(data) > 1:
                return set(row[0] for row in data[1:] if row[0])
            return set()
        except:
            return set()
    
    def upload_dataframe(self, df, worksheet_name, mode="replace"):
        """Загрузить DataFrame"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=50)
        
        df_clean = df.copy()
        df_clean = df_clean.replace([float('inf'), float('-inf')], 0)
        df_clean = df_clean.fillna('')
        
        if mode == "replace":
            worksheet.clear()
            start_row = 1
        elif mode == "append":
            existing_data = worksheet.get_all_values()
            start_row = len(existing_data) + 1
        else:  # update
            existing_orders = self.get_existing_orders(worksheet_name)
            existing_data = worksheet.get_all_values()
            
            if "Номер заказа" in df_clean.columns:
                new_df = df_clean[~df_clean["Номер заказа"].isin(existing_orders)]
                if not new_df.empty:
                    start_row = len(existing_data) + 1
                    df_clean = new_df
                else:
                    logger.info(f"Нет новых данных для {worksheet_name}")
                    return 0
            else:
                worksheet.clear()
                start_row = 1
        
        data = []
        if mode == "replace" or start_row == 1:
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
        
        if data:
            worksheet.update(values=data, range_name=f'A{start_row}')
        
        logger.info(f"✅ {worksheet_name}: загружено {len(df_clean)} строк")
        return len(df_clean)
    
    def format_worksheet(self, worksheet_name):
        """Применить форматирование"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            worksheet.format('A1:Z1', {
                'textFormat': {'bold': True},
                'horizontalAlignment': 'CENTER',
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            worksheet.freeze(rows=1)
        except Exception as e:
            logger.warning(f"Ошибка форматирования {worksheet_name}: {e}")


def load_config():
    """Загрузить конфигурацию"""
    if os.path.exists('config.json'):
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    
    logger.error("Файл config.json не найден!")
    sys.exit(1)


def load_sync_state():
    """Загрузить состояние синхронизации"""
    if os.path.exists('sync_state.json'):
        with open('sync_state.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        "last_sync": None,
        "synced_orders": []
    }


def save_sync_state(state):
    """Сохранить состояние синхронизации"""
    with open('sync_state.json', 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4, ensure_ascii=False)


def main():
    """Основная функция"""
    logger.info("=" * 60)
    logger.info("АВТОМАТИЧЕСКАЯ СИНХРОНИЗАЦИЯ МОЙСКЛАД")
    logger.info("=" * 60)
    
    # Загрузка конфигурации
    config = load_config()
    sync_state = load_sync_state()
    
    # Режим синхронизации из переменной окружения
    sync_mode = os.environ.get('SYNC_MODE', 'update')
    
    logger.info(f"Режим синхронизации: {sync_mode}")
    logger.info(f"Период: {config['days_back']} дней")
    
    try:
        # 1. Подключение к API
        logger.info("🔌 Подключение к МойСклад API...")
        api = MoySkladAPI(config['moysklad_token'])
        
        # 2. Определение периода
        date_to = datetime.now()
        date_from = date_to - timedelta(days=config['days_back'])
        
        date_from_str = date_from.strftime("%Y-%m-%d 00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%d 23:59:59")
        
        logger.info(f"Период: {date_from_str} - {date_to_str}")
        
        # 3. Загрузка заказов
        logger.info("📦 Загрузка заказов...")
        orders = api.get_all_orders(date_from_str, date_to_str)
        
        if not orders:
            logger.warning("⚠️ Заказы не найдены")
            return
        
        # 4. Сбор артикулов для комплектов
        logger.info("🔍 Сбор артикулов для комплектов...")
        unique_articles = set()
        
        for order in orders:
            positions = order.get('positions', {}).get('rows', [])
            for pos in positions:
                assortment = pos.get('assortment', {})
                item_type = assortment.get('meta', {}).get('type', 'product')
                article = assortment.get('article')
                
                if article and item_type == 'bundle':
                    unique_articles.add(article)
        
        logger.info(f"Найдено артикулов: {len(unique_articles)}")
        
        # 5. Загрузка товаров
        products_cache = {}
        if unique_articles:
            logger.info("📦 Загрузка товаров для комплектов...")
            for article in unique_articles:
                endpoint = "/entity/product"
                params = {"filter": f"article={article}", "limit": 1}
                
                data = api._make_request(endpoint, params)
                if data and data.get('rows'):
                    product = data['rows'][0]
                    products_cache[article] = product
                
                time.sleep(0.05)
            
            logger.info(f"Загружено товаров: {len(products_cache)}")
        
        # 6. Обработка данных
        logger.info("⚙️ Обработка данных...")
        processor = OrderProcessor()
        orders_data, positions_data, summaries = processor.process_orders(orders, products_cache)
        
        # 7. Создание DataFrame
        logger.info("📊 Создание DataFrame...")
        df_orders = pd.DataFrame(orders_data)
        df_positions = pd.DataFrame(positions_data)
        df_summary = pd.DataFrame(summaries)
        
        # 8. Статистика
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
        
        # 9. Сохранение локальных копий
        logger.info("💾 Сохранение локальных копий...")
        df_orders.to_csv("orders.csv", index=False, encoding='utf-8-sig')
        df_positions.to_csv("positions.csv", index=False, encoding='utf-8-sig')
        df_summary.to_csv("summary.csv", index=False, encoding='utf-8-sig')
        logger.info("✅ CSV файлы сохранены")
        
        # 10. Загрузка в Google Sheets
        logger.info("☁️ Загрузка в Google Sheets...")
        uploader = GoogleSheetsUploader(
            config['google_credentials_file'],
            config['spreadsheet_name']
        )
        
        uploaded_orders = uploader.upload_dataframe(df_orders, "Заказы общая информация", mode=sync_mode)
        uploaded_positions = uploader.upload_dataframe(df_positions, "Позиции детально", mode=sync_mode)
        uploaded_summary = uploader.upload_dataframe(df_summary, "Сводка с прибылью", mode=sync_mode)
        uploader.upload_dataframe(df_stats, "Общая статистика", mode="replace")
        
        # 11. Форматирование
        logger.info("🎨 Применение форматирования...")
        for sheet_name in ["Заказы общая информация", "Позиции детально", 
                          "Сводка с прибылью", "Общая статистика"]:
            uploader.format_worksheet(sheet_name)
        
        # 12. Обновление состояния
        synced_orders = sync_state.get("synced_orders", [])
        new_order_numbers = [o["Номер заказа"] for o in orders_data]
        
        if sync_mode == "update":
            synced_orders.extend([o for o in new_order_numbers if o not in synced_orders])
        elif sync_mode == "replace":
            synced_orders = new_order_numbers
        else:  # append
            synced_orders.extend(new_order_numbers)
        
        sync_state["last_sync"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sync_state["synced_orders"] = list(set(synced_orders))
        
        save_sync_state(sync_state)
        
        # 13. Итоговая статистика
        logger.info("=" * 60)
        logger.info("✅ СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА УСПЕШНО")
        logger.info("=" * 60)
        logger.info(f"Обработано заказов: {len(orders)}")
        logger.info(f"Обработано позиций: {len(df_positions)}")
        logger.info(f"Загружено в Google Sheets: {uploaded_orders} заказов")
        logger.info(f"Режим: {sync_mode}")
        logger.info("=" * 60)
        
        if not df_stats.empty:
            logger.info("\n📊 СТАТИСТИКА:")
            for _, row in df_stats.iterrows():
                logger.info(f"  {row['Метрика']}: {row['Значение']}")
        
    except Exception as e:
        logger.error(f"❌ ОШИБКА СИНХРОНИЗАЦИИ: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()