import streamlit as st
import requests
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import json
import os
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Создаём обработчик вывода в консоль (Streamlit его подхватит)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Добавляем обработчик к логгеру (если ещё не добавлен)
if not logger.handlers:
    logger.addHandler(handler)

# Конфигурация страницы
st.set_page_config(
    page_title="МойСклад - Финансовый Отчёт",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Кастомный CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffeeba;
        border-radius: 5px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .stButton>button {
        width: 100%;
        background-color: #1f77b4;
        color: white;
        font-weight: bold;
        border-radius: 5px;
        padding: 0.5rem 1rem;
    }
</style>
""", unsafe_allow_html=True)

# Конфигурационные файлы
CONFIG_FILE = "config.json"
FIELDS_CONFIG_FILE = "fields_config.json"
SYNC_STATE_FILE = "sync_state.json"

# Дефолтные поля для каждой таблицы
DEFAULT_FIELDS = {
    "orders": [
        "Номер заказа", "Дата", "Контрагент", "Организация", "Статус",
        "Применён", "Сумма заказа", "Оплачено", "Отгружено", "НДС"
    ],
    "positions": [
        "Номер заказа", "Дата заказа", "Товар", "Артикул", "Количество",
        "Цена", "Сумма", "Себестоимость товара ед.", "ПОЛНАЯ себестоимость",
        "Прибыль", "Маржа %"
    ],
    "summary": [
        "Номер заказа", "Выручка", "Себестоимость товаров", "Доставка",
        "Комиссия МП", "ПОЛНАЯ себестоимость", "НДС", "Чистая прибыль",
        "Рентабельность %"
    ]
}

class ConfigManager:
    """Управление конфигурацией"""
    
    @staticmethod
    def load_config():
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {
            "moysklad_token": "",
            "google_credentials_file": "credentials.json",
            "spreadsheet_name": "Финансовый отчёт",
            "sync_schedule": "daily",
            "sync_time": "09:00",
            "days_back": 30
        }
    
    @staticmethod
    def save_config(config):
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
    
    @staticmethod
    def load_fields_config():
        if os.path.exists(FIELDS_CONFIG_FILE):
            with open(FIELDS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return DEFAULT_FIELDS
    
    @staticmethod
    def save_fields_config(fields):
        with open(FIELDS_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(fields, f, indent=4, ensure_ascii=False)
    
    @staticmethod
    def load_sync_state():
        if os.path.exists(SYNC_STATE_FILE):
            with open(SYNC_STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {
            "last_sync": None,
            "synced_orders": []
        }
    
    @staticmethod
    def save_sync_state(state):
        with open(SYNC_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(state, f, indent=4, ensure_ascii=False)


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
            st.error(f"Ошибка запроса {endpoint}: {e}")
            return None
    
    def get_all_orders(self, date_from=None, date_to=None, progress_callback=None):
        all_orders = []
        offset = 0
        limit = 100
        
        while True:
            if progress_callback:
                progress_callback(f"Загрузка заказов: offset {offset}...")
            
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
    
    def process_orders(self, orders, products_cache, progress_callback=None):
        orders_data = []
        positions_data = []
        summaries = []
        
        for idx, order in enumerate(orders):
            if progress_callback and (idx + 1) % 50 == 0:
                progress_callback(f"Обработано: {idx + 1}/{len(orders)}")
            
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

        # 🔄 Вместо файла берём данные из Streamlit Secrets
        if "google_credentials" in st.secrets:
            creds_dict = st.secrets["google_credentials"]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
        else:
            # fallback — если вдруг ты тестируешь локально
            creds = Credentials.from_service_account_file(credentials_file, scopes=scope)

        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open(spreadsheet_name)
    
    def get_existing_orders(self, worksheet_name):
        """Получить существующие номера заказов"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_values()
            if len(data) > 1:
                # Предполагаем, что первая колонка - "Номер заказа"
                return set(row[0] for row in data[1:] if row[0])
            return set()
        except:
            return set()
    
    def upload_dataframe(self, df, worksheet_name, selected_fields=None, mode="replace"):
        """Загрузить DataFrame с выбранными полями"""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=50)
        
        # Фильтрация полей
        if selected_fields:
            available_fields = [f for f in selected_fields if f in df.columns]
            df = df[available_fields]
        
        # Очистка данных
        df_clean = df.copy()
        df_clean = df_clean.replace([float('inf'), float('-inf')], 0)
        df_clean = df_clean.fillna('')

        existing_data = worksheet.get_all_values()
        is_empty = len(existing_data) == 0

        if mode == "replace":
            worksheet.clear()
            start_row = 1
            include_header = True
        elif mode == "append":
            start_row = len(existing_data) + 1
            include_header = is_empty  # только если лист пустой
        else:  # update
            existing_orders = self.get_existing_orders(worksheet_name)
            if "Номер заказа" in df_clean.columns:
                new_df = df_clean[~df_clean["Номер заказа"].isin(existing_orders)]
                if new_df.empty:
                    logger.info(f"Нет новых данных для {worksheet_name}")
                    return 0
                df_clean = new_df
                start_row = len(existing_data) + 1
                include_header = False
            else:
                worksheet.clear()
                start_row = 1
                include_header = True

        # Подготовка данных
        data = []
        if include_header:
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
            st.warning(f"Ошибка форматирования: {e}")


def main():
    """Основная функция"""
    st.markdown('<h1 class="main-header">📊 МойСклад - Финансовый Отчёт</h1>', unsafe_allow_html=True)
    
    # Sidebar - Навигация
    st.sidebar.title("📋 Навигация")
    page = st.sidebar.radio("Выберите раздел:", 
                            ["🏠 Главная", "⚙️ Настройки", "📊 Управление полями", 
                             "🔄 Синхронизация", "📈 Статистика"])
    
    config = ConfigManager.load_config()
    fields_config = ConfigManager.load_fields_config()
    sync_state = ConfigManager.load_sync_state()
    
    # ==================== ГЛАВНАЯ ====================
    if page == "🏠 Главная":
        st.subheader("Добро пожаловать!")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("📦 Последняя синхронизация", 
                     sync_state.get("last_sync", "Никогда") or "Никогда")
        
        with col2:
            st.metric("📋 Синхронизировано заказов", 
                     len(sync_state.get("synced_orders", [])))
        
        with col3:
            st.metric("⏰ Расписание", 
                     f"{config.get('sync_schedule', 'daily')} в {config.get('sync_time', '09:00')}")
        
        st.markdown("---")
        
        # Быстрая синхронизация
        st.subheader("🚀 Быстрая загрузка данных")
        
        col1, col2 = st.columns(2)
        
        with col1:
            days = st.number_input("За последние дней:", 
                                  min_value=1, max_value=365, 
                                  value=config.get("days_back", 30))
        
        with col2:
            st.write("")
            st.write("")
            if st.button("▶️ Запустить загрузку", use_container_width=True):
                run_sync(config, fields_config, sync_state, days)
    
    # ==================== НАСТРОЙКИ ====================
    elif page == "⚙️ Настройки":
        st.subheader("⚙️ Настройки подключения")
        
        tab1, tab2, tab3 = st.tabs(["🔑 API", "📊 Google Sheets", "⏰ Автосинхронизация"])
        
        with tab1:
            st.markdown("### МойСклад API")
            token = st.text_input("Токен API:", 
                                 value=config.get("moysklad_token", ""),
                                 type="password")
            
            if st.button("🧪 Проверить подключение"):
                if token:
                    api = MoySkladAPI(token)
                    test = api._make_request("/entity/customerorder", {"limit": 1})
                    if test:
                        st.success("✅ Подключение успешно!")
                    else:
                        st.error("❌ Ошибка подключения")
                else:
                    st.warning("⚠️ Введите токен")
            
            config["moysklad_token"] = token
        
        with tab2:
            st.markdown("### Google Sheets")
            creds_file = st.text_input("Файл credentials:", 
                                       value=config.get("google_credentials_file", "credentials.json"))
            
            spreadsheet = st.text_input("Название таблицы:", 
                                       value=config.get("spreadsheet_name", "Финансовый отчёт"))
            
            config["google_credentials_file"] = creds_file
            config["spreadsheet_name"] = spreadsheet
        
        with tab3:
            st.markdown("### Расписание синхронизации")
            
            schedule = st.selectbox("Частота:",
                                   ["daily", "weekly", "manual"],
                                   index=["daily", "weekly", "manual"].index(config.get("sync_schedule", "daily")))
            
            sync_time = st.time_input("Время запуска (для GitHub Actions):",
                                     value=datetime.strptime(config.get("sync_time", "09:00"), "%H:%M").time())
            
            days_back = st.number_input("Загружать за последние дней:",
                                       min_value=1, max_value=365,
                                       value=config.get("days_back", 30))
            
            config["sync_schedule"] = schedule
            config["sync_time"] = sync_time.strftime("%H:%M")
            config["days_back"] = days_back
        
        if st.button("💾 Сохранить настройки", use_container_width=True):
            ConfigManager.save_config(config)
            st.success("✅ Настройки сохранены!")
            st.rerun()
    
    # ==================== УПРАВЛЕНИЕ ПОЛЯМИ ====================
    elif page == "📊 Управление полями":
        st.subheader("📊 Управление полями отчёта")
        
        tab1, tab2, tab3 = st.tabs(["📦 Заказы", "📋 Позиции", "💰 Сводка"])
        
        with tab1:
            st.markdown("### Поля таблицы 'Заказы'")
            all_fields = DEFAULT_FIELDS["orders"] + list(set(fields_config.get("orders", DEFAULT_FIELDS["orders"])) - set(DEFAULT_FIELDS["orders"]))
            
            selected = st.multiselect(
                "Выберите поля для отображения:",
                all_fields,
                default=fields_config.get("orders", DEFAULT_FIELDS["orders"])
            )
            
            fields_config["orders"] = selected
        
        with tab2:
            st.markdown("### Поля таблицы 'Позиции'")
            all_fields = DEFAULT_FIELDS["positions"] + list(set(fields_config.get("positions", DEFAULT_FIELDS["positions"])) - set(DEFAULT_FIELDS["positions"]))
            
            selected = st.multiselect(
                "Выберите поля для отображения:",
                all_fields,
                default=fields_config.get("positions", DEFAULT_FIELDS["positions"]),
                key="pos"
            )
            
            fields_config["positions"] = selected
        
        with tab3:
            st.markdown("### Поля таблицы 'Сводка'")
            all_fields = DEFAULT_FIELDS["summary"] + list(set(fields_config.get("summary", DEFAULT_FIELDS["summary"])) - set(DEFAULT_FIELDS["summary"]))
            
            selected = st.multiselect(
                "Выберите поля для отображения:",
                all_fields,
                default=fields_config.get("summary", DEFAULT_FIELDS["summary"]),
                key="sum"
            )
            
            fields_config["summary"] = selected
        
        if st.button("💾 Сохранить конфигурацию полей", use_container_width=True):
            ConfigManager.save_fields_config(fields_config)
            st.success("✅ Конфигурация полей сохранена!")
    
    # ==================== СИНХРОНИЗАЦИЯ ====================
    elif page == "🔄 Синхронизация":
        st.subheader("🔄 Управление синхронизацией")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"**Последняя синхронизация:**\n\n{sync_state.get('last_sync', 'Никогда')}")
        
        with col2:
            st.info(f"**Синхронизировано заказов:**\n\n{len(sync_state.get('synced_orders', []))}")
        
        st.markdown("---")
        
        sync_mode = st.radio("Режим синхронизации:",
                            ["🔄 Обновление (только новые заказы)",
                             "🔃 Полная замена данных",
                             "➕ Добавление к существующим"])
        
        date_col1, date_col2 = st.columns(2)
        
        with date_col1:
            date_from = st.date_input("Дата начала:",
                                     value=datetime.now() - timedelta(days=config.get("days_back", 30)))
        
        with date_col2:
            date_to = st.date_input("Дата окончания:",
                                   value=datetime.now())
        
        if st.button("🚀 Запустить синхронизацию", use_container_width=True):
            mode_map = {
                "🔄 Обновление (только новые заказы)": "update",
                "🔃 Полная замена данных": "replace",
                "➕ Добавление к существующим": "append"
            }
            
            run_sync(config, fields_config, sync_state, 
                    days=(date_to - date_from).days,
                    mode=mode_map[sync_mode],
                    date_from=date_from,
                    date_to=date_to)
    
    # ==================== СТАТИСТИКА ====================
    elif page == "📈 Статистика":
        st.subheader("📈 Статистика и анализ")
        
        if not config.get("moysklad_token") or not config.get("google_credentials_file"):
            st.warning("⚠️ Настройте подключения в разделе 'Настройки'")
            return
        
        try:
            uploader = GoogleSheetsUploader(
                config["google_credentials_file"],
                config["spreadsheet_name"]
            )
            
            # Загружаем данные из Google Sheets
            try:
                worksheet = uploader.spreadsheet.worksheet("Общая статистика")
                stats_data = worksheet.get_all_records()
                
                if stats_data:
                    df_stats = pd.DataFrame(stats_data)
                    
                    st.markdown("### 💰 Основные показатели")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    for idx, row in df_stats.iterrows():
                        metric = row.get("Метрика", "")
                        value = row.get("Значение", 0)
                        
                        if idx % 4 == 0:
                            col1.metric(metric, f"{value:,.2f}")
                        elif idx % 4 == 1:
                            col2.metric(metric, f"{value:,.2f}")
                        elif idx % 4 == 2:
                            col3.metric(metric, f"{value:,.2f}")
                        else:
                            col4.metric(metric, f"{value:,.2f}")
                    
                    st.markdown("---")
                    st.dataframe(df_stats, use_container_width=True)
                else:
                    st.info("📊 Данных пока нет. Запустите синхронизацию.")
            
            except gspread.exceptions.WorksheetNotFound:
                st.info("📊 Лист статистики не найден. Запустите синхронизацию.")
        
        except Exception as e:
            st.error(f"❌ Ошибка загрузки статистики: {e}")

def run_sync(config, fields_config, sync_state, days=30, mode="update", date_from=None, date_to=None):
    """Запуск синхронизации данных"""
    
    if not config.get("moysklad_token"):
        st.error("❌ Не указан токен МойСклад")
        return
    
    if not config.get("google_credentials_file"):
        st.error("❌ Не указан файл credentials")
        return
    
    # Прогресс-бар
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # 1. Подключение к API
        status_text.text("🔌 Подключение к МойСклад...")
        progress_bar.progress(10)
        
        api = MoySkladAPI(config["moysklad_token"])
        
        # 2. Загрузка заказов
        status_text.text("📦 Загрузка заказов...")
        progress_bar.progress(20)
        
        if date_from and date_to:
            date_from_str = date_from.strftime("%Y-%m-%d 00:00:00")
            date_to_str = date_to.strftime("%Y-%m-%d 23:59:59")
        else:
            date_to = datetime.now()
            date_from = date_to - timedelta(days=days)
            date_from_str = date_from.strftime("%Y-%m-%d 00:00:00")
            date_to_str = date_to.strftime("%Y-%m-%d 23:59:59")
        
        orders = api.get_all_orders(
            date_from_str, 
            date_to_str,
            progress_callback=lambda msg: status_text.text(msg)
        )
        
        if not orders:
            st.warning("⚠️ Заказы не найдены за указанный период")
            return
        
        progress_bar.progress(40)
        
        # 3. Сбор артикулов
        status_text.text("🔍 Анализ артикулов...")
        unique_articles = set()
        
        for order in orders:
            positions = order.get('positions', {}).get('rows', [])
            for pos in positions:
                assortment = pos.get('assortment', {})
                item_type = assortment.get('meta', {}).get('type', 'product')
                article = assortment.get('article')
                
                if article and item_type == 'bundle':
                    unique_articles.add(article)
        
        # 4. Загрузка товаров
        status_text.text("📦 Загрузка данных товаров...")
        progress_bar.progress(50)
        
        products_cache = {}
        if unique_articles:
            for article in unique_articles:
                endpoint = "/entity/product"
                params = {"filter": f"article={article}", "limit": 1}
                
                data = api._make_request(endpoint, params)
                if data and data.get('rows'):
                    product = data['rows'][0]
                    products_cache[article] = product
                
                time.sleep(0.05)
        
        # 5. Обработка данных
        status_text.text("⚙️ Обработка данных...")
        progress_bar.progress(60)
        
        processor = OrderProcessor()
        orders_data, positions_data, summaries = processor.process_orders(
            orders, 
            products_cache,
            progress_callback=lambda msg: status_text.text(msg)
        )
        
        # 6. Создание DataFrame
        status_text.text("📊 Формирование таблиц...")
        progress_bar.progress(70)
        
        df_orders = pd.DataFrame(orders_data)
        df_positions = pd.DataFrame(positions_data)
        df_summary = pd.DataFrame(summaries)
        
        # 7. Статистика
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
        
        # 8. Загрузка в Google Sheets
        status_text.text("☁️ Загрузка в Google Sheets...")
        progress_bar.progress(80)
        
        uploader = GoogleSheetsUploader(
            config["google_credentials_file"],
            config["spreadsheet_name"]
        )
        
        # Загружаем с учетом выбранных полей и режима
        uploaded_orders = uploader.upload_dataframe(
            df_orders, 
            "Заказы общая информация",
            selected_fields=fields_config.get("orders"),
            mode=mode
        )
        
        uploaded_positions = uploader.upload_dataframe(
            df_positions,
            "Позиции детально",
            selected_fields=fields_config.get("positions"),
            mode=mode
        )
        
        uploaded_summary = uploader.upload_dataframe(
            df_summary,
            "Сводка с прибылью",
            selected_fields=fields_config.get("summary"),
            mode=mode
        )
        
        uploader.upload_dataframe(
            df_stats,
            "Общая статистика",
            mode="replace"
        )
        
        progress_bar.progress(90)
        
        # 9. Форматирование
        status_text.text("🎨 Применение форматирования...")
        
        for sheet_name in ["Заказы общая информация", "Позиции детально", 
                          "Сводка с прибылью", "Общая статистика"]:
            uploader.format_worksheet(sheet_name)
        
        # 10. Обновление состояния синхронизации
        synced_orders = sync_state.get("synced_orders", [])
        new_order_numbers = [o["Номер заказа"] for o in orders_data]
        
        if mode == "update":
            synced_orders.extend([o for o in new_order_numbers if o not in synced_orders])
        elif mode == "replace":
            synced_orders = new_order_numbers
        else:  # append
            synced_orders.extend(new_order_numbers)
        
        sync_state["last_sync"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sync_state["synced_orders"] = list(set(synced_orders))
        
        ConfigManager.save_sync_state(sync_state)
        
        # 11. Завершение
        progress_bar.progress(100)
        status_text.text("✅ Синхронизация завершена!")
        
        # Результаты
        st.success("### ✅ Синхронизация успешно завершена!")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Обработано заказов", len(orders))
        
        with col2:
            st.metric("Обработано позиций", len(df_positions))
        
        with col3:
            if mode == "update":
                st.metric("Добавлено новых", uploaded_orders)
            else:
                st.metric("Загружено", uploaded_orders)
        
        # Показываем статистику
        if not df_stats.empty:
            st.markdown("---")
            st.markdown("### 📊 Общая статистика")
            st.dataframe(df_stats, use_container_width=True)
        
        # Сохраняем CSV
        st.markdown("---")
        st.markdown("### 💾 Локальные копии")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            csv_orders = df_orders.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                "📥 Скачать Заказы (CSV)",
                csv_orders,
                "orders.csv",
                "text/csv"
            )
        
        with col2:
            csv_positions = df_positions.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                "📥 Скачать Позиции (CSV)",
                csv_positions,
                "positions.csv",
                "text/csv"
            )
        
        with col3:
            csv_summary = df_summary.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                "📥 Скачать Сводку (CSV)",
                csv_summary,
                "summary.csv",
                "text/csv"
            )
    
    except Exception as e:
        progress_bar.progress(0)
        status_text.text("")
        st.error(f"❌ Ошибка синхронизации: {e}")
        st.exception(e)


if __name__ == "__main__":
    main()