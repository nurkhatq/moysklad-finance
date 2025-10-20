import requests
import json
from datetime import datetime

class MoySkladDiagnostic:
    """Диагностический инструмент для МойСклад API"""
    
    def __init__(self, token):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }
    
    def print_json(self, data, title=""):
        """Красиво выводит JSON"""
        print("\n" + "=" * 80)
        if title:
            print(f"📋 {title}")
            print("=" * 80)
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("=" * 80)
    
    def get_order_by_name(self, order_name):
        """Получить заказ по номеру"""
        url = f"{self.base_url}/entity/customerorder"
        params = {
            "filter": f"name={order_name}",
            "limit": 1
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('rows'):
                return data['rows'][0]
            return None
        except Exception as e:
            print(f"❌ Ошибка получения заказа: {e}")
            return None
    
    def get_order_positions(self, order_id):
        """Получить позиции заказа"""
        url = f"{self.base_url}/entity/customerorder/{order_id}/positions"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            return data.get('rows', [])
        except Exception as e:
            print(f"❌ Ошибка получения позиций заказа: {e}")
            return []
    
    def get_product_by_article(self, article):
        """Получить товар по артикулу"""
        url = f"{self.base_url}/entity/product"
        params = {
            "filter": f"article={article}"
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('rows'):
                return data['rows'][0]
            return None
        except Exception as e:
            print(f"❌ Ошибка получения товара: {e}")
            return None
    
    def get_product_full_info(self, product_id):
        """Получить полную информацию о товаре"""
        url = f"{self.base_url}/entity/product/{product_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Ошибка получения полной информации о товаре: {e}")
            return None
    
    def get_stock_all(self, product_id):
        """Получить остатки через /report/stock/all"""
        url = f"{self.base_url}/report/stock/all"
        params = {
            "filter": f"product=https://api.moysklad.ru/api/remap/1.2/entity/product/{product_id}"
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Ошибка получения остатков (stock/all): {e}")
            return None
    
    def get_stock_bystore(self, product_id, moment=None):
        """Получить остатки через /report/stock/bystore"""
        url = f"{self.base_url}/report/stock/bystore"
        params = {
            "filter": f"product=https://api.moysklad.ru/api/remap/1.2/entity/product/{product_id}"
        }
        
        if moment:
            params["moment"] = moment
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"❌ Ошибка получения остатков (stock/bystore): {e}")
            return None
    
    def get_last_supply(self, product_id):
        """Получить последнюю приёмку товара"""
        url = f"{self.base_url}/entity/supply"
        params = {
            "filter": f"positions.assortment=https://api.moysklad.ru/api/remap/1.2/entity/product/{product_id}",
            "order": "moment,desc",
            "limit": 1,
            "expand": "positions"
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('rows'):
                return data['rows'][0]
            return None
        except Exception as e:
            print(f"❌ Ошибка получения приёмок: {e}")
            return None
    
    def diagnose_product(self, order_name, article=None):
        """Полная диагностика товара"""
        
        print("\n\n")
        print("🔍" + "=" * 78 + "🔍")
        print(f"   ДИАГНОСТИКА: Заказ {order_name}" + (f", Артикул {article}" if article else ""))
        print("🔍" + "=" * 78 + "🔍")
        
        # 1. Получаем заказ
        print("\n📦 ШАГ 1: Получение заказа...")
        order = self.get_order_by_name(order_name)
        
        if not order:
            print(f"❌ Заказ {order_name} не найден!")
            return
        
        order_id = order.get('id')
        print(f"✅ Заказ найден: {order.get('name')}")
        print(f"   ID: {order_id}")
        print(f"   Дата: {order.get('moment')}")
        print(f"   Сумма: {order.get('sum', 0) / 100} руб.")
        
        # 2. Получаем позиции заказа отдельным запросом
        print(f"\n📦 ШАГ 2: Получение позиций заказа...")
        positions = self.get_order_positions(order_id)
        
        print(f"✅ Найдено позиций: {len(positions)}")
        
        if not positions:
            print(f"⚠️  Позиции не найдены!")
            return
        
        # 3. Показываем все позиции
        print(f"\n📋 ШАГ 3: Список товаров в заказе:")
        
        for idx, pos in enumerate(positions, 1):
            assortment_meta = pos.get('assortment', {}).get('meta', {})
            assortment_href = assortment_meta.get('href', '')
            
            print(f"\n   {idx}. Позиция:")
            print(f"      Количество: {pos.get('quantity')}")
            print(f"      Цена: {pos.get('price', 0) / 100} руб.")
            print(f"      Сумма: {(pos.get('price', 0) * pos.get('quantity', 0)) / 100} руб.")
            print(f"      Ссылка на товар: {assortment_href}")
            
            # Получаем информацию о товаре
            if assortment_href:
                product_id = assortment_href.split('/')[-1]
                product_type = assortment_meta.get('type', 'unknown')
                
                print(f"      Тип: {product_type}")
                print(f"      ID: {product_id}")
                
                # Получаем полную информацию о товаре
                if product_type == 'product':
                    self.diagnose_product_details(product_id, pos.get('quantity'), pos.get('price', 0), order.get('moment'))
                elif product_type == 'variant':
                    self.diagnose_variant_details(product_id, pos.get('quantity'), pos.get('price', 0), order.get('moment'))
    
    def diagnose_variant_details(self, variant_id, quantity, price, moment):
        """Диагностика модификации товара"""
        url = f"{self.base_url}/entity/variant/{variant_id}"
        
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            variant = response.json()
            
            print(f"\n      📦 МОДИФИКАЦИЯ ТОВАРА:")
            print(f"         Название: {variant.get('name')}")
            print(f"         Артикул: {variant.get('article', 'нет')}")
            print(f"         Код: {variant.get('code', 'нет')}")
            
            # Базовый товар
            product_meta = variant.get('product', {}).get('meta', {})
            if product_meta:
                product_id = product_meta.get('href', '').split('/')[-1]
                print(f"         Базовый товар ID: {product_id}")
            
            # Цена закупки
            buy_price = variant.get('buyPrice', {})
            if buy_price:
                print(f"         💰 Цена закупки: {buy_price.get('value', 0) / 100} руб.")
            else:
                print(f"         ⚠️  Цена закупки: НЕ УКАЗАНА")
            
            # Получаем остатки для модификации
            self.check_stock_for_item(variant_id, 'variant', moment)
            
        except Exception as e:
            print(f"         ❌ Ошибка получения модификации: {e}")
    
    def diagnose_product_details(self, product_id, quantity, price, moment):
        """Диагностика обычного товара"""
        product_full = self.get_product_full_info(product_id)
        
        if not product_full:
            return
        
        print(f"\n      📦 ТОВАР:")
        print(f"         Название: {product_full.get('name')}")
        print(f"         Артикул: {product_full.get('article', 'нет')}")
        print(f"         Код: {product_full.get('code', 'нет')}")
        
        # Цена закупки
        buy_price = product_full.get('buyPrice', {})
        if buy_price:
            print(f"         💰 Цена закупки: {buy_price.get('value', 0) / 100} руб.")
        else:
            print(f"         ⚠️  Цена закупки: НЕ УКАЗАНА")
        
        # Получаем остатки
        self.check_stock_for_item(product_id, 'product', moment)
    
    def check_stock_for_item(self, item_id, item_type, moment):
        """Проверка остатков для товара или модификации"""
        
        # Пробуем получить остатки по складам
        url = f"{self.base_url}/report/stock/bystore"
        
        filter_param = f"{item_type}=https://api.moysklad.ru/api/remap/1.2/entity/{item_type}/{item_id}"
        
        params = {
            "filter": filter_param
        }
        
        if moment:
            params["moment"] = moment
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            stock_data = response.json()
            
            if stock_data and stock_data.get('rows'):
                print(f"\n         🏪 ОСТАТКИ ПО СКЛАДАМ:")
                
                total_stock = 0
                total_cost_weighted = 0
                total_qty_for_cost = 0
                has_cost = False
                
                for row in stock_data['rows']:
                    stock_by_store_list = row.get('stockByStore', [])
                    
                    for store in stock_by_store_list:
                        store_name = store.get('name', 'Неизвестный склад')
                        stock = store.get('stock', 0)
                        reserve = store.get('reserve', 0)
                        cost = store.get('cost', 0)
                        
                        print(f"\n            📍 {store_name}:")
                        print(f"               Остаток: {stock}")
                        print(f"               Резерв: {reserve}")
                        
                        if cost > 0:
                            print(f"               💰 Себестоимость: {cost / 100} руб.")
                            total_cost_weighted += cost * stock
                            total_qty_for_cost += stock
                            has_cost = True
                        else:
                            print(f"               ⚠️  Себестоимость: НЕТ ДАННЫХ")
                        
                        total_stock += stock
                
                print(f"\n         📊 ИТОГО:")
                print(f"            Общий остаток: {total_stock}")
                
                if has_cost and total_qty_for_cost > 0:
                    avg_cost = total_cost_weighted / total_qty_for_cost / 100
                    print(f"            ✅ Средняя себестоимость: {avg_cost:.2f} руб.")
                else:
                    print(f"            ❌ Себестоимость: НЕ ДОСТУПНА")
                    
                    # Проверяем последнюю приёмку
                    self.check_last_supply(item_id, item_type)
            else:
                print(f"\n         ⚠️  Остатки не найдены")
                
        except Exception as e:
            print(f"\n         ❌ Ошибка получения остатков: {e}")
    
    def check_last_supply(self, item_id, item_type):
        """Проверка последней приёмки"""
        url = f"{self.base_url}/entity/supply"
        
        filter_param = f"positions.assortment=https://api.moysklad.ru/api/remap/1.2/entity/{item_type}/{item_id}"
        
        params = {
            "filter": filter_param,
            "order": "moment,desc",
            "limit": 1,
            "expand": "positions"
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('rows'):
                supply = data['rows'][0]
                print(f"\n            📦 ПОСЛЕДНЯЯ ПРИЁМКА:")
                print(f"               Номер: {supply.get('name')}")
                print(f"               Дата: {supply.get('moment')}")
                
                # Ищем позицию
                positions = supply.get('positions', {}).get('rows', [])
                for pos in positions:
                    pos_assortment_href = pos.get('assortment', {}).get('meta', {}).get('href', '')
                    if item_id in pos_assortment_href:
                        print(f"               Количество: {pos.get('quantity')}")
                        print(f"               Цена закупки: {pos.get('price', 0) / 100} руб.")
                        break
            else:
                print(f"\n            ❌ ПРИЁМКИ НЕ НАЙДЕНЫ")
                print(f"               Товар не был оприходован на склад!")
                
        except Exception as e:
            print(f"\n            ⚠️  Ошибка проверки приёмок: {e}")


def main():
    """Основная функция"""
    
    MOYSKLAD_TOKEN = "105d4f38eb9a02400c3a6428ea71640babe37e98"
    
    # Создаём диагностический инструмент
    diagnostic = MoySkladDiagnostic(MOYSKLAD_TOKEN)
    
    print("\n")
    print("🔧" * 40)
    print("   ДИАГНОСТИЧЕСКИЙ ИНСТРУМЕНТ МОЙСКЛАД")
    print("🔧" * 40)
    print("\nПроверка доступа к API...")
    
    # Тестовые данные
    test_cases = [
        {"order": "661002421", "article": None},
        {"order": "WB3998670716", "article": None}
    ]
    
    for test_case in test_cases:
        diagnostic.diagnose_product(test_case["order"], test_case.get("article"))
    
    print("\n\n")
    print("=" * 80)
    print("✅ ДИАГНОСТИКА ЗАВЕРШЕНА")
    print("=" * 80)
    print("\nТеперь пришлите скриншот карточки товара из МойСклад,")
    print("чтобы сравнить с тем, что показывает API")


if __name__ == "__main__":
    main()