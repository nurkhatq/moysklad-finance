import requests
import json
from datetime import datetime, timedelta

class TestMoySkladDelivery:
    def __init__(self, token):
        self.base_url = "https://api.moysklad.ru/api/remap/1.2"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept-Encoding": "gzip",
            "Content-Type": "application/json"
        }
    
    def get_test_orders(self, limit=10):
        """Получить тестовые заказы"""
        endpoint = "/entity/customerorder"
        params = {
            "limit": limit,
            "expand": "positions.assortment,attributes"
        }
        
        response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, params=params)
        return response.json()
    
    def analyze_attributes(self, order):
        """Проанализировать атрибуты заказа"""
        print(f"\n🔍 Анализ заказа: {order.get('name', 'N/A')}")
        print(f"📅 Дата: {order.get('moment', 'N/A')}")
        print(f"💰 Сумма заказа: {order.get('sum', 0) / 100:.2f} руб.")
        
        attributes = order.get('attributes', [])
        print(f"📋 Количество атрибутов: {len(attributes)}")
        
        delivery_found = False
        commission_found = False
        
        for attr in attributes:
            attr_name = attr.get('name', '')
            attr_value = attr.get('value', '')
            attr_type = attr.get('type', '')
            
            print(f"   📝 Атрибут: '{attr_name}'")
            print(f"      Тип: {attr_type}")
            print(f"      Значение: {attr_value}")
            
            # Проверяем на доставку
            if any(word in attr_name.lower() for word in ['доставк', 'delivery']):
                delivery_found = True
                print(f"      🚛 ОБНАРУЖЕНА ДОСТАВКА!")
            
            # Проверяем на комиссию
            if any(word in attr_name.lower() for word in ['комисс', 'commission']):
                commission_found = True
                print(f"      💰 ОБНАРУЖЕНА КОМИССИЯ!")
        
        return delivery_found, commission_found
    
    def calculate_delivery_distribution(self, order):
        """Рассчитать распределение доставки по позициям"""
        print(f"\n📊 РАСПРЕДЕЛЕНИЕ ДОСТАВКИ ДЛЯ ЗАКАЗА {order.get('name', 'N/A')}")
        
        # Найдем стоимость доставки в атрибутах
        delivery_cost = 0
        for attr in order.get('attributes', []):
            attr_name = attr.get('name', '').lower()
            attr_value = attr.get('value', 0)
            
            if any(word in attr_name for word in ['доставк', 'delivery']):
                if any(word in attr_name for word in ['стоимость', 'сумма', 'цена', 'cost']):
                    try:
                        delivery_cost = float(attr_value)
                        print(f"💰 Найдена стоимость доставки: {delivery_cost} руб.")
                    except:
                        pass
        
        if delivery_cost == 0:
            print("❌ Стоимость доставки не найдена или равна 0")
            return
        
        positions = order.get('positions', {}).get('rows', [])
        order_sum = order.get('sum', 0) / 100
        
        print(f"📦 Количество позиций: {len(positions)}")
        print(f"💰 Сумма заказа: {order_sum} руб.")
        
        total_after_distribution = 0
        
        for i, pos in enumerate(positions):
            assortment = pos.get('assortment', {})
            quantity = pos.get('quantity', 0)
            price = pos.get('price', 0) / 100
            position_sum = price * quantity
            
            # Доля позиции в заказе
            position_share = (position_sum / order_sum) if order_sum > 0 else (1 / len(positions) if len(positions) > 0 else 0)
            
            # Доставка на позицию
            delivery_per_position = delivery_cost * position_share
            
            total_after_distribution += delivery_per_position
            
            print(f"\n   📦 Позиция {i+1}:")
            print(f"      Товар: {assortment.get('name', 'N/A')}")
            print(f"      Количество: {quantity}")
            print(f"      Цена: {price:.2f} руб.")
            print(f"      Сумма позиции: {position_sum:.2f} руб.")
            print(f"      Доля в заказе: {position_share:.4f}")
            print(f"      Доставка на позицию: {delivery_per_position:.2f} руб.")
        
        print(f"\n📊 ИТОГО распределено: {total_after_distribution:.2f} руб.")
        print(f"📊 Исходная доставка: {delivery_cost:.2f} руб.")
        print(f"📊 Разница: {abs(total_after_distribution - delivery_cost):.2f} руб.")
    
    def test_specific_order(self, order_id):
        """Протестировать конкретный заказ по ID"""
        endpoint = f"/entity/customerorder/{order_id}"
        params = {
            "expand": "positions.assortment,attributes"
        }
        
        response = requests.get(f"{self.base_url}{endpoint}", headers=self.headers, params=params)
        order = response.json()
        
        self.analyze_attributes(order)
        self.calculate_delivery_distribution(order)
    
    def run_comprehensive_test(self):
        """Запустить комплексный тест"""
        print("🚀 ЗАПУСК ТЕСТА РАСПРЕДЕЛЕНИЯ ДОСТАВКИ")
        print("=" * 60)
        
        # Получаем тестовые заказы
        data = self.get_test_orders(limit=5)
        
        if 'rows' not in data:
            print("❌ Не удалось получить заказы")
            return
        
        orders = data['rows']
        print(f"✅ Получено заказов для теста: {len(orders)}")
        
        delivery_stats = {
            'total_orders': len(orders),
            'orders_with_delivery': 0,
            'orders_with_commission': 0,
            'orders_with_both': 0
        }
        
        # Анализируем каждый заказ
        for i, order in enumerate(orders):
            print(f"\n{'='*50}")
            print(f"📦 ТЕСТ ЗАКАЗА {i+1}/{len(orders)}")
            print(f"{'='*50}")
            
            delivery_found, commission_found = self.analyze_attributes(order)
            
            if delivery_found:
                delivery_stats['orders_with_delivery'] += 1
                self.calculate_delivery_distribution(order)
            
            if commission_found:
                delivery_stats['orders_with_commission'] += 1
            
            if delivery_found and commission_found:
                delivery_stats['orders_with_both'] += 1
        
        # Выводим статистику
        print(f"\n{'='*60}")
        print("📊 СТАТИСТИКА ТЕСТА")
        print(f"{'='*60}")
        print(f"📦 Всего заказов: {delivery_stats['total_orders']}")
        print(f"🚛 Заказов с доставкой: {delivery_stats['orders_with_delivery']}")
        print(f"💰 Заказов с комиссией: {delivery_stats['orders_with_commission']}")
        print(f"📦💰 Заказов с доставкой и комиссией: {delivery_stats['orders_with_both']}")
        
        # Сохраняем пример заказа для отладки
        if orders:
            with open('test_order_example.json', 'w', encoding='utf-8') as f:
                json.dump(orders[0], f, ensure_ascii=False, indent=2)
            print(f"\n💾 Пример заказа сохранен в: test_order_example.json")


def main():
    # Настройки
    MOYSKLAD_TOKEN = "105d4f38eb9a02400c3a6428ea71640babe37e98"
    
    # Создаем тестер
    tester = TestMoySkladDelivery(MOYSKLAD_TOKEN)
    
    # Запускаем комплексный тест
    tester.run_comprehensive_test()
    
    # Если хотите протестировать конкретный заказ, раскомментируйте:
    # order_id = "ваш-id-заказа-здесь"
    # tester.test_specific_order(order_id)


if __name__ == "__main__":
    main()