"""
Тестовый скрипт для проверки работы CoinMarketCap API
и анализа формата ответа
"""
import asyncio
import aiohttp
import ssl
import certifi
import json
from typing import Dict, List, Any
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()


class CMCAPITester:
    def __init__(self):
        self.api_key = os.getenv('COINMARKETCAP_API_KEY')
        self.base_url = 'https://pro-api.coinmarketcap.com'

    async def test_api(self):
        """Основной метод тестирования"""
        # Список токенов для тестирования (включая проблемные)
        test_symbols = ['BTC', 'ETH', 'AVAIL', 'KERNEL', 'ZK', 'SUI', 'APT']

        print("=" * 80)
        print("ТЕСТИРОВАНИЕ COINMARKETCAP API")
        print("=" * 80)
        print(f"API Key: {'Установлен' if self.api_key else 'НЕ УСТАНОВЛЕН!'}")
        print(f"Base URL: {self.base_url}")
        print(f"Тестовые символы: {', '.join(test_symbols)}")
        print("=" * 80)

        headers = {
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip'
        }

        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(ssl=ssl_context)

        async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
            # Тест 1: Проверка одного символа
            print("\n1. ТЕСТ ОДНОГО СИМВОЛА (BTC)")
            print("-" * 40)
            await self.test_single_symbol(session, 'BTC')

            # Тест 2: Проверка множественных символов
            print("\n2. ТЕСТ МНОЖЕСТВЕННЫХ СИМВОЛОВ")
            print("-" * 40)
            await self.test_multiple_symbols(session, test_symbols)

            # Тест 3: Проверка проблемных символов по отдельности
            print("\n3. ПРОВЕРКА ПРОБЛЕМНЫХ СИМВОЛОВ ПО ОТДЕЛЬНОСТИ")
            print("-" * 40)
            for symbol in ['AVAIL', 'KERNEL', 'ZK']:
                await self.test_single_symbol(session, symbol)

            # Тест 4: Поиск символов через endpoint listings
            print("\n4. ПОИСК СИМВОЛОВ ЧЕРЕЗ LISTINGS")
            print("-" * 40)
            await self.search_symbols(session, ['AVAIL', 'KERNEL', 'ZK'])

    async def test_single_symbol(self, session: aiohttp.ClientSession, symbol: str):
        """Тест одного символа"""
        url = f"{self.base_url}/v1/cryptocurrency/quotes/latest"
        params = {
            'symbol': symbol,
            'convert': 'USD'
        }

        try:
            async with session.get(url, params=params) as response:
                print(f"Запрос символа: {symbol}")
                print(f"URL: {response.url}")
                print(f"Статус: {response.status}")

                data = await response.json()

                # Анализ структуры ответа
                print(f"Структура ответа:")
                print(f"  - status: {data.get('status', {})}")
                print(f"  - data keys: {list(data.get('data', {}).keys())}")

                # Если есть данные, показываем их
                symbol_data = data.get('data', {}).get(symbol)
                if symbol_data:
                    print(f"  ✓ Данные для {symbol} найдены!")
                    print(f"    - id: {symbol_data.get('id')}")
                    print(f"    - name: {symbol_data.get('name')}")
                    print(f"    - symbol: {symbol_data.get('symbol')}")
                    print(f"    - slug: {symbol_data.get('slug')}")
                    quote = symbol_data.get('quote', {}).get('USD', {})
                    print(f"    - price: ${quote.get('price', 0):.2f}")
                    print(f"    - market_cap: ${quote.get('market_cap', 0):,.0f}")
                else:
                    print(f"  ✗ Данные для {symbol} НЕ найдены!")
                    # Проверяем, может быть символ под другим ключом
                    if data.get('data'):
                        print(f"  Доступные ключи в data: {list(data.get('data', {}).keys())}")

        except Exception as e:
            print(f"Ошибка при запросе {symbol}: {e}")

    async def test_multiple_symbols(self, session: aiohttp.ClientSession, symbols: List[str]):
        """Тест множественных символов"""
        url = f"{self.base_url}/v1/cryptocurrency/quotes/latest"
        params = {
            'symbol': ','.join(symbols),
            'convert': 'USD'
        }

        try:
            async with session.get(url, params=params) as response:
                print(f"Запрос символов: {', '.join(symbols)}")
                print(f"Статус: {response.status}")

                data = await response.json()

                # Полный вывод структуры data для отладки
                print("\nПОЛНАЯ СТРУКТУРА ОТВЕТА DATA:")
                print(json.dumps(list(data.get('data', {}).keys()), indent=2))

                # Анализ каждого символа
                print("\nАНАЛИЗ СИМВОЛОВ:")
                for symbol in symbols:
                    if symbol in data.get('data', {}):
                        print(f"  ✓ {symbol} - найден")
                    else:
                        print(f"  ✗ {symbol} - НЕ найден")
                        # Проверяем возможные вариации
                        possible_keys = [k for k in data.get('data', {}).keys() if symbol in k.upper()]
                        if possible_keys:
                            print(f"    Возможные совпадения: {possible_keys}")

        except Exception as e:
            print(f"Ошибка при множественном запросе: {e}")

    async def search_symbols(self, session: aiohttp.ClientSession, symbols: List[str]):
        """Поиск символов через listings"""
        # Сначала получим список всех активных криптовалют
        url = f"{self.base_url}/v1/cryptocurrency/map"

        try:
            print("Получение полного списка криптовалют...")
            async with session.get(url) as response:
                data = await response.json()

                if response.status == 200:
                    crypto_list = data.get('data', [])
                    print(f"Всего криптовалют в базе: {len(crypto_list)}")

                    # Ищем наши символы
                    for search_symbol in symbols:
                        print(f"\nПоиск '{search_symbol}':")
                        found = False

                        for crypto in crypto_list:
                            # Проверяем точное совпадение символа
                            if crypto.get('symbol') == search_symbol:
                                found = True
                                print(f"  ✓ Найден точный символ:")
                                print(f"    - ID: {crypto.get('id')}")
                                print(f"    - Name: {crypto.get('name')}")
                                print(f"    - Symbol: {crypto.get('symbol')}")
                                print(f"    - Slug: {crypto.get('slug')}")
                                print(f"    - Status: {crypto.get('is_active')}")
                                break

                            # Проверяем частичное совпадение в имени
                            elif search_symbol.lower() in crypto.get('name', '').lower():
                                if not found:
                                    print(f"  ~ Частичное совпадение в имени:")
                                    found = True
                                print(f"    - {crypto.get('symbol')} - {crypto.get('name')}")

                        if not found:
                            print(f"  ✗ Символ '{search_symbol}' не найден в базе CoinMarketCap")
                            # Ищем похожие
                            similar = [c for c in crypto_list if search_symbol[:2].upper() in c.get('symbol', '')]
                            if similar[:5]:
                                print(f"  Похожие символы: {', '.join([c.get('symbol') for c in similar[:5]])}")

        except Exception as e:
            print(f"Ошибка при поиске символов: {e}")


async def main():
    """Главная функция"""
    tester = CMCAPITester()
    await tester.test_api()


if __name__ == "__main__":
    asyncio.run(main())