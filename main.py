"""
Главный модуль для сбора данных о фьючерсных парах
"""
import asyncio
import sys
from datetime import datetime
from typing import Dict, List, Any
from decimal import Decimal

from api.binance import BinanceAPI
from api.bybit import BybitAPI
from api.coinmarketcap import CoinMarketCapAPI
from db.database import Database
from utils.config import Config
from utils.logger import logger
from utils.converters import DataConverter


class FuturesDataCollector:
    """Основной класс для сбора данных о фьючерсах"""

    def __init__(self):
        self.config = Config()
        self.db = Database()
        self.converter = DataConverter()
        self.start_time = None

    async def initialize(self):
        """Инициализация компонентов"""
        try:
            # Проверка конфигурации
            self.config.validate()

            # Подключение к базе данных
            await self.db.connect()

            logger.info("Инициализация завершена успешно")

        except Exception as e:
            logger.error(f"Ошибка инициализации: {e}")
            raise

    async def cleanup(self):
        """Очистка ресурсов"""
        await self.db.disconnect()

    async def collect_exchange_data(self, exchange_name: str) -> List[Dict[str, Any]]:
        """Сбор данных с одной биржи"""
        logger.info(f"Начало сбора данных с {exchange_name}")

        try:
            if exchange_name == 'Binance':
                async with BinanceAPI() as api:
                    pairs = await api.get_futures_pairs()

                    tasks = []
                    for pair in pairs:
                        task = api.collect_pair_data(pair['symbol'])
                        tasks.append(task)

                    batch_size = 50
                    all_data = []

                    for i in range(0, len(tasks), batch_size):
                        batch = tasks[i:i + batch_size]
                        batch_results = await asyncio.gather(*batch, return_exceptions=True)

                        for result in batch_results:
                            if isinstance(result, Exception):
                                logger.error(f"Ошибка при сборе данных: {result}")
                            else:
                                all_data.append(result)

                        if i + batch_size < len(tasks):
                            await asyncio.sleep(1)

                    return all_data

            elif exchange_name == 'Bybit':
                async with BybitAPI() as api:
                    pairs = await api.get_futures_pairs()

                    tasks = []
                    for pair in pairs:
                        task = api.collect_pair_data(pair['symbol'])
                        tasks.append(task)

                    batch_size = 30
                    all_data = []

                    for i in range(0, len(tasks), batch_size):
                        batch = tasks[i:i + batch_size]
                        batch_results = await asyncio.gather(*batch, return_exceptions=True)

                        for result in batch_results:
                            if isinstance(result, Exception):
                                logger.error(f"Ошибка при сборе данных: {result}")
                            else:
                                all_data.append(result)

                        if i + batch_size < len(tasks):
                            await asyncio.sleep(2)

                    return all_data

            else:
                raise ValueError(f"Неизвестная биржа: {exchange_name}")

        except Exception as e:
            logger.error(f"Ошибка при сборе данных с {exchange_name}: {e}")
            await self.db.save_api_error(exchange_name, 'collect_data', 'ERROR', str(e))
            return []

    async def collect_cmc_data(self, token_symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Сбор данных с CoinMarketCap"""
        logger.info(f"Сбор данных CoinMarketCap для {len(token_symbols)} токенов")

        try:
            async with CoinMarketCapAPI() as api:
                # Получаем цену BTC
                btc_price = await api.get_btc_price()

                # Получаем данные токенов
                tokens_data = await api.get_tokens_data(token_symbols)

                # Добавляем цену BTC к результатам
                if btc_price:
                    tokens_data['_btc_price'] = btc_price

                return tokens_data

        except Exception as e:
            logger.error(f"Ошибка при сборе данных CoinMarketCap: {e}")
            await self.db.save_api_error('CoinMarketCap', 'collect_data', 'ERROR', str(e))
            return {}

    async def process_and_save_data(self, exchange_data: List[Dict[str, Any]],
                                    cmc_data: Dict[str, Dict[str, Any]]):
        """Обработка и сохранение данных в БД"""
        logger.info("Обработка и сохранение данных")

        btc_price = cmc_data.get('_btc_price', Decimal('0'))
        saved_count = 0
        error_count = 0

        for data in exchange_data:
            try:
                # Извлекаем символ токена
                token_symbol = self.converter.extract_token_symbol(data['symbol'])
                if not token_symbol:
                    continue

                # Получаем или создаем токен
                token_id = await self.db.get_or_create_token(token_symbol)

                # Получаем или создаем пару
                pair_id = await self.db.get_or_create_futures_pair(
                    token_id=token_id,
                    exchange=data['exchange'],
                    pair_symbol=data['symbol']
                )

                # Получаем данные из CoinMarketCap
                cmc_token_data = cmc_data.get(token_symbol, {})

                # Рассчитываем объем в BTC
                volume_btc = None
                if data.get('volume_24h') and btc_price and btc_price > 0:
                    volume_btc = self.converter.convert_to_btc(
                        data['volume_24h'],
                        btc_price
                    )

                # Подготавливаем данные для сохранения
                futures_data = {
                    'pair_id': pair_id,
                    'open_interest_contracts': data.get('open_interest_contracts'),
                    'open_interest_usd': data.get('open_interest_usd'),
                    'funding_rate': data.get('funding_rate'),
                    'volume_btc': volume_btc,
                    'volume_usd': cmc_token_data.get('volume_24h_usd'),
                    'price_usd': cmc_token_data.get('price_usd'),
                    'market_cap_usd': cmc_token_data.get('market_cap_usd'),
                    'btc_price': btc_price
                }

                # Логируем данные перед сохранением
                logger.debug(f"Сохранение данных для {data['symbol']}: "
                             f"OI_USD={futures_data['open_interest_usd']}, "
                             f"Price_USD={futures_data['price_usd']}, "
                             f"Volume_USD={futures_data['volume_usd']}")

                # Сохраняем в БД
                await self.db.save_futures_data(futures_data)
                saved_count += 1

            except Exception as e:
                logger.error(f"Ошибка при сохранении данных для {data.get('symbol')}: {e}")
                error_count += 1

        logger.info(f"Сохранено записей: {saved_count}, ошибок: {error_count}")

        # Заменить метод run() в классе FuturesDataCollector в файле main.py на следующий:

    async def run(self):
        """Основной метод запуска сбора данных"""
        self.start_time = datetime.now()
        logger.info(f"Запуск сбора данных в {self.start_time}")

        try:
            # Инициализация
            await self.initialize()

            # Сбор данных о фьючерсах с бирж параллельно
            futures_tasks = [
                self.collect_exchange_data('Binance'),
                self.collect_exchange_data('Bybit')
            ]

            futures_results = await asyncio.gather(*futures_tasks, return_exceptions=True)

            # Объединяем результаты по фьючерсам
            all_futures_data = []
            for result in futures_results:
                if isinstance(result, Exception):
                    logger.error(f"Ошибка при сборе данных фьючерсов с биржи: {result}")
                else:
                    all_futures_data.extend(result)

            logger.info(f"Собрано данных по фьючерсам: {len(all_futures_data)} пар")

            # Сбор данных о спотовых парах с бирж параллельно
            spot_tasks = [
                self.collect_spot_exchange_data('Binance'),
                self.collect_spot_exchange_data('Bybit')
            ]

            spot_results = await asyncio.gather(*spot_tasks, return_exceptions=True)

            # Объединяем результаты по спотовым парам
            all_spot_data = []
            for result in spot_results:
                if isinstance(result, Exception):
                    logger.error(f"Ошибка при сборе спотовых данных с биржи: {result}")
                else:
                    all_spot_data.extend(result)

            logger.info(f"Собрано спотовых данных: {len(all_spot_data)} пар")

            # Извлекаем уникальные символы токенов из фьючерсных данных
            token_symbols = set()
            for data in all_futures_data:
                token_symbol = self.converter.extract_token_symbol(data['symbol'])
                if token_symbol:
                    token_symbols.add(token_symbol)

            # Добавляем BTC для конвертации
            token_symbols.add('BTC')

            # Сбор данных с CoinMarketCap
            cmc_data = await self.collect_cmc_data(list(token_symbols))

            # Обработка и сохранение данных о фьючерсах
            await self.process_and_save_data(all_futures_data, cmc_data)

            # Обработка и сохранение спотовых данных
            await self.process_and_save_spot_data(all_spot_data)

            # Статистика выполнения
            execution_time = (datetime.now() - self.start_time).total_seconds()
            logger.info(f"Сбор данных завершен за {execution_time:.2f} секунд")
            logger.info(f"Обработано: {len(all_futures_data)} фьючерсных пар, {len(all_spot_data)} спотовых пар")

        except Exception as e:
            logger.error(f"Критическая ошибка при выполнении: {e}")
            raise
        finally:
            await self.cleanup()

    async def collect_spot_exchange_data(self, exchange_name: str) -> List[Dict[str, Any]]:
        """Сбор данных о спотовых парах с одной биржи"""
        logger.info(f"Начало сбора спотовых данных с {exchange_name}")

        try:
            if exchange_name == 'Binance':
                async with BinanceAPI() as api:
                    pairs = await api.get_spot_pairs()

                    tasks = []
                    for pair in pairs:
                        task = api.collect_spot_pair_data(pair['symbol'])
                        tasks.append(task)

                    batch_size = 50
                    all_data = []

                    for i in range(0, len(tasks), batch_size):
                        batch = tasks[i:i + batch_size]
                        batch_results = await asyncio.gather(*batch, return_exceptions=True)

                        for result in batch_results:
                            if isinstance(result, Exception):
                                logger.error(f"Ошибка при сборе спотовых данных: {result}")
                            else:
                                all_data.append(result)

                        if i + batch_size < len(tasks):
                            await asyncio.sleep(1)

                    return all_data

            elif exchange_name == 'Bybit':
                async with BybitAPI() as api:
                    pairs = await api.get_spot_pairs()

                    tasks = []
                    for pair in pairs:
                        task = api.collect_spot_pair_data(pair['symbol'])
                        tasks.append(task)

                    batch_size = 30
                    all_data = []

                    for i in range(0, len(tasks), batch_size):
                        batch = tasks[i:i + batch_size]
                        batch_results = await asyncio.gather(*batch, return_exceptions=True)

                        for result in batch_results:
                            if isinstance(result, Exception):
                                logger.error(f"Ошибка при сборе спотовых данных: {result}")
                            else:
                                all_data.append(result)

                        if i + batch_size < len(tasks):
                            await asyncio.sleep(2)

                    return all_data

            else:
                raise ValueError(f"Неизвестная биржа: {exchange_name}")

        except Exception as e:
            logger.error(f"Ошибка при сборе спотовых данных с {exchange_name}: {e}")
            await self.db.save_api_error(exchange_name, 'collect_spot_data', 'ERROR', str(e))
            return []

    async def process_and_save_spot_data(self, spot_data: List[Dict[str, Any]]):
        """Обработка и сохранение спотовых данных в БД"""
        logger.info(f"Обработка и сохранение {len(spot_data)} спотовых записей")

        saved_count = 0
        error_count = 0

        for data in spot_data:
            try:
                # Извлекаем символ токена из спотовой пары
                token_symbol = self.converter.extract_token_from_spot_pair(data['symbol'])
                if not token_symbol:
                    continue

                # Получаем или создаем токен
                token_id = await self.db.get_or_create_token(token_symbol)

                # Получаем или создаем пару с типом SPOT
                pair_id = await self.db.get_or_create_futures_pair(
                    token_id=token_id,
                    exchange=data['exchange'],
                    pair_symbol=data['symbol'],
                    contract_type='SPOT'
                )

                # Подготавливаем данные для сохранения
                spot_data_to_save = {
                    'pair_id': pair_id,
                    'volume_btc': data.get('volume_btc')
                }

                # Логируем данные перед сохранением
                logger.debug(f"Сохранение спотовых данных для {data['symbol']}: "
                             f"volume_btc={spot_data_to_save['volume_btc']}")

                # Сохраняем в БД
                await self.db.save_spot_data(spot_data_to_save)
                saved_count += 1

            except Exception as e:
                logger.error(f"Ошибка при сохранении спотовых данных для {data.get('symbol')}: {e}")
                error_count += 1

        logger.info(f"Сохранено спотовых записей: {saved_count}, ошибок: {error_count}")


async def main():
    """Точка входа в приложение"""
    collector = FuturesDataCollector()

    try:
        await collector.run()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания")
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Запуск асинхронного приложения
    asyncio.run(main())
