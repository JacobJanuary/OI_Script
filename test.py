#!/usr/bin/env python3
"""
Скрипт для анализа крупных сделок на Binance с сохранением в MySQL.

Модуль фильтрует торговые пары по объему, исключает пары стейблкоинов,
находит сделки на сумму $89,000 и более, и сохраняет их в базу данных.
"""

import asyncio
import logging
import os
import ssl
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

try:
    import aiohttp
    from aiohttp import ClientError, ClientSession, TCPConnector
except ImportError:
    print("Необходимо установить aiohttp: pip install aiohttp")
    sys.exit(1)

try:
    import certifi
except ImportError:
    print("Необходимо установить certifi: pip install certifi")
    sys.exit(1)

try:
    import aiomysql
except ImportError:
    print("Необходимо установить aiomysql: pip install aiomysql")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("Необходимо установить python-dotenv: pip install python-dotenv")
    sys.exit(1)

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Константы
BINANCE_API_URL = "https://api.binance.com"
EXCHANGE_INFO_ENDPOINT = "/api/v3/exchangeInfo"
TICKER_24HR_ENDPOINT = "/api/v3/ticker/24hr"
TRADES_ENDPOINT = "/api/v3/trades"

# Фильтры
MIN_VOLUME_USD = 1_000_000  # Минимальный объем торгов за 24 часа
MIN_TRADE_VALUE_USD = 49_000  # Минимальная сумма сделки для вывода

# Стейблкоины для исключения
STABLECOINS = {
    'USDT', 'USDC', 'BUSD', 'TUSD', 'USDP', 'USDD', 'GUSD',
    'FRAX', 'LUSD', 'USTC', 'ALUSD', 'CUSD', 'CEUR', 'EUROC',
    'AGEUR', 'AEUR', 'STEUR', 'EURS', 'EURT', 'EURC', 'PAX',
    'FDUSD', 'PYUSD', 'USDB', 'USDJ', 'USDX', 'USDQ', 'TRIBE', # Добавлена запятая
    'XUSD'
}

# Wrapped токены для исключения
WRAPPED_TOKENS = {
    'WBTC', 'WETH', 'WBNB', 'WBETH', 'WBCH', 'WLTC', 'WZEC',
    'WMATIC', 'WAVAX', 'WFTM', 'WONE', 'WCRO', 'WNEAR', 'WKAVA',
    'WXRP', 'WADA', 'WDOT', 'WSOL', 'WTRX', 'WEOS', 'WXLM',
    'WALGO', 'WICP', 'WEGLD', 'WXTZ', 'WFIL', 'WAXL', 'WFLOW',
    'WMINA', 'WGLMR', 'WKLAY', 'WRUNE', 'WZIL', 'WAR', 'WROSE',
    'WVET', 'WQTUM', 'WNEO', 'WHBAR', 'WZRX', 'WBAT', 'WENJ',
    'WCHZ', 'WMANA', 'WGRT', 'W1INCH', 'WCOMP', 'WSNX', 'WCRV'
}

# Rate limit настройки
MAX_CONCURRENT_REQUESTS = 3  # Еще меньше параллельных запросов
TRADES_LIMIT = 1000  # Максимальное количество сделок для анализа (максимум для Binance API)
REQUEST_WEIGHT_TRADES = 10  # Вес для /api/v3/trades
REQUEST_WEIGHT_EXCHANGE_INFO = 20  # Вес для /api/v3/exchangeInfo
REQUEST_WEIGHT_TICKERS = 40  # Вес для /api/v3/ticker/24hr (все тикеры)
MAX_WEIGHT_PER_MINUTE = 1200
DELAY_BETWEEN_REQUESTS = 0.2  # Увеличена задержка
RETRY_DELAY = 5
MAX_RETRIES = 3

# MySQL конфигурация
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'db': os.getenv('MYSQL_DATABASE', 'crypto_db'),
}


def create_ssl_context(verify_ssl: bool = True) -> ssl.SSLContext:
    """
    Создает SSL контекст с настройками для обхода проблем с сертификатами.

    Args:
        verify_ssl: Проверять ли SSL сертификаты

    Returns:
        Настроенный SSL контекст
    """
    if verify_ssl:
        ssl_context = ssl.create_default_context()
        try:
            ssl_context.load_verify_locations(certifi.where())
        except Exception:
            logger.warning("Не удалось загрузить сертификаты из certifi")
    else:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        logger.warning("SSL проверка отключена! Используйте только для отладки.")

    return ssl_context


@dataclass
class Trade:
    """
    Представление торговой сделки.

    Attributes:
        id: Уникальный идентификатор сделки
        symbol: Символ торговой пары
        price: Цена сделки
        qty: Количество в сделке
        time: Временная метка сделки
        is_buyer_maker: True если покупатель был мейкером
        base_asset: Базовый актив <--- НОВОЕ ПОЛЕ
        quote_asset: Котировочный актив
        value_usd: Примерная стоимость в USD
    """
    id: int
    symbol: str
    price: Decimal
    qty: Decimal
    time: int
    is_buyer_maker: bool
    base_asset: str  # <--- НОВОЕ ПОЛЕ
    quote_asset: str
    value_usd: Decimal

    @classmethod
    def from_api_response(cls, data: Dict, symbol: str, base_asset: str, quote_asset: str, quote_price_usd: Decimal) -> 'Trade': # <--- Добавлен base_asset
        """
        Создает объект Trade из ответа API.

        Args:
            data: Словарь с данными сделки от API
            symbol: Символ торговой пары
            base_asset: Базовый актив <--- НОВЫЙ АРГУМЕНТ
            quote_asset: Котировочный актив
            quote_price_usd: Цена котировочного актива в USD

        Returns:
            Объект Trade
        """
        price = Decimal(data['price'])
        qty = Decimal(data['qty'])
        value_usd = price * qty * quote_price_usd

        return cls(
            id=data['id'],
            symbol=symbol,
            price=price,
            qty=qty,
            time=data['time'],
            is_buyer_maker=data['isBuyerMaker'],
            base_asset=base_asset, # <--- ИСПОЛЬЗОВАНИЕ НОВОГО АРГУМЕНТА
            quote_asset=quote_asset,
            value_usd=value_usd
        )


@dataclass
class TradingPairInfo:
    """Информация о торговой паре."""
    symbol: str
    base_asset: str
    quote_asset: str
    volume_24h_usd: Decimal
    quote_price_usd: Decimal


class RateLimiter:
    """Контроллер rate limits для API запросов."""

    def __init__(self, max_weight_per_minute: int) -> None:
        """
        Инициализирует rate limiter.

        Args:
            max_weight_per_minute: Максимальный вес запросов в минуту
        """
        self.max_weight_per_minute = max_weight_per_minute
        self.requests: List[Tuple[float, int]] = []
        self.lock = asyncio.Lock()

    async def acquire(self, weight: int) -> None:
        """
        Ожидает, пока можно будет выполнить запрос с указанным весом.

        Args:
            weight: Вес запроса
        """
        while True:
            async with self.lock:
                current_time = time.time()

                # Удаляем запросы старше минуты
                self.requests = [
                    (ts, w) for ts, w in self.requests
                    if current_time - ts < 60
                ]

                # Считаем текущий вес
                current_weight = sum(w for _, w in self.requests)

                # Если можем выполнить запрос - выполняем
                if current_weight + weight <= self.max_weight_per_minute:
                    self.requests.append((current_time, weight))
                    return

                # Иначе вычисляем время ожидания
                if self.requests:
                    oldest_request_time = min(ts for ts, _ in self.requests)
                    wait_time = max(0.1, 60 - (current_time - oldest_request_time) + 1)
                else:
                    wait_time = 1.0

            # Ждем вне блокировки
            logger.info(
                f"Rate limit достигнут ({current_weight + weight}/{self.max_weight_per_minute}), ожидание {wait_time:.1f} секунд")
            await asyncio.sleep(wait_time)


class BinanceClient:
    """Асинхронный клиент для работы с Binance API."""

    def __init__(self, session: ClientSession, rate_limiter: RateLimiter) -> None:
        """
        Инициализирует клиент Binance.

        Args:
            session: Асинхронная HTTP сессия
            rate_limiter: Контроллер rate limits
        """
        self.session = session
        self.base_url = BINANCE_API_URL
        self.rate_limiter = rate_limiter

    async def get_exchange_info(self) -> Dict:
        """
        Получает информацию о бирже.

        Returns:
            Словарь с информацией о торговых парах
        """
        await self.rate_limiter.acquire(REQUEST_WEIGHT_EXCHANGE_INFO)

        url = f"{self.base_url}{EXCHANGE_INFO_ENDPOINT}"
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.json()

    async def get_24hr_tickers(self) -> List[Dict]:
        """
        Получает 24-часовую статистику для всех пар.

        Returns:
            Список словарей с данными тикеров
        """
        await self.rate_limiter.acquire(REQUEST_WEIGHT_TICKERS)

        url = f"{self.base_url}{TICKER_24HR_ENDPOINT}"
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.json()

    async def get_recent_trades(self, symbol: str, retry_count: int = 0) -> List[Dict]:
        """
        Получает последние сделки для указанной торговой пары.

        Args:
            symbol: Символ торговой пары
            retry_count: Текущее количество попыток

        Returns:
            Список сделок в сыром виде
        """
        try:
            await self.rate_limiter.acquire(REQUEST_WEIGHT_TRADES)
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

            url = f"{self.base_url}{TRADES_ENDPOINT}"
            params = {'symbol': symbol, 'limit': TRADES_LIMIT}

            async with self.session.get(url, params=params) as response:
                if response.status in [429, 418]:
                    if retry_count < MAX_RETRIES:
                        retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                        logger.warning(f"Rate limit для {symbol}, повтор через {retry_after}с")
                        await asyncio.sleep(retry_after)
                        return await self.get_recent_trades(symbol, retry_count + 1)
                    else:
                        return []

                if response.status == 400:
                    return []

                response.raise_for_status()
                return await response.json()

        except Exception as e:
            if retry_count < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY)
                return await self.get_recent_trades(symbol, retry_count + 1)
            else:
                logger.error(f"Ошибка при получении сделок для {symbol}: {e}")
                return []


class TradingDataAnalyzer:
    """Анализатор торговых данных."""

    def __init__(self) -> None:
        """Инициализирует анализатор."""
        self.quote_prices_usd: Dict[str, Decimal] = {
            'USDT': Decimal('1.0'),
            'USDC': Decimal('1.0'),
            'BUSD': Decimal('1.0'),
            'FDUSD': Decimal('1.0'),
        }

    def is_stablecoin_pair(self, base_asset: str, quote_asset: str) -> bool:
        """
        Проверяет, является ли пара парой стейблкоинов.

        Args:
            base_asset: Базовый актив
            quote_asset: Котировочный актив

        Returns:
            True если оба актива - стейблкоины
        """
        return base_asset in STABLECOINS and quote_asset in STABLECOINS

    def is_wrapped_token(self, asset: str) -> bool:
        """
        Проверяет, является ли токен wrapped токеном.

        Args:
            asset: Название актива

        Returns:
            True если актив - wrapped токен
        """
        return asset in WRAPPED_TOKENS or asset.startswith('W') and len(asset) > 2

    def calculate_volume_usd(self, volume: str, quote_asset: str) -> Decimal:
        """
        Рассчитывает объем в USD.

        Args:
            volume: Объем в котировочной валюте
            quote_asset: Котировочный актив

        Returns:
            Объем в USD
        """
        volume_decimal = Decimal(volume)
        quote_price = self.quote_prices_usd.get(quote_asset, Decimal('0'))
        return volume_decimal * quote_price

    def update_quote_prices(self, tickers: List[Dict]) -> None:
        """
        Обновляет цены котировочных активов в USD.

        Args:
            tickers: Список тикеров с 24hr данными
        """
        # Обновляем цены основных котировочных активов
        for ticker in tickers:
            symbol = ticker['symbol']

            # BTC price in USDT
            if symbol == 'BTCUSDT':
                self.quote_prices_usd['BTC'] = Decimal(ticker['lastPrice'])
            # ETH price in USDT
            elif symbol == 'ETHUSDT':
                self.quote_prices_usd['ETH'] = Decimal(ticker['lastPrice'])
            # BNB price in USDT
            elif symbol == 'BNBUSDT':
                self.quote_prices_usd['BNB'] = Decimal(ticker['lastPrice'])

    def filter_trading_pairs(
            self,
            exchange_info: Dict,
            tickers: List[Dict]
    ) -> List[TradingPairInfo]:
        """
        Фильтрует торговые пары по критериям.

        Args:
            exchange_info: Информация о парах
            tickers: 24-часовая статистика

        Returns:
            Список отфильтрованных пар с информацией
        """
        # Создаем словарь тикеров для быстрого доступа
        ticker_map = {t['symbol']: t for t in tickers}

        # Обновляем цены котировочных активов
        self.update_quote_prices(tickers)

        filtered_pairs = []

        for symbol_info in exchange_info['symbols']:
            # Проверяем, что это спотовая пара и она активна
            if (symbol_info['status'] != 'TRADING' or
                    not symbol_info['isSpotTradingAllowed']):
                continue

            symbol = symbol_info['symbol']
            base_asset = symbol_info['baseAsset']
            quote_asset = symbol_info['quoteAsset']

            # Пропускаем пары стейблкоинов
            if self.is_stablecoin_pair(base_asset, quote_asset):
                continue

            # Пропускаем wrapped токены
            if self.is_wrapped_token(base_asset):
                continue

            # Получаем данные тикера
            ticker = ticker_map.get(symbol)
            if not ticker:
                continue

            # Рассчитываем объем в USD
            quote_volume = ticker.get('quoteVolume', '0')
            volume_usd = self.calculate_volume_usd(quote_volume, quote_asset)

            # Фильтруем по минимальному объему
            if volume_usd < MIN_VOLUME_USD:
                continue

            quote_price_usd = self.quote_prices_usd.get(quote_asset, Decimal('0'))

            filtered_pairs.append(TradingPairInfo(
                symbol=symbol,
                base_asset=base_asset,
                quote_asset=quote_asset,
                volume_24h_usd=volume_usd,
                quote_price_usd=quote_price_usd
            ))

        return filtered_pairs

    def find_large_trades(
            self,
            trades_data: List[Dict],
            pair_info: TradingPairInfo
    ) -> List[Trade]:
        """
        Находит крупные сделки (>= $MIN_TRADE_VALUE_USD).

        Args:
            trades_data: Сырые данные сделок от API
            pair_info: Информация о торговой паре

        Returns:
            Список крупных сделок
        """
        large_trades = []

        for trade_data in trades_data:
            trade = Trade.from_api_response(
                data=trade_data,
                symbol=pair_info.symbol,
                base_asset=pair_info.base_asset, # <--- ПЕРЕДАЧА base_asset
                quote_asset=pair_info.quote_asset,
                quote_price_usd=pair_info.quote_price_usd
            )

            if trade.value_usd >= MIN_TRADE_VALUE_USD:
                large_trades.append(trade)

        return large_trades


async def process_pair(
        client: BinanceClient,
        pair_info: TradingPairInfo,
        analyzer: TradingDataAnalyzer,
        semaphore: asyncio.Semaphore
) -> List[Trade]:
    """
    Обрабатывает одну торговую пару.

    Args:
        client: Клиент Binance API
        pair_info: Информация о паре
        analyzer: Анализатор данных
        semaphore: Семафор для ограничения параллельных запросов

    Returns:
        Список крупных сделок
    """
    async with semaphore:
        trades_data = await client.get_recent_trades(pair_info.symbol)
        if not trades_data:
            return []

        return analyzer.find_large_trades(trades_data, pair_info)


class DatabaseManager:
    """Менеджер для работы с MySQL базой данных."""

    def __init__(self):
        """Инициализирует менеджер базы данных."""
        self.pool = None

    async def connect(self) -> None:
        """Создает пул соединений с базой данных."""
        try:
            self.pool = await aiomysql.create_pool(
                **MYSQL_CONFIG,
                autocommit=True,
                minsize=1,
                maxsize=10
            )
            logger.info("Подключение к MySQL установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к MySQL: {e}")
            raise

    async def close(self) -> None:
        """Закрывает пул соединений."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Соединение с MySQL закрыто")

    async def create_tables(self) -> None:
        """Создает необходимые таблицы, если они не существуют."""
        create_table_sql = """
                           CREATE TABLE IF NOT EXISTS large_trades (
                               id BIGINT PRIMARY KEY,
                               symbol VARCHAR(20) NOT NULL,
                               price DECIMAL(20, 8) NOT NULL,
                               quantity DECIMAL(20, 8) NOT NULL,
                               value_usd DECIMAL(20, 2) NOT NULL,
                               base_asset VARCHAR(10) NOT NULL, -- <-- НОВАЯ КОЛОНКА
                               quote_asset VARCHAR(10) NOT NULL,
                               is_buyer_maker BOOLEAN NOT NULL,
                               trade_time DATETIME NOT NULL,
                               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                               INDEX idx_symbol (symbol),
                               INDEX idx_value_usd (value_usd),
                               INDEX idx_trade_time (trade_time),
                               INDEX idx_base_asset (base_asset) -- <-- ИНДЕКС ДЛЯ НОВОЙ КОЛОНКИ
                           ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                           """
        # Убраны обратные слеши и лишние пробелы для лучшей читаемости SQL

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(create_table_sql)
                logger.info("Таблица large_trades создана/проверена")

    async def save_trades(self, trades: List[Trade]) -> Tuple[int, int]:
        """
        Сохраняет сделки в базу данных.

        Args:
            trades: Список сделок для сохранения

        Returns:
            Кортеж (количество новых сделок, количество дубликатов)
        """
        if not trades:
            return 0, 0

        insert_sql = """
                     INSERT IGNORE INTO large_trades 
                     (id, symbol, price, quantity, value_usd, base_asset, quote_asset, is_buyer_maker, trade_time)
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                     """
        # <--- Добавлены base_asset и %s

        # Сначала проверяем существующие ID
        trade_ids = [trade.id for trade in trades]
        placeholders = ','.join(['%s'] * len(trade_ids))
        check_sql = f"SELECT id FROM large_trades WHERE id IN ({placeholders})"

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                # Получаем существующие ID
                await cursor.execute(check_sql, trade_ids)
                existing_ids = {row[0] for row in await cursor.fetchall()}

                # Фильтруем новые сделки
                new_trades = []
                duplicate_count = 0

                for trade in trades:
                    if trade.id in existing_ids:
                        duplicate_count += 1
                        logger.info(
                            f"Дубликат: сделка {trade.symbol} ID:{trade.id} "
                            f"на сумму ${trade.value_usd:,.2f} уже в БД"
                        )
                    else:
                        new_trades.append(trade)

                # Сохраняем только новые сделки
                if new_trades:
                    values = [
                        (
                            trade.id,
                            trade.symbol,
                            float(trade.price),
                            float(trade.qty),
                            float(trade.value_usd),
                            trade.base_asset,  # <--- ДОБАВЛЕНО ЗНАЧЕНИЕ ДЛЯ base_asset
                            trade.quote_asset,
                            trade.is_buyer_maker,
                            datetime.fromtimestamp(trade.time / 1000)
                        )
                        for trade in new_trades
                    ]

                    await cursor.executemany(insert_sql, values)
                    saved_count = cursor.rowcount

                    if saved_count > 0:
                        logger.info(f"Сохранено {saved_count} новых сделок в БД")
                        # Выводим информацию о новых сделках
                        for trade in new_trades[:5]:  # Показываем первые 5
                            trade_time = datetime.fromtimestamp(trade.time / 1000)
                            print(
                                f"  НОВАЯ: {trade.symbol} ({trade.base_asset}/{trade.quote_asset}) ${trade.value_usd:,.2f} в {trade_time.strftime('%H:%M:%S')}")
                        if len(new_trades) > 5:
                            print(f"  ... и еще {len(new_trades) - 5} сделок")

                    return saved_count, duplicate_count
                else:
                    return 0, duplicate_count

    async def get_recent_trades_count(self, hours: int = 24) -> int:
        """
        Получает количество сделок за последние N часов.

        Args:
            hours: Количество часов

        Returns:
            Количество сделок
        """
        query = """
                SELECT COUNT(*)
                FROM large_trades
                WHERE trade_time > DATE_SUB(NOW(), INTERVAL %s HOUR)
                """
        # Убраны обратные слеши для лучшей читаемости SQL

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, (hours,))
                result = await cursor.fetchone()
                return result[0] if result else 0


async def test_connection(session: ClientSession) -> bool:
    """
    Проверяет соединение с Binance API.

    Args:
        session: HTTP сессия

    Returns:
        True если соединение успешно
    """
    try:
        url = f"{BINANCE_API_URL}/api/v3/ping"
        async with session.get(url) as response:
            response.raise_for_status()
            logger.info("Соединение с Binance API установлено")
            return True
    except Exception as e:
        logger.error(f"Не удалось подключиться к Binance API: {e}")
        return False


async def run_monitoring_cycle(
        session: ClientSession,
        db_manager: DatabaseManager
) -> None:
    """
    Выполняет один цикл мониторинга.

    Args:
        session: HTTP сессия
        db_manager: Менеджер базы данных
    """
    # Создаем клиент и анализатор
    rate_limiter = RateLimiter(MAX_WEIGHT_PER_MINUTE)
    client = BinanceClient(session, rate_limiter)
    analyzer = TradingDataAnalyzer()

    try:
        # Получаем информацию о парах и тикеры
        logger.info("Получаем информацию о торговых парах...")
        exchange_info = await client.get_exchange_info()

        logger.info("Получаем 24-часовую статистику...")
        tickers = await client.get_24hr_tickers()

        # Фильтруем пары
        logger.info("Фильтруем торговые пары...")
        filtered_pairs = analyzer.filter_trading_pairs(exchange_info, tickers)

        logger.info(
            f"Найдено {len(filtered_pairs)} пар с объемом > ${MIN_VOLUME_USD:,} "
            f"(исключены пары стейблкоинов и wrapped токены)"
        )

        if not filtered_pairs:
            logger.error("Не найдено подходящих торговых пар") # Исправлено на logger.error
            return

        # Сортируем пары по объему для приоритетной обработки
        sorted_pairs = sorted(
            filtered_pairs,
            key=lambda x: x.volume_24h_usd,
            reverse=True
        )

        # Выводим топ-10 пар по объему
        print("\nТоп-10 пар по объему торгов:")
        print("-" * 60)
        for i, pair in enumerate(sorted_pairs[:10], 1):
            print(f"{i}. {pair.symbol} ({pair.base_asset}/{pair.quote_asset}): ${pair.volume_24h_usd:,.0f}")
        print("-" * 60)

        # Ищем крупные сделки
        logger.info(f"\nИщем сделки на сумму >= ${MIN_TRADE_VALUE_USD:,}...")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # Обрабатываем пары батчами
        all_large_trades = []
        batch_size = 30
        total_new = 0
        total_duplicates = 0

        for i in range(0, len(sorted_pairs), batch_size):
            batch = sorted_pairs[i:i + batch_size]

            tasks = [
                process_pair(client, pair_info, analyzer, semaphore)
                for pair_info in batch
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_trades = []
            for result in results:
                if isinstance(result, Exception):
                    logger.debug(f"Ошибка при обработке: {result}") # Исправлено на logger.debug
                    continue
                batch_trades.extend(result)

            # Сохраняем найденные сделки в БД
            if batch_trades:
                new_count, dup_count = await db_manager.save_trades(batch_trades)
                total_new += new_count
                total_duplicates += dup_count

            all_large_trades.extend(batch_trades)

            # Показываем прогресс
            processed = min(i + batch_size, len(sorted_pairs))
            logger.info(
                f"Обработано {processed}/{len(sorted_pairs)} пар | "
                f"Найдено: {len(all_large_trades)} | "
                f"Новых: {total_new} | Дубликатов: {total_duplicates}"
            )

            if i + batch_size < len(sorted_pairs):
                await asyncio.sleep(3) # Немного уменьшил задержку между батчами, можно вернуть 5 если нужно

        # Показываем итоги цикла
        print(f"\n{'=' * 80}")
        print(f"ИТОГИ ЦИКЛА МОНИТОРИНГА:")
        print(f"Проверено пар: {len(sorted_pairs)}")
        print(f"Найдено крупных сделок: {len(all_large_trades)}")
        print(f"Новых сделок сохранено: {total_new}")
        print(f"Дубликатов пропущено: {total_duplicates}")

        # Показываем статистику из БД
        recent_count = await db_manager.get_recent_trades_count(24)
        print(f"Всего в БД за 24 часа: {recent_count}")
        print(f"{'=' * 80}\n")

    except Exception as e: # Более общее исключение для отлова ClientError и других
        logger.error(f"Ошибка в цикле мониторинга: {e}")
        # Не будем здесь вызывать raise, чтобы цикл мог продолжаться после ошибки
        # raise # Раскомментируйте, если хотите, чтобы программа завершалась при ошибке в цикле


async def main(verify_ssl: bool = True) -> None:
    """
    Основная функция программы с бесконечным циклом мониторинга.

    Args:
        verify_ssl: Проверять ли SSL сертификаты
    """
    # Инициализируем менеджер БД
    db_manager = DatabaseManager()

    try:
        # Подключаемся к БД
        await db_manager.connect()
        await db_manager.create_tables()

        # Создаем SSL контекст
        ssl_context = create_ssl_context(verify_ssl)

        # Настраиваем коннектор
        timeout = aiohttp.ClientTimeout(total=30) # Увеличен общий таймаут
        connector = TCPConnector(
            ssl=ssl_context,
            limit=50, # Максимальное количество одновременных подключений
            limit_per_host=10 # Максимальное количество одновременных подключений к одному хосту
        )

        async with aiohttp.ClientSession(
                connector=connector,
                timeout=timeout
        ) as session:
            # Тестируем соединение
            if not await test_connection(session):
                if verify_ssl:
                    logger.info("Пробуем подключиться без проверки SSL...")
                    # Рекурсивный вызов main здесь может быть не лучшей идеей,
                    # лучше передавать флаг напрямую или изменить логику.
                    # Пока оставим как есть, но стоит иметь в виду.
                    await main(verify_ssl=False)
                    return
                else:
                    logger.error("Не удалось подключиться к Binance API даже без проверки SSL. Завершение работы.")
                    return

            # Бесконечный цикл мониторинга
            cycle_count = 0
            while True:
                cycle_count += 1
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                print(f"\n{'#' * 80}")
                print(f"НАЧАЛО ЦИКЛА #{cycle_count} | Время: {current_time}")
                print(f"{'#' * 80}\n")

                try:
                    # Выполняем цикл мониторинга
                    await run_monitoring_cycle(session, db_manager)

                    # Пауза между циклами
                    pause_minutes = 0 # Было 5, изменил на 0 для более быстрого повтора. Верните 5, если нужно.
                    if pause_minutes > 0:
                        logger.info(f"Цикл #{cycle_count} завершен. Пауза {pause_minutes} минут до следующего цикла...")
                        # Показываем обратный отсчет
                        for remaining in range(pause_minutes * 60, 0, -30): # Обновление каждые 30 сек
                            minutes, seconds = divmod(remaining, 60)
                            print(f"\rСледующий цикл через: {minutes:02d}:{seconds:02d}", end='', flush=True)
                            await asyncio.sleep(min(30, remaining)) # Ждем не более 30 сек или оставшееся время
                        print("\r" + " " * 30 + "\r", end='') # Очистка строки обратного отсчета
                    else:
                        logger.info(f"Цикл #{cycle_count} завершен. Следующий цикл начнется немедленно.")
                        await asyncio.sleep(1) # Минимальная пауза, чтобы избежать слишком частого старта

                except KeyboardInterrupt:
                    logger.info("Получен сигнал остановки (Ctrl+C)")
                    break
                except ClientError as e: # Отдельная обработка ClientError для логирования
                    logger.error(f"Ошибка HTTP клиента в цикле #{cycle_count}: {e}")
                    logger.info("Повтор через 1 минуту...")
                    await asyncio.sleep(60)
                except Exception as e:
                    logger.error(f"Непредвиденная ошибка в цикле #{cycle_count}: {e}")
                    logger.info("Повтор через 1 минуту...")
                    await asyncio.sleep(60)

    except KeyboardInterrupt:
        logger.info("Программа остановлена пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка на уровне main: {e}")
    finally:
        # Закрываем соединение с БД
        if db_manager.pool: # Проверка, что пул был создан
            await db_manager.close()
        logger.info("Мониторинг завершен")


if __name__ == "__main__":
    # Проверяем переменные окружения
    if os.environ.get('DISABLE_SSL_VERIFY', '').lower() == 'true':
        logger.warning("SSL проверка отключена через переменную окружения")
        asyncio.run(main(verify_ssl=False))
    else:
        asyncio.run(main(verify_ssl=True))