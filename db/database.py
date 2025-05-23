"""
Модуль для работы с базой данных MySQL
"""
import aiomysql
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal
from contextlib import asynccontextmanager
from utils.config import Config
from utils.logger import logger


class Database:
    """Класс для работы с MySQL базой данных"""

    def __init__(self):
        self.config = Config()
        self.pool = None

    async def connect(self):
        """Создание пула соединений с базой данных"""
        try:
            self.pool = await aiomysql.create_pool(
                host=self.config.MYSQL_HOST,
                port=self.config.MYSQL_PORT,
                user=self.config.MYSQL_USER,
                password=self.config.MYSQL_PASSWORD,
                db=self.config.MYSQL_DATABASE,
                charset='utf8mb4',
                autocommit=True,
                minsize=5,
                maxsize=20,
                echo=False
            )
            logger.info("Успешное подключение к базе данных MySQL")
        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            raise

    async def disconnect(self):
        """Закрытие пула соединений"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            logger.info("Соединение с базой данных закрыто")

    @asynccontextmanager
    async def get_cursor(self):
        """Контекстный менеджер для получения курсора"""
        async with self.pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                yield cursor

    async def get_or_create_token(self, symbol: str) -> int:
        """Получить или создать токен, возвращает ID токена"""
        symbol = symbol.upper()

        async with self.get_cursor() as cursor:
            # Проверяем существование токена
            await cursor.execute(
                "SELECT id FROM tokens WHERE symbol = %s",
                (symbol,)
            )
            result = await cursor.fetchone()

            if result:
                return result['id']

            # Создаем новый токен
            await cursor.execute(
                "INSERT INTO tokens (symbol) VALUES (%s)",
                (symbol,)
            )
            return cursor.lastrowid

    async def get_or_create_futures_pair(self, token_id: int, exchange: str,
                                         pair_symbol: str, contract_type: str = 'PERPETUAL') -> int:
        """Получить или создать фьючерсную пару, возвращает ID пары"""
        async with self.get_cursor() as cursor:
            # Проверяем существование пары
            await cursor.execute(
                """SELECT id
                   FROM futures_pairs
                   WHERE exchange = %s
                     AND pair_symbol = %s""",
                (exchange, pair_symbol)
            )
            result = await cursor.fetchone()

            if result:
                return result['id']

            # Создаем новую пару
            await cursor.execute(
                """INSERT INTO futures_pairs
                       (token_id, exchange, pair_symbol, contract_type)
                   VALUES (%s, %s, %s, %s)""",
                (token_id, exchange, pair_symbol, contract_type)
            )
            return cursor.lastrowid

    async def save_futures_data(self, data: Dict[str, Any]):
        """Сохранение данных о фьючерсах"""
        async with self.get_cursor() as cursor:
            await cursor.execute(
                """INSERT INTO futures_data
                   (pair_id, open_interest_contracts, open_interest_usd,
                    funding_rate, volume_btc, volume_usd, price_usd,
                    market_cap_usd, btc_price)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    data['pair_id'],
                    data.get('open_interest_contracts'),
                    data.get('open_interest_usd'),
                    data.get('funding_rate'),
                    data.get('volume_btc'),
                    data.get('volume_usd'),
                    data.get('price_usd'),
                    data.get('market_cap_usd'),
                    data.get('btc_price')
                )
            )

    async def save_api_error(self, exchange: str, endpoint: str,
                             error_code: str, error_message: str):
        """Сохранение информации об ошибке API"""
        async with self.get_cursor() as cursor:
            await cursor.execute(
                """INSERT INTO api_errors
                       (exchange, endpoint, error_code, error_message)
                   VALUES (%s, %s, %s, %s)""",
                (exchange, endpoint, error_code, error_message[:1000])  # Ограничиваем длину сообщения
            )

    async def get_cached_cmc_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Получить кэшированные данные CoinMarketCap"""
        async with self.get_cursor() as cursor:
            await cursor.execute(
                """SELECT *
                   FROM cmc_cache
                   WHERE symbol = %s
                     AND last_updated > DATE_SUB(NOW(), INTERVAL 10 MINUTE)""",
                (symbol,)
            )
            return await cursor.fetchone()

    async def save_cmc_cache(self, symbol: str, price_usd: Decimal,
                             volume_24h_usd: Decimal, market_cap_usd: Decimal):
        """Сохранить данные в кэш CoinMarketCap"""
        async with self.get_cursor() as cursor:
            await cursor.execute(
                """INSERT INTO cmc_cache
                       (symbol, price_usd, volume_24h_usd, market_cap_usd)
                   VALUES (%s, %s, %s, %s) ON DUPLICATE KEY
                UPDATE
                    price_usd =
                VALUES (price_usd), volume_24h_usd =
                VALUES (volume_24h_usd), market_cap_usd =
                VALUES (market_cap_usd), last_updated = CURRENT_TIMESTAMP""",
                (symbol, price_usd, volume_24h_usd, market_cap_usd)
            )

    async def get_all_futures_pairs(self) -> List[Dict[str, Any]]:
        """Получить все фьючерсные пары из базы данных"""
        async with self.get_cursor() as cursor:
            await cursor.execute(
                """SELECT fp.*, t.symbol as token_symbol
                   FROM futures_pairs fp
                            JOIN tokens t ON fp.token_id = t.id"""
            )
            return await cursor.fetchall()