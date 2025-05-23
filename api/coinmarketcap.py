"""
Модуль для работы с CoinMarketCap API
"""
import aiohttp
import asyncio
import ssl
import certifi
from typing import Dict, List, Optional, Any
from decimal import Decimal, InvalidOperation
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.config import Config
from utils.logger import logger

class CoinMarketCapAPI:
    """Класс для работы с CoinMarketCap API"""

    def __init__(self):
        self.config = Config()
        self.base_url = self.config.COINMARKETCAP_BASE_URL
        self.api_key = self.config.COINMARKETCAP_API_KEY
        self.session = None
        self.rate_limiter = asyncio.Semaphore(5)

    async def __aenter__(self):
        """Вход в контекстный менеджер"""
        headers = {
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip'
        }

        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(
            limit=20,
            limit_per_host=10,
            ssl=ssl_context
        )

        self.session = aiohttp.ClientSession(
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30),
            connector=connector
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекстного менеджера"""
        if self.session:
            await self.session.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError))
    )
    async def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Выполнение HTTP запроса с повторными попытками"""
        url = f"{self.base_url}{endpoint}"

        async with self.rate_limiter:
            try:
                async with self.session.get(url, params=params) as response:
                    data = await response.json()

                    if response.status == 429:
                        logger.warning("CoinMarketCap rate limit exceeded. Waiting 60 seconds")
                        await asyncio.sleep(60)
                        raise aiohttp.ClientError("Rate limit exceeded")

                    status = data.get('status', {})
                    if status.get('error_code') != 0 and status.get('error_code') is not None:
                        error_msg = status.get('error_message', 'Unknown error')
                        logger.error(f"CoinMarketCap API error: {error_msg}")
                        raise aiohttp.ClientError(f"API error: {error_msg}")

                    return data.get('data', {})

            except aiohttp.ClientConnectorCertificateError as e:
                logger.error(f"SSL Certificate error при запросе к CoinMarketCap API: {e}")
                raise
            except asyncio.TimeoutError:
                logger.error(f"Timeout при запросе к CoinMarketCap API: {endpoint}")
                raise
            except Exception as e:
                logger.error(f"Ошибка при запросе к CoinMarketCap API: {e}")
                raise

    async def get_quotes_latest(self, symbols: List[str]) -> Dict[str, Any]:
        """Получить последние котировки для списка символов"""
        try:
            # CoinMarketCap позволяет запрашивать до 200 символов за раз
            chunks = [symbols[i:i+200] for i in range(0, len(symbols), 200)]
            all_data = {}

            for chunk in chunks:
                params = {
                    'symbol': ','.join(chunk),
                    'convert': 'USD'
                }

                data = await self._make_request('/v1/cryptocurrency/quotes/latest', params)
                all_data.update(data)

                if len(chunks) > 1:
                    await asyncio.sleep(2)

            return all_data

        except Exception as e:
            logger.error(f"Ошибка при получении котировок CoinMarketCap: {e}")
            raise

    async def get_btc_price(self) -> Optional[Decimal]:
        """Получить текущую цену BTC в USD"""
        try:
            data = await self.get_quotes_latest(['BTC'])
            btc_data = data.get('BTC', {})
            quote = btc_data.get('quote', {}).get('USD', {})
            price = quote.get('price')

            if price is not None:
                return Decimal(str(price))
            return None

        except Exception as e:
            logger.error(f"Ошибка при получении цены BTC: {e}")
            return None

    def safe_decimal(self, value: Any, default: Decimal = Decimal('0')) -> Decimal:
        """Безопасное преобразование в Decimal"""
        if value is None:
            return default
        try:
            # Преобразуем в строку и затем в Decimal
            return Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.warning(f"Не удалось преобразовать {value} в Decimal: {e}")
            return default

    def extract_token_data(self, symbol: str, token_data: Dict[str, Any]) -> Dict[str, Any]:
        """Извлечь данные токена из ответа API"""
        try:
            # ИСПРАВЛЕНИЕ: token_data уже содержит данные конкретного токена
            # Не нужно искать по ключу symbol
            quote = token_data.get('quote', {}).get('USD', {})

            # Безопасное извлечение и преобразование данных
            result = {
                'symbol': symbol,
                'price_usd': self.safe_decimal(quote.get('price')),
                'volume_24h_usd': self.safe_decimal(quote.get('volume_24h')),
                'market_cap_usd': self.safe_decimal(quote.get('market_cap')),
                'percent_change_24h': self.safe_decimal(quote.get('percent_change_24h')),
                'circulating_supply': self.safe_decimal(token_data.get('circulating_supply')),
                'total_supply': self.safe_decimal(token_data.get('total_supply')),
                'max_supply': self.safe_decimal(token_data.get('max_supply')) if token_data.get('max_supply') else None
            }

            logger.debug(f"Извлечены данные для {symbol}: price={result['price_usd']}, volume={result['volume_24h_usd']}, mcap={result['market_cap_usd']}")

            return result

        except Exception as e:
            logger.error(f"Ошибка при извлечении данных токена {symbol}: {e}")
            return {
                'symbol': symbol,
                'price_usd': Decimal('0'),
                'volume_24h_usd': Decimal('0'),
                'market_cap_usd': Decimal('0'),
                'percent_change_24h': Decimal('0'),
                'circulating_supply': Decimal('0'),
                'total_supply': Decimal('0'),
                'max_supply': None
            }

    async def get_tokens_data(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Получить данные для списка токенов"""
        try:
            # Убираем дубликаты и приводим к верхнему регистру
            unique_symbols = list(set(symbol.upper() for symbol in symbols if symbol))

            if not unique_symbols:
                logger.warning("Список символов пуст")
                return {}

            logger.info(f"Запрос данных CoinMarketCap для {len(unique_symbols)} токенов")

            quotes_data = await self.get_quotes_latest(unique_symbols)

            result = {}
            for symbol in unique_symbols:
                if symbol in quotes_data:
                    # ИСПРАВЛЕНИЕ: передаем quotes_data[symbol] напрямую
                    result[symbol] = self.extract_token_data(symbol, quotes_data[symbol])
                else:
                    logger.warning(f"Данные для токена {symbol} не найдены в CoinMarketCap")

            return result

        except Exception as e:
            logger.error(f"Ошибка при получении данных токенов: {e}")
            return {}