"""
Модуль для работы с Binance Futures API
"""
import aiohttp
import asyncio
import ssl
import certifi
from typing import Dict, List, Optional, Any
from decimal import Decimal
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from utils.config import Config
from utils.logger import logger
from utils.converters import DataConverter

class BinanceAPI:
    """Класс для работы с Binance Futures API"""

    def __init__(self):
        self.config = Config()
        self.base_url = self.config.BINANCE_BASE_URL
        self.session = None
        self.rate_limiter = asyncio.Semaphore(20)  # Ограничение параллельных запросов

    async def __aenter__(self):
        """Вход в контекстный менеджер"""
        # Создаем SSL контекст с правильными сертификатами
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE  # Временное решение для отладки

        # Создаем коннектор с SSL контекстом
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            ssl=ssl_context
        )

        # Создаем сессию с таймаутом и коннектором
        self.session = aiohttp.ClientSession(
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
                    if response.status == 429:
                        # Превышен лимит запросов
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Binance rate limit exceeded. Waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                        raise aiohttp.ClientError("Rate limit exceeded")

                    if response.status != 200:
                        text = await response.text()
                        logger.error(f"Binance API error: {response.status} - {text}")
                        raise aiohttp.ClientError(f"API error: {response.status}")

                    return await response.json()

            except aiohttp.ClientConnectorCertificateError as e:
                logger.error(f"SSL Certificate error при запросе к Binance API: {e}")
                raise
            except asyncio.TimeoutError:
                logger.error(f"Timeout при запросе к Binance API: {endpoint}")
                raise
            except Exception as e:
                logger.error(f"Ошибка при запросе к Binance API: {e}")
                raise

    async def get_exchange_info(self) -> Dict[str, Any]:
        """Получить информацию о бирже и торговых парах"""
        logger.info("Получение информации о бирже Binance")
        return await self._make_request('/fapi/v1/exchangeInfo')

    async def get_futures_pairs(self) -> List[Dict[str, Any]]:
        """Получить список всех фьючерсных пар"""
        try:
            exchange_info = await self.get_exchange_info()
            symbols = exchange_info.get('symbols', [])

            # Фильтруем только активные PERPETUAL контракты
            futures_pairs = []
            for symbol in symbols:
                if (symbol.get('status') == 'TRADING' and
                    symbol.get('contractType') == 'PERPETUAL' and
                    symbol.get('quoteAsset') in ['USDT', 'BUSD']):

                    futures_pairs.append({
                        'symbol': symbol['symbol'],
                        'baseAsset': symbol['baseAsset'],
                        'quoteAsset': symbol['quoteAsset'],
                        'contractType': symbol['contractType'],
                        'pricePrecision': symbol['pricePrecision'],
                        'quantityPrecision': symbol['quantityPrecision']
                    })

            logger.info(f"Найдено {len(futures_pairs)} активных фьючерсных пар на Binance")
            return futures_pairs

        except Exception as e:
            logger.error(f"Ошибка при получении списка фьючерсных пар Binance: {e}")
            raise

    async def get_open_interest(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Получить открытый интерес для символа"""
        try:
            data = await self._make_request('/fapi/v1/openInterest', {'symbol': symbol})
            return {
                'symbol': symbol,
                'openInterest': Decimal(data.get('openInterest', '0')),
                'time': data.get('time')
            }
        except Exception as e:
            logger.error(f"Ошибка при получении OI для {symbol}: {e}")
            return None

    async def get_funding_rate(self, symbol: str) -> Optional[Decimal]:
        """Получить текущую ставку финансирования"""
        try:
            data = await self._make_request('/fapi/v1/premiumIndex', {'symbol': symbol})
            funding_rate = data.get('lastFundingRate')
            return Decimal(funding_rate) if funding_rate else None
        except Exception as e:
            logger.error(f"Ошибка при получении funding rate для {symbol}: {e}")
            return None

    async def get_ticker_price(self, symbol: str) -> Optional[Decimal]:
        """Получить текущую цену символа"""
        try:
            data = await self._make_request('/fapi/v1/ticker/price', {'symbol': symbol})
            price = data.get('price')
            return Decimal(price) if price else None
        except Exception as e:
            logger.error(f"Ошибка при получении цены для {symbol}: {e}")
            return None

    async def get_24hr_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Получить 24-часовую статистику по символу"""
        try:
            data = await self._make_request('/fapi/v1/ticker/24hr', {'symbol': symbol})
            return {
                'symbol': symbol,
                'volume': Decimal(data.get('volume', '0')),
                'quoteVolume': Decimal(data.get('quoteVolume', '0')),
                'count': int(data.get('count', 0))
            }
        except Exception as e:
            logger.error(f"Ошибка при получении 24hr ticker для {symbol}: {e}")
            return None

    async def collect_pair_data(self, symbol: str) -> Dict[str, Any]:
        """Собрать все данные для одной пары"""
        logger.debug(f"Сбор данных для пары Binance: {symbol}")

        # Параллельный сбор данных
        tasks = [
            self.get_open_interest(symbol),
            self.get_funding_rate(symbol),
            self.get_ticker_price(symbol),
            self.get_24hr_ticker(symbol)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Обработка результатов
        open_interest_data = results[0] if not isinstance(results[0], Exception) else None
        funding_rate = results[1] if not isinstance(results[1], Exception) else None
        price = results[2] if not isinstance(results[2], Exception) else None
        ticker_data = results[3] if not isinstance(results[3], Exception) else None

        # Расчет OI в USD
        open_interest_usd = None
        if open_interest_data and price:
            open_interest_usd = open_interest_data['openInterest'] * price

        return {
            'exchange': 'Binance',
            'symbol': symbol,
            'open_interest_contracts': open_interest_data['openInterest'] if open_interest_data else None,
            'open_interest_usd': open_interest_usd,
            'funding_rate': funding_rate,
            'price': price,
            'volume_24h': ticker_data['quoteVolume'] if ticker_data else None,
            'trade_count_24h': ticker_data['count'] if ticker_data else None
        }