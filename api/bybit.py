"""
Модуль для работы с Bybit API V5
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

class BybitAPI:
    """Класс для работы с Bybit API V5"""

    def __init__(self):
        self.config = Config()
        self.base_url = self.config.BYBIT_BASE_URL
        self.session = None
        self.rate_limiter = asyncio.Semaphore(10)

    async def __aenter__(self):
        """Вход в контекстный менеджер"""
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=20,
            ssl=ssl_context
        )

        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=connector
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекстный менеджера"""
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
                        logger.warning("Bybit rate limit exceeded. Waiting 10 seconds")
                        await asyncio.sleep(10)
                        raise aiohttp.ClientError("Rate limit exceeded")

                    if data.get('retCode') != 0:
                        error_msg = data.get('retMsg', 'Unknown error')
                        logger.error(f"Bybit API error: {error_msg}")
                        raise aiohttp.ClientError(f"API error: {error_msg}")

                    return data.get('result', {})

            except aiohttp.ClientConnectorCertificateError as e:
                logger.error(f"SSL Certificate error при запросе к Bybit API: {e}")
                raise
            except asyncio.TimeoutError:
                logger.error(f"Timeout при запросе к Bybit API: {endpoint}")
                raise
            except Exception as e:
                logger.error(f"Ошибка при запросе к Bybit API: {e}")
                raise

    async def get_instruments_info(self, category: str = 'linear') -> List[Dict[str, Any]]:
        """Получить информацию об инструментах"""
        logger.info(f"Получение информации об инструментах Bybit (category={category})")

        all_instruments = []
        cursor = None

        while True:
            params = {
                'category': category,
                'limit': 1000
            }
            if cursor:
                params['cursor'] = cursor

            data = await self._make_request('/v5/market/instruments-info', params)
            instruments = data.get('list', [])
            all_instruments.extend(instruments)

            cursor = data.get('nextPageCursor')
            if not cursor:
                break

        return all_instruments

    async def get_futures_pairs(self) -> List[Dict[str, Any]]:
        """Получить список всех фьючерсных пар"""
        try:
            instruments = await self.get_instruments_info('linear')

            futures_pairs = []
            for instrument in instruments:
                if (instrument.get('status') == 'Trading' and
                    instrument.get('quoteCoin') in ['USDT', 'USDC'] and
                    instrument.get('contractType') == 'LinearPerpetual'):

                    futures_pairs.append({
                        'symbol': instrument['symbol'],
                        'baseCoin': instrument['baseCoin'],
                        'quoteCoin': instrument['quoteCoin'],
                        'contractType': 'PERPETUAL',
                        'minPrice': instrument.get('priceFilter', {}).get('minPrice'),
                        'maxPrice': instrument.get('priceFilter', {}).get('maxPrice'),
                        'contractSize': instrument.get('lotSizeFilter', {}).get('basePrecision', '1')
                    })

            logger.info(f"Найдено {len(futures_pairs)} активных фьючерсных пар на Bybit")
            return futures_pairs

        except Exception as e:
            logger.error(f"Ошибка при получении списка фьючерсных пар Bybit: {e}")
            raise

    async def get_open_interest(self, symbol: str, category: str = 'linear') -> Optional[Dict[str, Any]]:
        """Получить открытый интерес для символа"""
        try:
            params = {
                'category': category,
                'symbol': symbol,
                'intervalTime': '5min',
                'limit': 1
            }

            data = await self._make_request('/v5/market/open-interest', params)
            oi_list = data.get('list', [])

            if oi_list:
                latest = oi_list[0]
                # openInterest в Bybit - это количество открытых контрактов в базовой валюте
                oi_contracts = latest.get('openInterest', '0')

                return {
                    'symbol': symbol,
                    'openInterest': Decimal(oi_contracts),  # Количество контрактов
                    'timestamp': latest.get('timestamp')
                }

            return None

        except Exception as e:
            logger.error(f"Ошибка при получении OI для {symbol}: {e}")
            return None

    async def get_funding_rate(self, symbol: str, category: str = 'linear') -> Optional[Decimal]:
        """Получить текущую ставку финансирования"""
        try:
            params = {
                'category': category,
                'symbol': symbol,
                'limit': 1
            }

            data = await self._make_request('/v5/market/funding/history', params)
            funding_list = data.get('list', [])

            if funding_list:
                latest = funding_list[0]
                funding_rate = latest.get('fundingRate')
                return Decimal(funding_rate) if funding_rate else None

            return None

        except Exception as e:
            logger.error(f"Ошибка при получении funding rate для {symbol}: {e}")
            return None

    async def get_ticker(self, symbol: str, category: str = 'linear') -> Optional[Dict[str, Any]]:
        """Получить данные тикера"""
        try:
            params = {
                'category': category,
                'symbol': symbol
            }

            data = await self._make_request('/v5/market/tickers', params)
            tickers = data.get('list', [])

            if tickers:
                ticker = tickers[0]
                return {
                    'symbol': symbol,
                    'lastPrice': Decimal(ticker.get('lastPrice', '0')),
                    'volume24h': Decimal(ticker.get('volume24h', '0')),
                    'turnover24h': Decimal(ticker.get('turnover24h', '0'))
                }

            return None

        except Exception as e:
            logger.error(f"Ошибка при получении ticker для {symbol}: {e}")
            return None

    async def collect_pair_data(self, symbol: str) -> Dict[str, Any]:
        """Собрать все данные для одной пары"""
        logger.debug(f"Сбор данных для пары Bybit: {symbol}")

        tasks = [
            self.get_open_interest(symbol),
            self.get_funding_rate(symbol),
            self.get_ticker(symbol)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        open_interest_data = results[0] if not isinstance(results[0], Exception) else None
        funding_rate = results[1] if not isinstance(results[1], Exception) else None
        ticker_data = results[2] if not isinstance(results[2], Exception) else None

        # Расчет OI в USD
        open_interest_usd = None
        open_interest_contracts = None

        if open_interest_data and ticker_data:
            # openInterest от Bybit - это количество контрактов в базовой валюте
            open_interest_contracts = open_interest_data['openInterest']
            # Для расчета стоимости в USD умножаем на текущую цену
            price = ticker_data['lastPrice']
            open_interest_usd = open_interest_contracts * price

            logger.debug(f"Bybit {symbol}: OI contracts={open_interest_contracts}, price={price}, OI USD={open_interest_usd}")

        return {
            'exchange': 'Bybit',
            'symbol': symbol,
            'open_interest_contracts': open_interest_contracts,  # Количество контрактов
            'open_interest_usd': open_interest_usd,  # Рассчитанная стоимость в USD
            'funding_rate': funding_rate,
            'price': ticker_data['lastPrice'] if ticker_data else None,
            'volume_24h': ticker_data['turnover24h'] if ticker_data else None  # turnover24h это объем в USD
        }