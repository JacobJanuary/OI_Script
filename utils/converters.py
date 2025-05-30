"""
Модуль для конвертации данных
"""
from decimal import Decimal
from typing import Optional, Dict, Any
from utils.logger import logger


class DataConverter:
    """Класс для конвертации различных типов данных"""

    @staticmethod
    def extract_token_symbol(pair_symbol: str) -> Optional[str]:
        """
        Извлечение символа токена из символа пары
        Например: SUIUSDT -> SUI, BTCUSDT -> BTC
        """
        # Список стейблкоинов и квотируемых валют
        quote_currencies = ['USDT', 'USDC', 'BUSD', 'USD', 'BTC', 'ETH', 'BNB']

        for quote in quote_currencies:
            if pair_symbol.endswith(quote):
                return pair_symbol[:-len(quote)]

        # Если не найдено совпадение, логируем предупреждение
        logger.warning(f"Не удалось извлечь символ токена из пары: {pair_symbol}")
        return None

    @staticmethod
    def convert_to_usd(amount: Decimal, price: Decimal) -> Decimal:
        """Конвертация суммы в USD по заданной цене"""
        try:
            return Decimal(str(amount)) * Decimal(str(price))
        except Exception as e:
            logger.error(f"Ошибка при конвертации в USD: {e}")
            return Decimal('0')

    @staticmethod
    def convert_to_btc(amount_usd: Decimal, btc_price: Decimal) -> Decimal:
        """Конвертация суммы из USD в BTC"""
        try:
            if btc_price == 0:
                return Decimal('0')
            return Decimal(str(amount_usd)) / Decimal(str(btc_price))
        except Exception as e:
            logger.error(f"Ошибка при конвертации в BTC: {e}")
            return Decimal('0')

    @staticmethod
    def safe_decimal(value: Any, default: Decimal = Decimal('0')) -> Decimal:
        """Безопасное преобразование в Decimal"""
        if value is None:
            return default
        try:
            return Decimal(str(value))
        except Exception:
            return default

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Нормализация символа токена (приведение к верхнему регистру)"""
        return symbol.upper().strip()

    # Добавить этот метод в класс DataConverter в файле utils/converters.py

    @staticmethod
    def extract_token_from_spot_pair(pair_symbol: str) -> Optional[str]:
        """
        Извлечение символа токена из спотовой пары к BTC
        Например: ETHBTC -> ETH, BNBBTC -> BNB
        """
        if pair_symbol.endswith('BTC') and len(pair_symbol) > 3:
            return pair_symbol[:-3]

        # Если формат не соответствует ожидаемому
        logger.warning(f"Не удалось извлечь символ токена из спотовой пары: {pair_symbol}")
        return None