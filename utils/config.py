"""
Модуль для работы с конфигурацией
"""
import os
from dotenv import load_dotenv

class Config:
    """Класс для управления конфигурацией приложения"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._initialized = True
        load_dotenv()

        # CoinMarketCap
        self.COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY')

        # MySQL
        self.MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
        self.MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
        self.MYSQL_USER = os.getenv('MYSQL_USER')
        self.MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
        self.MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

        # API Rate Limits
        self.BINANCE_RATE_LIMIT = int(os.getenv('BINANCE_RATE_LIMIT', 1200))
        self.BYBIT_RATE_LIMIT = int(os.getenv('BYBIT_RATE_LIMIT', 120))
        self.COINMARKETCAP_RATE_LIMIT = int(os.getenv('COINMARKETCAP_RATE_LIMIT', 30))

        # Logging
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_FILE = os.getenv('LOG_FILE', 'script.log')

        # API URLs
        self.BINANCE_BASE_URL = 'https://fapi.binance.com'
        self.BYBIT_BASE_URL = 'https://api.bybit.com'
        self.COINMARKETCAP_BASE_URL = 'https://pro-api.coinmarketcap.com'

        # SSL Settings
        self.SSL_VERIFY = os.getenv('SSL_VERIFY', 'True').lower() == 'true'

    def validate(self):
        """Проверка обязательных параметров конфигурации"""
        required_params = [
            'COINMARKETCAP_API_KEY',
            'MYSQL_USER',
            'MYSQL_PASSWORD',
            'MYSQL_DATABASE'
        ]

        missing_params = []
        for param in required_params:
            if not getattr(self, param):
                missing_params.append(param)

        if missing_params:
            raise ValueError(f"Отсутствуют обязательные параметры конфигурации: {', '.join(missing_params)}")

        return True