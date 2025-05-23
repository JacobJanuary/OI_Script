"""
Модуль для настройки логирования
"""
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from utils.config import Config


class Logger:
    """Класс для настройки и управления логированием"""

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
        self.config = Config()
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """Настройка логгера с ротацией файлов"""
        logger = logging.getLogger('crypto_futures_collector')
        logger.setLevel(getattr(logging, self.config.LOG_LEVEL))

        # Удаляем существующие обработчики
        logger.handlers = []

        # Формат логов
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Обработчик для файла с ротацией
        file_handler = RotatingFileHandler(
            self.config.LOG_FILE,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Обработчик для консоли
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger

    def get_logger(self):
        """Получить экземпляр логгера"""
        return self.logger


# Глобальный экземпляр логгера
logger = Logger().get_logger()