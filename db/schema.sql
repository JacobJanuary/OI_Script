-- Создание базы данных
CREATE DATABASE IF NOT EXISTS crypto_futures_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE crypto_futures_db;

-- Таблица токенов
CREATE TABLE IF NOT EXISTS tokens (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(50) UNIQUE NOT NULL COMMENT 'Символ токена, например SUI',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_symbol (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Таблица фьючерсных пар
CREATE TABLE IF NOT EXISTS futures_pairs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    token_id INT NOT NULL,
    exchange VARCHAR(20) NOT NULL COMMENT 'Биржа: Binance, Bybit',
    pair_symbol VARCHAR(50) NOT NULL COMMENT 'Символ пары, например SUIUSDT',
    contract_type VARCHAR(20) DEFAULT 'PERPETUAL' COMMENT 'Тип контракта',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (token_id) REFERENCES tokens(id) ON DELETE CASCADE,
    UNIQUE KEY unique_exchange_pair (exchange, pair_symbol),
    INDEX idx_exchange (exchange),
    INDEX idx_pair_symbol (pair_symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Таблица исторических данных
CREATE TABLE IF NOT EXISTS futures_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pair_id INT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    open_interest_contracts DECIMAL(18, 2) COMMENT 'OI в контрактах (для Binance)',
    open_interest_usd DECIMAL(18, 2) COMMENT 'OI в USD',
    funding_rate DECIMAL(10, 8) COMMENT 'Фандинговая ставка',
    volume_btc DECIMAL(18, 8) COMMENT 'Объем торгов в BTC',
    volume_usd DECIMAL(18, 2) COMMENT 'Объем торгов в USD (из CoinMarketCap)',
    price_usd DECIMAL(18, 2) COMMENT 'Цена в USD (из CoinMarketCap)',
    market_cap_usd DECIMAL(18, 2) COMMENT 'Рыночная капитализация в USD',
    btc_price DECIMAL(18, 2) COMMENT 'Цена BTC в USD на момент записи',
    FOREIGN KEY (pair_id) REFERENCES futures_pairs(id) ON DELETE CASCADE,
    INDEX idx_timestamp (timestamp),
    INDEX idx_pair_timestamp (pair_id, timestamp),
    INDEX idx_pair_id (pair_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS spot_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    pair_id INT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    volume_btc DECIMAL(18, 8) COMMENT 'Объем торгов в BTC',
    FOREIGN KEY (pair_id) REFERENCES futures_pairs(id) ON DELETE CASCADE,
    INDEX idx_timestamp (timestamp),
    INDEX idx_pair_timestamp (pair_id, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- Таблица ошибок API
CREATE TABLE IF NOT EXISTS api_errors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,
    endpoint VARCHAR(255),
    error_code VARCHAR(50),
    error_message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_exchange_timestamp (exchange, timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Таблица для кэширования данных CoinMarketCap
CREATE TABLE IF NOT EXISTS cmc_cache (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    price_usd DECIMAL(18, 2),
    volume_24h_usd DECIMAL(18, 2),
    market_cap_usd DECIMAL(18, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol (symbol),
    INDEX idx_last_updated (last_updated)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;