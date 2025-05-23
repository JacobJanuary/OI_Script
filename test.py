import requests


def analyze_btc_pairs():
    """Анализ торговых пар к BTC на Binance"""

    print("Получаем данные с Binance API...")

    try:
        # Получаем 24-часовую статистику для всех пар
        response = requests.get('https://api.binance.com/api/v3/ticker/24hr')
        response.raise_for_status()
        data = response.json()

        # Фильтруем только пары к BTC
        btc_pairs = []

        for ticker in data:
            if ticker['symbol'].endswith('BTC'):
                quote_volume = float(ticker['quoteVolume'])
                btc_pairs.append({
                    'symbol': ticker['symbol'],
                    'volume': float(ticker['volume']),
                    'quoteVolume': quote_volume,
                    'count': int(ticker['count']),
                    'priceChange': float(ticker['priceChange']),
                    'priceChangePercent': float(ticker['priceChangePercent']),
                    'lastPrice': float(ticker['lastPrice'])
                })

        # Общая статистика
        total_pairs = len(btc_pairs)
        pairs_less_1btc = [pair for pair in btc_pairs if pair['quoteVolume'] < 1.0]
        pairs_more_1btc = [pair for pair in btc_pairs if pair['quoteVolume'] >= 1.0]

        # Сортируем пары с объемом >= 1 BTC по убыванию
        pairs_more_1btc.sort(key=lambda x: x['quoteVolume'], reverse=True)

        # Выводим статистику
        print("=" * 80)
        print("📊 СТАТИСТИКА ТОРГОВЫХ ПАР К BTC НА BINANCE")
        print("=" * 80)
        print(f"🔹 Всего пар торгуется к BTC: {total_pairs}")
        print(f"🔸 Пар с объемом менее 1 BTC: {len(pairs_less_1btc)} ({len(pairs_less_1btc) / total_pairs * 100:.1f}%)")
        print(
            f"🔸 Пар с объемом 1 BTC и более: {len(pairs_more_1btc)} ({len(pairs_more_1btc) / total_pairs * 100:.1f}%)")

        # Общий объем торгов
        total_volume = sum(pair['quoteVolume'] for pair in btc_pairs)
        volume_more_1btc = sum(pair['quoteVolume'] for pair in pairs_more_1btc)

        print(f"🔸 Общий объем торгов всех пар: {total_volume:.2f} BTC")
        print(f"🔸 Объем пар >= 1 BTC: {volume_more_1btc:.2f} BTC ({volume_more_1btc / total_volume * 100:.1f}%)")

        # Выводим список пар с объемом >= 1 BTC
        if pairs_more_1btc:
            print("\n" + "=" * 80)
            print("📈 ПАРЫ С ОБЪЕМОМ >= 1 BTC (ОТСОРТИРОВАНЫ ПО УБЫВАНИЮ)")
            print("=" * 80)
            print(f"{'№':>3} {'Пара':15} {'Объем (BTC)':>15} {'Сделок':>8} {'Цена':>12} {'Изм. %':>8}")
            print("-" * 80)

            for i, pair in enumerate(pairs_more_1btc, 1):
                price_change_color = "📈" if pair['priceChangePercent'] >= 0 else "📉"
                print(f"{i:3d} {pair['symbol']:15} {pair['quoteVolume']:>15.2f} "
                      f"{pair['count']:>8,} {pair['lastPrice']:>12.8f} "
                      f"{price_change_color}{pair['priceChangePercent']:>6.2f}%")

            print("-" * 80)
            print(f"Топ-3 по объему:")
            for i, pair in enumerate(pairs_more_1btc[:3], 1):
                print(f"  {i}. {pair['symbol']}: {pair['quoteVolume']:,.2f} BTC")

        # Дополнительная аналитика
        if btc_pairs:
            print(f"\n📊 ДОПОЛНИТЕЛЬНАЯ АНАЛИТИКА:")
            print(f"🔸 Средний объем на пару: {total_volume / total_pairs:.4f} BTC")

            # Медиана
            sorted_volumes = sorted([p['quoteVolume'] for p in btc_pairs])
            median_volume = sorted_volumes[len(sorted_volumes) // 2]
            print(f"🔸 Медианный объем: {median_volume:.8f} BTC")

            # Пары с разными объемами
            pairs_0_01 = len([p for p in btc_pairs if 0.01 <= p['quoteVolume'] < 0.1])
            pairs_0_1 = len([p for p in btc_pairs if 0.1 <= p['quoteVolume'] < 1])
            pairs_1_10 = len([p for p in btc_pairs if 1 <= p['quoteVolume'] < 10])
            pairs_10_plus = len([p for p in btc_pairs if p['quoteVolume'] >= 10])

            print(f"🔸 Распределение по объемам:")
            print(f"   • 0.01-0.1 BTC: {pairs_0_01} пар")
            print(f"   • 0.1-1 BTC: {pairs_0_1} пар")
            print(f"   • 1-10 BTC: {pairs_1_10} пар")
            print(f"   • 10+ BTC: {pairs_10_plus} пар")

            # Самая активная и неактивная пара
            most_active = max(btc_pairs, key=lambda x: x['quoteVolume'])
            least_active = min([p for p in btc_pairs if p['quoteVolume'] > 0],
                               key=lambda x: x['quoteVolume'], default=btc_pairs[0])

            print(f"🔸 Самая активная пара: {most_active['symbol']} ({most_active['quoteVolume']:.2f} BTC)")
            print(f"🔸 Наименее активная пара: {least_active['symbol']} ({least_active['quoteVolume']:.8f} BTC)")

        print("=" * 80)

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе к API: {e}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")


# Запускаем анализ
if __name__ == "__main__":
    analyze_btc_pairs()