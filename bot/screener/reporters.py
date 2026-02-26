"""
Модуль для формирования отчетов о результатах скрининга.
"""

from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from tabulate import tabulate

from .stock_screener import StockScore
from .strategy_matcher import StrategyMatcher


class ConsoleReporter:
    """
    Формирует красивые отчеты для вывода в консоль.
    """
    
    @staticmethod
    def print_top_stocks(scores: List[StockScore], title: str = "ТОП АКЦИЙ ДЛЯ ПОКУПКИ"):
        """
        Выводит топ акций в консоль.
        """
        print("\n" + "=" * 80)
        print(f"📊 {title}")
        print("=" * 80)
        print(f"Время анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 80)
        
        table_data = []
        for i, score in enumerate(scores, 1):
            # Определяем эмодзи для рекомендации
            rec_emoji = {
                'STRONG_BUY': '🟢',
                'BUY': '✅',
                'HOLD': '⚪',
                'SELL': '🔻',
                'STRONG_SELL': '🔴'
            }.get(score.recommendation, '⚪')
            
            table_data.append([
                i,
                score.ticker,
                score.name[:20],
                f"{score.price:.2f} ₽",
                f"{score.rsi:.1f}",
                score.trend_long.upper(),
                f"{score.atr_percent:.1f}%",
                f"{score.total_score:.1f}",
                f"{rec_emoji} {score.recommendation}"
            ])
        
        headers = ['#', 'Тикер', 'Компания', 'Цена', 'RSI', 'Тренд', 'Волат.', 'Оценка', 'Рекомендация']
        print(tabulate(table_data, headers=headers, tablefmt='grid'))
        print("=" * 80)
    
    @staticmethod
    def print_strategy_stocks(matcher: StrategyMatcher, all_scores: List[StockScore]):
        """
        Выводит акции, подходящие для разных стратегий.
        """
        print("\n" + "=" * 80)
        print("🎯 АКЦИИ ДЛЯ РАЗНЫХ СТРАТЕГИЙ")
        print("=" * 80)
        
        # 1. SMA Crossover
        print("\n📈 SMA Crossover (трендовая стратегия):")
        sma_stocks = matcher.get_for_sma_crossover(all_scores)
        for i, stock in enumerate(sma_stocks, 1):
            print(f"  {i}. {stock['ticker']} - {stock['name']} | "
                  f"Цена: {stock['price']:.2f} | RSI: {stock['rsi']:.1f}")
        
        # 2. RSI Mean Reversion
        print("\n📉 RSI Mean Reversion (возврат к среднему):")
        rsi_stocks = matcher.get_for_rsi_mean_reversion(all_scores)
        for i, stock in enumerate(rsi_stocks, 1):
            print(f"  {i}. {stock['ticker']} - {stock['name']} | "
                  f"RSI: {stock['rsi']:.1f} | BB позиция: {stock['bb_position']:.2f}")
        
        # 3. Momentum
        print("\n⚡ Momentum (сильные движения):")
        mom_stocks = matcher.get_for_momentum(all_scores)
        for i, stock in enumerate(mom_stocks, 1):
            print(f"  {i}. {stock['ticker']} - {stock['name']} | "
                  f"Momentum: {stock['momentum']:.2f} | Тренд: {stock['trend']}")
        
        # 4. Value
        print("\n💰 Value (стоимостные):")
        val_stocks = matcher.get_for_value(all_scores)
        for i, stock in enumerate(val_stocks, 1):
            print(f"  {i}. {stock['ticker']} - {stock['name']} | "
                  f"RSI: {stock['rsi']:.1f} | Волат.: {stock['atr_percent']:.1f}%")
        
        # 5. Парный трейдинг
        print("\n🔄 Потенциальные пары для парного трейдинга:")
        pairs = matcher.get_for_pair_trading(all_scores, all_scores)
        for i, pair in enumerate(pairs, 1):
            print(f"  {i}. {pair['sector']}: {pair['pair'][0]} / {pair['pair'][1]} "
                  f"(RSI спред: {pair['rsi_spread']:.1f})")
    
    @staticmethod
    def print_by_sector(scores: List[StockScore]):
        """
        Выводит лучшие акции по секторам.
        """
        print("\n" + "=" * 80)
        print("🏭 ЛУЧШИЕ АКЦИИ ПО СЕКТОРАМ")
        print("=" * 80)
        
        # Группируем по секторам
        sectors = {}
        for score in scores:
            if score.sector not in sectors:
                sectors[score.sector] = []
            sectors[score.sector].append(score)
        
        for sector, sector_stocks in sectors.items():
            print(f"\n📌 {sector}:")
            top3 = sorted(sector_stocks, key=lambda x: x.total_score, reverse=True)[:3]
            for stock in top3:
                print(f"  • {stock.ticker} - {stock.name} | "
                      f"Оценка: {stock.total_score:.1f} | Рекомендация: {stock.recommendation}")
    
    @staticmethod
    def print_ranker_info(ranker, scores: List[StockScore]):
        """
        Выводит информацию от ранкера.
        """
        print("\n" + "=" * 80)
        print("📋 ДЕТАЛЬНЫЙ АНАЛИЗ")
        print("=" * 80)
        
        # По итоговой оценке
        print("\n🏆 По итоговой оценке:")
        by_score = ranker.rank_by_total_score(scores)[:5]
        for item in by_score:
            print(f"  {item['ticker']}: {item['total_score']:.1f} - {item['recommendation']}")
        
        # По RSI
        print("\n📊 По RSI (перепроданные):")
        by_rsi = ranker.rank_by_rsi(scores)[:5]
        for item in by_rsi:
            print(f"  {item['ticker']}: RSI={item['rsi']:.1f} - {item['signal']}")
        
        # По волатильности
        print("\n🌊 По волатильности (низкая):")
        by_vol = ranker.rank_by_volatility(scores)[:5]
        for item in by_vol:
            print(f"  {item['ticker']}: ATR={item['atr_percent']:.1f}% - {item['volatility']}")
        
        # По позиции в BB
        print("\n📉 У нижней полосы Боллинджера:")
        by_bb = ranker.rank_by_bb_position(scores)[:5]
        for item in by_bb:
            print(f"  {item['ticker']}: позиция={item['bb_position']:.2f}")


class HTMLReporter:
    """
    Формирует HTML-отчеты для сохранения в файл.
    """
    
    @staticmethod
    def generate_html_report(scores: List[StockScore], filename: str = "stock_report.html"):
        """
        Генерирует HTML-отчет.
        """
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Анализ акций</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .STRONG_BUY {{ background-color: #00ff00; }}
                .BUY {{ background-color: #90ff90; }}
                .HOLD {{ background-color: #ffff90; }}
                .SELL {{ background-color: #ff9090; }}
                .STRONG_SELL {{ background-color: #ff0000; color: white; }}
            </style>
        </head>
        <body>
            <h1>📊 Анализ акций</h1>
            <p>Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <h2>Топ акций для покупки</h2>
            <table>
                <tr>
                    <th>#</th>
                    <th>Тикер</th>
                    <th>Компания</th>
                    <th>Сектор</th>
                    <th>Цена</th>
                    <th>RSI</th>
                    <th>Тренд</th>
                    <th>Оценка</th>
                    <th>Рекомендация</th>
                </tr>
        """
        
        for i, score in enumerate(scores[:20], 1):
            html += f"""
                <tr class="{score.recommendation}">
                    <td>{i}</td>
                    <td>{score.ticker}</td>
                    <td>{score.name}</td>
                    <td>{score.sector}</td>
                    <td>{score.price:.2f}</td>
                    <td>{score.rsi:.1f}</td>
                    <td>{score.trend_long}</td>
                    <td>{score.total_score:.1f}</td>
                    <td>{score.recommendation}</td>
                </tr>
            """
        
        html += """
            </table>
        </body>
        </html>
        """
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html)
        
        print(f"✅ HTML-отчет сохранен в {filename}")