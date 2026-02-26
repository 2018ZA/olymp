#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Скрипт для получения топа акций для покупки.
Запуск: python top_stocks.py [--top N] [--html]
"""

import argparse
import sys
from screener.stock_screener import StockScreener
from screener.strategy_matcher import StrategyMatcher
from screener.ranker import Ranker
from screener.reporters import ConsoleReporter, HTMLReporter
from utils.logger import setup_logger as setup_logging


def main():
    """Главная функция для анализа акций"""
    
    # Настройка аргументов командной строки
    parser = argparse.ArgumentParser(description='Анализ акций и получение топа для покупки')
    parser.add_argument('--top', type=int, default=10, help='Количество акций в топе (по умолчанию 10)')
    parser.add_argument('--html', action='store_true', help='Сохранить HTML-отчет')
    parser.add_argument('--sector', type=str, help='Фильтр по сектору')
    parser.add_argument('--strategy', type=str, choices=['sma', 'rsi', 'momentum', 'value'],
                       help='Показать акции для конкретной стратегии')
    
    args = parser.parse_args()
    
    # Настраиваем логирование
    setup_logging()
    
    print("=" * 80)
    print("📈 АНАЛИЗ АКЦИЙ - ПОИСК ЛУЧШИХ ДЛЯ ПОКУПКИ")
    print("=" * 80)
    
    try:
        # Создаем скринер
        screener = StockScreener()
        
        # Анализируем все акции
        print(f"\n🔄 Анализируем акции...")
        all_scores = screener.analyze_all(min_score=30)
        
        if not all_scores:
            print("❌ Не удалось получить данные для анализа")
            return
        
        print(f"✅ Проанализировано {len(all_scores)} акций")
        
        # Создаем вспомогательные объекты
        matcher = StrategyMatcher()
        ranker = Ranker()
        reporter = ConsoleReporter()
        
        # Если запрошена конкретная стратегия
        if args.strategy:
            print(f"\n🎯 Акции для стратегии: {args.strategy.upper()}")
            
            if args.strategy == 'sma':
                stocks = matcher.get_for_sma_crossover(all_scores, args.top)
                for i, s in enumerate(stocks, 1):
                    print(f"{i}. {s['ticker']} - {s['name']} | Цена: {s['price']:.2f} | RSI: {s['rsi']:.1f}")
            
            elif args.strategy == 'rsi':
                stocks = matcher.get_for_rsi_mean_reversion(all_scores, args.top)
                for i, s in enumerate(stocks, 1):
                    print(f"{i}. {s['ticker']} - {s['name']} | RSI: {s['rsi']:.1f} | BB: {s['bb_position']:.2f}")
            
            elif args.strategy == 'momentum':
                stocks = matcher.get_for_momentum(all_scores, args.top)
                for i, s in enumerate(stocks, 1):
                    print(f"{i}. {s['ticker']} - {s['name']} | Momentum: {s['momentum']:.2f}")
            
            elif args.strategy == 'value':
                stocks = matcher.get_for_value(all_scores, args.top)
                for i, s in enumerate(stocks, 1):
                    print(f"{i}. {s['ticker']} - {s['name']} | RSI: {s['rsi']:.1f}")
        
        # Иначе показываем общий топ
        else:
            # Получаем топ акций
            top_stocks = screener.get_top_stocks(args.top)
            
            # Выводим топ
            reporter.print_top_stocks(top_stocks)
            
            # Показываем по секторам
            if args.sector:
                sector_stocks = screener.get_stocks_by_sector(args.sector)
                print(f"\n📌 Акции сектора {args.sector}:")
                for i, s in enumerate(sector_stocks[:5], 1):
                    print(f"{i}. {s.ticker} - {s.name} | Оценка: {s.total_score:.1f}")
            else:
                reporter.print_by_sector(all_scores)
            
            # Показываем для стратегий
            reporter.print_strategy_stocks(matcher, all_scores)
            
            # Детальный анализ
            reporter.print_ranker_info(ranker, all_scores)
        
        # Сохраняем HTML-отчет если нужно
        if args.html:
            HTMLReporter.generate_html_report(all_scores)
        
    except KeyboardInterrupt:
        print("\n⚠️ Анализ прерван пользователем")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()