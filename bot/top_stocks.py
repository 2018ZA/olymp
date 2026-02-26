#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
top_stocks.py - Анализатор акций MOEX

Скрининг рынка для поиска перспективных акций и подбора под стратегии.
Адаптирован для работы с новым MoexClient на базе moexalgo.

Использование:
    python top_stocks.py                    # Топ-10 акций
    python top_stocks.py --top 20            # Топ-20 акций
    python top_stocks.py --html               # Сохранить HTML отчет
    python top_stocks.py --sector IT          # Фильтр по сектору
    python top_stocks.py --strategy rsi       # Акции для RSI стратегии
"""

import argparse
import sys
import logging
from datetime import datetime
from pathlib import Path

from screener.stock_screener import StockScreener
from utils.logger import setup_logger

# Настройка логирования
logger = setup_logger('top_stocks')

def print_header(title: str):
    """Печатает красивый заголовок."""
    print("\n" + "="*80)
    print(f"📊 {title:^76}")
    print("="*80)

def print_section(title: str):
    """Печатает заголовок секции."""
    print("\n" + "─"*80)
    print(f"📌 {title}")
    print("─"*80)

def print_top_stocks(df, title: str = "ТОП АКЦИЙ ДЛЯ ПОКУПКИ"):
    """
    Красиво печатает таблицу с топ акциями.
    
    Args:
        df: DataFrame с результатами
        title: Заголовок таблицы
    """
    if df.empty:
        print("❌ Нет данных для отображения")
        return
    
    print_header(title)
    
    # Форматируем вывод
    print(f"\n{'#':<3} {'Тикер':<8} {'Компания':<30} {'Цена':<10} {'RSI':<6} {'Тренд':<8} {'Оценка':<8} {'Рекомендация':<20}")
    print("-"*110)
    
    for idx, row in df.iterrows():
        # Эмодзи для тренда
        trend_emoji = "📈" if row['trend'] == 'up' else "📉" if row['trend'] == 'down' else "➡️"
        
        print(f"{idx+1:<3} {row['ticker']:<8} {row['name'][:28]:<30} "
              f"{row['price']:<10.2f} {row['rsi']:<6.1f} "
              f"{trend_emoji} {row['trend']:<5} {row['score']:<8.1f} "
              f"{row['recommendation']:<20}")
    
    print("\n" + "="*80)

def print_strategy_stocks(df, strategy_name: str):
    """
    Печатает акции, подходящие под стратегию.
    
    Args:
        df: DataFrame с результатами
        strategy_name: Название стратегии
    """
    if df.empty:
        print(f"❌ Нет акций для стратегии {strategy_name}")
        return
    
    strategy_titles = {
        'rsi': 'RSI Mean Reversion',
        'sma': 'SMA Crossover',
        'momentum': 'Momentum',
        'value': 'Value Investing'
    }
    
    title = strategy_titles.get(strategy_name, strategy_name.upper())
    print_section(f"АКЦИИ ДЛЯ СТРАТЕГИИ: {title}")
    
    for idx, row in df.iterrows():
        print(f"  {idx+1}. {row['ticker']:<6} - {row['name'][:30]:<30} | "
              f"RSI: {row['rsi']:<5.1f} | Оценка: {row['score']:<5.1f} | "
              f"{row.get('strategy_note', '')}")

def print_trading_pairs(pairs):
    """
    Печатает найденные пары для парного трейдинга.
    
    Args:
        pairs: Список словарей с парами
    """
    if not pairs:
        print("❌ Потенциальных пар не найдено")
        return
    
    print_section("ПОТЕНЦИАЛЬНЫЕ ПАРЫ ДЛЯ ПАРНОГО ТРЕЙДИНГА")
    
    for idx, pair in enumerate(pairs, 1):
        print(f"\n  {idx}. {pair['sector']}")
        print(f"     Пара: {pair['asset1']} / {pair['asset2']}")
        print(f"     Средний спред: {pair['spread']:.2f}")
        print(f"     Текущий Z-score: {pair['current_z']:.2f}")
        print(f"     Сила связи: {pair['strength']}")

def save_html_report(df, filename: str = "stock_report.html"):
    """
    Сохраняет результаты в HTML файл.
    
    Args:
        df: DataFrame с результатами
        filename: Имя файла
    """
    if df.empty:
        logger.warning("Нет данных для сохранения HTML отчета")
        return
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Анализ акций MOEX</title>
        <meta charset="utf-8">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th {{ background-color: #4CAF50; color: white; padding: 10px; }}
            td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
            tr:hover {{ background-color: #f5f5f5; }}
            .buy {{ background-color: #d4edda; }}
            .watch {{ background-color: #fff3cd; }}
            .avoid {{ background-color: #f8d7da; }}
            .footer {{ margin-top: 20px; color: #777; }}
        </style>
    </head>
    <body>
        <h1>📊 Анализ акций MOEX</h1>
        <p>Отчет сгенерирован: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
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
    
    for idx, row in df.iterrows():
        row_class = ""
        if "СИЛЬНАЯ ПОКУПКА" in row['recommendation'] or "ПОКУПКА" in row['recommendation']:
            row_class = "buy"
        elif "ИЗБЕГАТЬ" in row['recommendation']:
            row_class = "avoid"
        else:
            row_class = "watch"
        
        html_content += f"""
            <tr class="{row_class}">
                <td>{idx+1}</td>
                <td><b>{row['ticker']}</b></td>
                <td>{row['name']}</td>
                <td>{row['sector']}</td>
                <td>{row['price']:.2f}</td>
                <td>{row['rsi']:.1f}</td>
                <td>{row['trend']}</td>
                <td>{row['score']:.1f}</td>
                <td>{row['recommendation']}</td>
            </tr>
        """
    
    html_content += """
        </table>
        <div class="footer">
            <p>Сгенерировано с использованием moexalgo</p>
        </div>
    </body>
    </html>
    """
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"HTML отчет сохранен в {filename}")
    print(f"\n📄 HTML отчет сохранен: {filename}")

def main():
    """Основная функция."""
    parser = argparse.ArgumentParser(description='Анализатор акций MOEX')
    parser.add_argument('--top', type=int, default=10, help='Количество акций в топе')
    parser.add_argument('--html', action='store_true', help='Сохранить HTML отчет')
    parser.add_argument('--sector', type=str, help='Фильтр по сектору')
    parser.add_argument('--strategy', type=str, choices=['rsi', 'sma', 'momentum', 'value'],
                       help='Показать акции для стратегии')
    parser.add_argument('--days', type=int, default=30, help='Дней истории для анализа')
    parser.add_argument('--pairs', action='store_true', help='Найти пары для парного трейдинга')
    
    args = parser.parse_args()
    
    try:
        # Инициализируем скринер
        screener = StockScreener(max_workers=5)
        
        if args.pairs:
            # Поиск пар для парного трейдинга
            print_header("ПОИСК ПАР ДЛЯ ПАРНОГО ТРЕЙДИНГА")
            pairs = screener.find_trading_pairs(sector=args.sector, days=args.days)
            print_trading_pairs(pairs)
            
        elif args.strategy:
            # Акции для конкретной стратегии
            print_header(f"АКЦИИ ДЛЯ СТРАТЕГИИ: {args.strategy.upper()}")
            results = screener.screen_by_strategy(args.strategy, days=args.days)
            
            if not results.empty:
                # Фильтруем по сектору, если нужно
                if args.sector:
                    results = results[results['sector'] == args.sector]
                
                print_strategy_stocks(results.head(args.top), args.strategy)
                
                if args.html:
                    filename = f"strategy_{args.strategy}_{datetime.now().strftime('%Y%m%d')}.html"
                    save_html_report(results.head(50), filename)
            else:
                print(f"❌ Не найдено акций для стратегии {args.strategy}")
        
        else:
            # Стандартный топ акций
            results = screener.screen_all_tickers(days=args.days, top_n=args.top)
            
            if not results.empty:
                # Фильтруем по сектору, если нужно
                if args.sector:
                    results = results[results['sector'] == args.sector]
                
                print_top_stocks(results, f"ТОП-{args.top} АКЦИЙ ДЛЯ ПОКУПКИ")
                
                # Дополнительно показываем акции под стратегии
                print_section("АКЦИИ ПОД СТРАТЕГИИ")
                
                for strategy in ['rsi', 'sma', 'momentum']:
                    strategy_results = screener.screen_by_strategy(strategy, days=args.days)
                    if not strategy_results.empty:
                        count = min(3, len(strategy_results))
                        print(f"\n  📈 {strategy.upper()}: ", end="")
                        for i in range(count):
                            ticker = strategy_results.iloc[i]['ticker']
                            score = strategy_results.iloc[i]['score']
                            print(f"{ticker}({score:.1f}) ", end="")
                        print()
                
                # Показываем топ-3 пары
                pairs = screener.find_trading_pairs(days=args.days)[:3]
                if pairs:
                    print(f"\n  🔗 ПАРЫ: ", end="")
                    for pair in pairs:
                        print(f"{pair['asset1']}/{pair['asset2']}({pair['current_z']:.1f}) ", end="")
                    print()
                
                if args.html:
                    filename = f"stock_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                    save_html_report(results, filename)
            else:
                print("❌ Не удалось получить данные для анализа")
        
        print("\n" + "="*80)
        print("✅ Анализ завершен")
        
    except KeyboardInterrupt:
        print("\n⚠️ Анализ прерван пользователем")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        print(f"\n❌ Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()