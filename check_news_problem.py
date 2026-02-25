#!/usr/bin/env python3
"""
Анализ проблемы обновления новостей
"""
import sys
import time
import json
from pathlib import Path

sys.path.insert(0, '.')

def main():
    print("=== АНАЛИЗ ПРОБЛЕМЫ НОВОСТЕЙ ===")
    print()
    
    # 1. Проверяем кэш
    cache_path = Path('news_cache.json')
    if not cache_path.exists():
        print("❌ Файл кэша не существует")
        return
    
    with open(cache_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    cache = data.get('cache', {})
    metrics = data.get('metrics', {})
    
    print("📊 МЕТРИКИ:")
    print(f"  Запросов в час: {metrics.get('hourly_calls', 0)}/{20}")
    print(f"  Запросов в день: {metrics.get('daily_calls', 0)}/{200}")
    print()
    
    # 2. Анализ категорий
    current_time = time.time()
    print("📰 КАТЕГОРИИ В КЭШЕ:")
    
    ru_keys = [k for k in cache.keys() if k.startswith('headlines_ru')]
    if not ru_keys:
        print("  ❌ Нет русскоязычных категорий")
        return
    
    all_categories = ['top', 'world', 'technology', 'business', 'science', 'health', 'sports', 'entertainment', 'politics']
    found = []
    missing = []
    
    for cat in all_categories:
        key = f'headlines_ru_{cat}'
        if key in cache:
            entry = cache[key]
            fetched = entry.get('fetched_at', 0)
            age_hours = (current_time - fetched) / 3600
            articles = len(entry.get('data', []))
            stale = '⚠️ (устаревшие)' if entry.get('is_stale', False) else ''
            
            print(f"  ✅ {cat:15} {articles:2} статей, {age_hours:.1f}ч назад {stale}")
            found.append(cat)
        else:
            print(f"  ❌ {cat:15} отсутствует")
            missing.append(cat)
    
    print()
    print("🔍 ПРОБЛЕМЫ:")
    print(f"  1. Отсутствуют категории: {', '.join(missing)}")
    print(f"  2. Устарели категории: technology (20ч), health (20ч), sports (20ч), politics (20ч)")
    print()
    
    # 3. Проверяем API для отсутствующих категорий
    print("🌐 ПРОВЕРКА API NEWSData.io:")
    api_key = 'pub_4d218b0e2165446c8995391fbca82859'
    
    import urllib.request
    
    for cat in missing:
        url = f'https://newsdata.io/api/1/latest?apikey={api_key}&language=ru&category={cat}'
        try:
            req = urllib.request.urlopen(url, timeout=15)
            data = json.loads(req.read().decode())
            status = data.get('status', 'unknown')
            count = len(data.get('results', []))
            print(f"  {cat:15} → статус: {status}, статей: {count}")
        except Exception as e:
            print(f"  {cat:15} → ошибка: {str(e)[:50]}...")
    
    print()
    print("💡 РЕКОМЕНДАЦИИ:")
    print("  1. Увеличить TTL кэша до 2-3 часов (сейчас 1 час)")
    print("  2. Добавить повторные попытки при ошибках API")
    print("  3. Использовать fallback категории при отсутствии данных")
    print("  4. Проверить логи планировщика - почему обновление раз в час не работает?")
    print()
    print("🔧 Проверить планировщик в bot.py:")
    print("   - scheduler.add_job(..., hours=1, ...)")
    print("   - refresh_all() пропускает если <5 запросов в час")
    print("   - Метрики могут не сбрасываться каждый час")

if __name__ == '__main__':
    main()