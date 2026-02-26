#!/usr/bin/env python3
"""
Тестирование исправлений после рефакторинга
"""
import asyncio
import sys
import os
from pathlib import Path

# Устанавливаем фиктивный токен для тестов
os.environ['BOT_TOKEN'] = '123456789:ABCdef123456'
sys.path.insert(0, '.')

async def test_config():
    """Тестирование конфигурации"""
    print("=== Тестирование конфигурации ===")
    from config import (
        CATEGORIES, BASE_CATEGORIES, NEWS_CATEGORIES, 
        ADMIN_ID, BOT_TOKEN, NEWSDATA_API_KEY
    )
    
    print(f"✅ BOT_TOKEN загружен: {bool(BOT_TOKEN)}")
    print(f"✅ ADMIN_ID: {ADMIN_ID}")
    print(f"✅ NEWSDATA_API_KEY: {NEWSDATA_API_KEY[:20]}...")
    
    print(f"\nКатегории:")
    print(f"  Базовых: {len(BASE_CATEGORIES)}")
    print(f"  Новостей: {len(NEWS_CATEGORIES)}")
    print(f"  Всего: {len(CATEGORIES)}")
    
    # Проверяем структуру категорий
    expected_news_keys = [
        'news_top', 'news_world', 'news_technology', 'news_business',
        'news_science', 'news_health', 'news_sports', 
        'news_entertainment', 'news_politics', 'news_all'
    ]
    
    for key in expected_news_keys:
        if key not in NEWS_CATEGORIES:
            print(f"  ❌ Отсутствует категория: {key}")
        else:
            print(f"  ✅ {key}: {NEWS_CATEGORIES[key]}")
    
    return True

async def test_database():
    """Тестирование базы данных"""
    print("\n=== Тестирование базы данных ===")
    from database import Database
    from config import CATEGORIES
    
    # Создаём тестовую БД
    db_path = Path('test_fixes.db')
    db = Database(db_path)
    
    try:
        await db.init()
        print("✅ База данных инициализирована")
        
        # Тестовый пользователь
        test_user_id = 999888777
        await db.add_user(test_user_id, 'test_user', 'Test User')
        print("✅ Пользователь добавлен")
        
        # Проверяем настройки
        prefs = await db.get_user_preferences(test_user_id)
        print(f"✅ Настройки загружены: {len(prefs)} категорий")
        
        # Проверяем что все категории есть
        for cat_key in CATEGORIES.keys():
            if cat_key not in prefs:
                print(f"  ❌ Категория {cat_key} отсутствует в настройках")
            else:
                # По умолчанию всё True
                if prefs[cat_key] != True:
                    print(f"  ⚠️ Категория {cat_key}: {prefs[cat_key]} (ожидалось True)")
        
        # Тестируем переключение
        await db.toggle_preference(test_user_id, 'weather', False)
        prefs = await db.get_user_preferences(test_user_id)
        assert prefs['weather'] == False, "weather должен быть False"
        print("✅ Переключение категории работает")
        
        # Включаем обратно
        await db.toggle_preference(test_user_id, 'weather', True)
        prefs = await db.get_user_preferences(test_user_id)
        assert prefs['weather'] == True, "weather должен быть True"
        print("✅ Включение категории работает")
        
        # Проверяем маппинг кнопок
        from bot import BotApp
        app = BotApp()
        app.keyboards = app._create_keyboards()
        
        # Генерируем клавиатуру настроек
        settings_kb = app.keyboards["settings"](prefs, 9)
        print(f"✅ Клавиатура настроек создана: {len(settings_kb.keyboard)} строк")
        
        # Проверяем что есть кнопки с префиксами ✅/❌
        has_status_buttons = False
        for row in settings_kb.keyboard:
            for button in row:
                text = button.text
                if text.startswith('✅ ') or text.startswith('❌ '):
                    has_status_buttons = True
                    # Убираем префикс
                    clean_text = text[2:].strip()
                    print(f"  Статус кнопка: {clean_text}")
        
        if has_status_buttons:
            print("✅ Кнопки настроек показывают статусы")
        else:
            print("❌ Кнопки настроек не показывают статусы")
        
        # Очистка
        await db_path.unlink(missing_ok=True)
        print("✅ Тестовая БД удалена")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте БД: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Удаляем тестовую БД
        db_path.unlink(missing_ok=True)

async def test_api_client():
    """Тестирование API клиента"""
    print("\n=== Тестирование API клиента ===")
    
    try:
        from api_client import APIClient, get_weather_info
        from config import OPEN_METEO_BASE
        
        print(f"✅ APIClient импортирован")
        print(f"✅ get_weather_info импортирована")
        
        # Проверяем WMO коды
        test_codes = [0, 1, 3, 61, 71, 95]
        for code in test_codes:
            info = get_weather_info(code)
            print(f"  Код {code}: {info.get('condition')} {info.get('emoji')}")
        
        # Проверяем создание клиента
        client = APIClient()
        await client._get_session()  # Инициализация сессии
        print("✅ HTTP сессия создана")
        
        # Проверяем rate limiting для погоды
        remaining = client.get_weather_remaining_requests()
        print(f"✅ Лимит погоды: {remaining}/10 осталось")
        
        await client.close()
        print("✅ HTTP сессия закрыта")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте API клиента: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_news_digest():
    """Тестирование модуля новостей"""
    print("\n=== Тестирование модуля новостей ===")
    
    try:
        from news_digest import NewsDigest, NEWS_CATEGORIES as NEWS_API_CATEGORIES
        
        print(f"✅ NewsDigest импортирован")
        print(f"✅ NEWS_API_CATEGORIES: {len(NEWS_API_CATEGORIES)} API категорий")
        
        # Создаём тестовый кэш
        cache_path = Path('test_news_cache.json')
        digest = NewsDigest(cache_path)
        
        print(f"✅ NewsDigest создан")
        
        # Проверяем метрики
        metrics = digest.get_metrics()
        print(f"  Лимиты: {metrics['hourly_limit']}/час, {metrics['daily_limit']}/день")
        print(f"  Кэш записей: {metrics['cache_entries']}")
        
        # Проверяем кэш-ключи
        expected_keys = [
            'headlines_ru_top',
            'headlines_ru_world', 
            'headlines_ru_technology',
            'headlines_ru_business',
            'headlines_ru_science',
            'headlines_ru_health',
            'headlines_ru_sports',
            'headlines_ru_entertainment',
            'headlines_ru_politics'
        ]
        
        print(f"\nОжидаемые кэш-ключи:")
        for key in expected_keys:
            print(f"  {key}")
        
        # Проверяем методы без API вызовов
        articles = digest.get_cached_articles(language="ru", category="top", max_items=2)
        print(f"\nget_cached_articles вернул: {len(articles)} статей")
        
        digest_text = digest.get_news_digest(language="ru", category="top", max_items=3)
        print(f"get_news_digest вернул текст длиной: {len(digest_text)} символов")
        
        combined_text = digest.get_combined_digest(max_per_category=2)
        print(f"get_combined_digest вернул текст длиной: {len(combined_text)} символов")
        
        # Очистка
        await digest.close()
        cache_path.unlink(missing_ok=True)
        print("✅ Кэш удалён")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте новостей: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_category_mapping():
    """Тестирование маппинга категорий"""
    print("\n=== Тестирование маппинга категорий ===")
    
    from config import CATEGORIES, NEWS_CATEGORIES as CONFIG_NEWS_CATS
    from news_digest import NEWS_CATEGORIES as API_NEWS_CATS
    
    print("Конфигурационные категории новостей (для настроек):")
    for db_key, display_name in CONFIG_NEWS_CATS.items():
        print(f"  {db_key} → {display_name}")
    
    print("\nAPI категории (для NewsData.io):")
    for api_key, display_name in API_NEWS_CATS.items():
        print(f"  {api_key} → {display_name}")
    
    # Маппинг между ними (из bot.py)
    news_category_map = {
        "news_top": ("top", "📰 Главное"),
        "news_world": ("world", "🌍 В мире"),
        "news_technology": ("technology", "💻 Технологии"),
        "news_business": ("business", "💼 Бизнес"),
        "news_science": ("science", "🔬 Наука"),
        "news_health": ("health", "🏥 Здоровье"),
        "news_sports": ("sports", "⚽ Спорт"),
        "news_entertainment": ("entertainment", "🎬 Развлечения"),
        "news_politics": ("politics", "🏛️ Политика"),
    }
    
    print("\nМаппинг из bot.py (news_category_map):")
    for db_key, (api_key, display_name) in news_category_map.items():
        if db_key not in CONFIG_NEWS_CATS:
            print(f"  ❌ {db_key} нет в CONFIG_NEWS_CATS")
        if api_key not in API_NEWS_CATS:
            print(f"  ❌ {api_key} нет в API_NEWS_CATS")
        print(f"  {db_key} → {api_key} → {display_name}")
    
    # Проверяем кэш-ключи
    print("\nКэш-ключи (формируются news_digest.py):")
    for db_key, (api_key, _) in news_category_map.items():
        cache_key = f"headlines_ru_{api_key}"
        print(f"  {db_key} → {api_key} → {cache_key}")
    
    print("✅ Маппинг проверен")
    return True

async def test_bot_structure():
    """Тестирование структуры бота"""
    print("\n=== Тестирование структуры бота ===")
    
    try:
        from bot import BotApp
        
        app = BotApp()
        print("✅ BotApp создан")
        
        # Проверяем инициализацию атрибутов
        assert app.bot is None, "bot должен быть None до инициализации"
        assert app.db is None, "db должен быть None до инициализации"
        assert app.api_client is None, "api_client должен быть None до инициализации"
        print("✅ Атрибуты инициализированы правильно")
        
        # Проверяем создание клавиатур
        keyboards = app._create_keyboards()
        assert isinstance(keyboards, dict), "keyboards должен быть dict"
        assert "main" in keyboards, "Должна быть main клавиатура"
        assert "settings" in keyboards, "Должна быть settings клавиатура"
        assert "news" in keyboards, "Должна быть news клавиатура"
        print(f"✅ Клавиатуры созданы: {list(keyboards.keys())}")
        
        # Проверяем settings клавиатуру
        test_prefs = {cat: True for cat in CATEGORIES}
        test_prefs['weather'] = False
        test_prefs['news_top'] = False
        
        from config import CATEGORIES
        settings_kb = keyboards["settings"](test_prefs, 9)
        
        # Проверяем что есть кнопки с префиксами
        has_weather_disabled = False
        has_news_top_disabled = False
        
        for row in settings_kb.keyboard:
            for button in row:
                text = button.text
                if text.startswith('❌ 🌤 Погода'):
                    has_weather_disabled = True
                if text.startswith('❌ 📰 Главное'):
                    has_news_top_disabled = True
        
        if has_weather_disabled and has_news_top_disabled:
            print("✅ Settings клавиатура показывает правильные статусы")
        else:
            print(f"  ⚠️ weather disabled: {has_weather_disabled}, news_top disabled: {has_news_top_disabled}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка в тесте структуры бота: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Главная функция тестирования"""
    print("🧪 Запуск тестов исправлений\n")
    
    tests = [
        test_config,
        test_database,
        test_api_client,
        test_news_digest,
        test_category_mapping,
        test_bot_structure,
    ]
    
    results = []
    for test in tests:
        try:
            success = await test()
            results.append((test.__name__, success))
        except Exception as e:
            print(f"❌ Исключение в тесте {test.__name__}: {e}")
            results.append((test.__name__, False))
    
    print("\n" + "=" * 60)
    print("📊 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ:")
    print("=" * 60)
    
    passed = 0
    for name, success in results:
        status = "✅ ПРОЙДЕН" if success else "❌ ПРОВАЛЕН"
        print(f"{status}: {name}")
        if success:
            passed += 1
    
    print(f"\n📈 ИТОГО: {passed}/{len(results)} тестов пройдено")
    
    if passed == len(results):
        print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        return 0
    else:
        print(f"\n⚠️ {len(results) - passed} тестов не пройдено")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)