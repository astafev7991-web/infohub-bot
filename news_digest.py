"""
Класс NewsDigest — новости из NewsData.io с агрессивным кэшированием.
Соблюдает лимиты: 200 запросов/день (Free tier).

API: https://newsdata.io/
Документация: https://newsdata.io/docs
"""
import json
import asyncio
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

import aiohttp

from config import NEWSDATA_API_KEY

logger = logging.getLogger(__name__)


# === КОНФИГУРАЦИЯ ===
NEWSDATA_BASE = "https://newsdata.io/api/1"

# Время жизни кэша (секунды)
CACHE_TTL = {
    "headlines_ru": 30 * 60,       # 30 минут
    "headlines_ru_world": 30 * 60, # 30 минут
    "headlines_ru_tech": 30 * 60,  # 30 минут
    "headlines_ru_business": 30 * 60,  # 30 минут
    "headlines_ru_science": 30 * 60,   # 30 минут
}

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)

# Категории новостей NewsData.io
NEWS_CATEGORIES = {
    "world": "🌍 Мир",
    "technology": "💻 Технологии",
    "business": "💼 Бизнес",
    "science": "🔬 Наука",
    "health": "🏥 Здоровье",
    "sports": "⚽ Спорт",
    "entertainment": "🎬 Развлечения",
    "politics": "🏛️ Политика",
    "top": "📰 Главное",
}

# Языки
NEWS_LANGUAGES = {
    "ru": "🇷🇺 Русский",
    "en": "🇺🇸 English",
}


@dataclass
class CacheEntry:
    """Запись в кэше"""
    data: Any
    fetched_at: float
    is_stale: bool = False
    api_calls: int = 0


@dataclass
class APIMetrics:
    """Метрики использования API"""
    total_calls: int = 0
    daily_calls: int = 0
    last_reset_date: str = ""
    
    def reset_if_new_day(self):
        """Сброс дневного счётчика"""
        today = datetime.now().strftime("%Y-%m-%d")
        if self.last_reset_date != today:
            self.daily_calls = 0
            self.last_reset_date = today
            logger.info(f"NewsDigest: Daily counter reset for {today}")


class NewsDigest:
    """
    Новости из NewsData.io с агрессивным кэшированием.
    
    Особенности:
    - Кэш в памяти + JSON-файл (fallback)
    - Лимит 200 запросов/день (Free tier)
    - Поддержка русского языка
    - Graceful degradation при ошибках
    """
    
    def __init__(self, cache_path: Path, api_key: str = None):
        self.cache_path = cache_path
        self.api_key = api_key or NEWSDATA_API_KEY
        self._cache: Dict[str, CacheEntry] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        self._metrics = APIMetrics()
        
        # Загружаем кэш из файла
        self._load_cache_from_file()
    
    # === ИНИЦИАЛИЗАЦИЯ ===
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=REQUEST_TIMEOUT,
                connector=aiohttp.TCPConnector(limit=5)
            )
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("NewsDigest: HTTP session closed")
    
    # === КЭШИРОВАНИЕ ===
    
    def _load_cache_from_file(self):
        """Загрузка кэша из JSON-файла"""
        if not self.cache_path.exists():
            return
        
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            for key, entry in data.get("cache", {}).items():
                if isinstance(entry, dict) and "data" in entry:
                    self._cache[key] = CacheEntry(
                        data=entry["data"],
                        fetched_at=entry.get("fetched_at", 0),
                        is_stale=entry.get("is_stale", False),
                        api_calls=entry.get("api_calls", 0)
                    )
            
            # Восстанавливаем метрики
            if "metrics" in data:
                self._metrics.total_calls = data["metrics"].get("total_calls", 0)
                self._metrics.daily_calls = data["metrics"].get("daily_calls", 0)
                self._metrics.last_reset_date = data["metrics"].get("last_reset_date", "")
            
            logger.info(f"NewsDigest: Loaded {len(self._cache)} cache entries")
            
        except Exception as e:
            logger.warning(f"NewsDigest: Failed to load cache: {e}")
    
    def _save_cache_to_file(self):
        """Сохранение кэша в JSON-файл"""
        try:
            data = {
                "cache": {
                    key: {
                        "data": entry.data,
                        "fetched_at": entry.fetched_at,
                        "is_stale": entry.is_stale,
                        "api_calls": entry.api_calls
                    }
                    for key, entry in self._cache.items()
                },
                "metrics": {
                    "total_calls": self._metrics.total_calls,
                    "daily_calls": self._metrics.daily_calls,
                    "last_reset_date": self._metrics.last_reset_date
                }
            }
            
            temp_path = self.cache_path.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            temp_path.replace(self.cache_path)
            
        except Exception as e:
            logger.error(f"NewsDigest: Failed to save cache: {e}")
    
    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._cache:
            return False
        
        entry = self._cache[key]
        ttl = CACHE_TTL.get(key, 30 * 60)
        age = time.time() - entry.fetched_at
        
        return age < ttl
    
    def _get_cached(self, key: str) -> Optional[CacheEntry]:
        return self._cache.get(key)
    
    def _set_cached(self, key: str, data: Any, is_stale: bool = False):
        entry = CacheEntry(
            data=data,
            fetched_at=time.time(),
            is_stale=is_stale,
            api_calls=self._cache.get(key, CacheEntry(data=None, fetched_at=0)).api_calls + 1
        )
        self._cache[key] = entry
        self._save_cache_to_file()
    
    # === API ЗАПРОСЫ ===
    
    async def _fetch_newsdata(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """
        Запрос к NewsData.io с учётом лимитов.
        Лимит: 200 запросов/день на Free tier.
        """
        self._metrics.reset_if_new_day()
        
        # Проверка дневного лимита (оставляем запас 10 запросов)
        if self._metrics.daily_calls >= 190:
            logger.warning("NewsDigest: Daily API limit approaching (190/200)")
            return None
        
        if not self.api_key:
            logger.warning("NewsDigest: API key not configured")
            return None
        
        try:
            session = await self._get_session()
            url = f"{NEWSDATA_BASE}{endpoint}"
            
            # Добавляем API key
            params = params or {}
            params["apikey"] = self.api_key
            
            self._metrics.total_calls += 1
            self._metrics.daily_calls += 1
            
            logger.info(
                f"NewsDigest: API call #{self._metrics.daily_calls}/200 today → {endpoint}"
            )
            
            async with session.get(url, params=params) as resp:
                if resp.status == 429:
                    logger.warning("NewsDigest: Rate limit hit (429)")
                    return None
                
                if resp.status == 401:
                    logger.error("NewsDigest: Invalid API key (401)")
                    return None
                
                resp.raise_for_status()
                data = await resp.json()
                
                if data.get("status") != "success":
                    logger.warning(f"NewsDigest: API error: {data.get('results', {}).get('message', 'Unknown error')}")
                    return None
                
                return data
                
        except aiohttp.ClientError as e:
            logger.error(f"NewsDigest: HTTP error: {e}")
            return None
        except Exception as e:
            logger.error(f"NewsDigest: Unexpected error: {e}")
            return None
    
    # === МЕТОДЫ ДАННЫХ ===
    
    async def get_latest_news(
        self,
        language: str = "ru",
        category: str = None,
        country: str = None,
        page_size: int = 10
    ) -> Optional[List[Dict]]:
        """
        Получение последних новостей из NewsData.io.
        
        Args:
            language: Код языка (ru, en)
            category: Категория (world, technology, business, etc.)
            country: Код страны (ru, us, gb, etc.)
            page_size: Количество новостей (макс. 10 на Free tier)
        """
        cache_key = f"headlines_{language}"
        if category:
            cache_key = f"headlines_{language}_{category}"
        
        # Возвращаем из кэша если валиден
        if self._is_cache_valid(cache_key):
            entry = self._get_cached(cache_key)
            logger.debug(f"NewsDigest: Returning cached {cache_key}")
            return entry.data
        
        async with self._lock:
            if self._is_cache_valid(cache_key):
                return self._get_cached(cache_key).data
            
            params = {
                "language": language,
            }
            
            if category:
                params["category"] = category
            if country:
                params["country"] = country
            
            data = await self._fetch_newsdata("/latest", params)
            
            if data and data.get("results"):
                articles = self._normalize_articles(data["results"])
                self._set_cached(cache_key, articles, is_stale=False)
                return articles
            
            # Fallback: возвращаем устаревший кэш
            entry = self._get_cached(cache_key)
            if entry and entry.data:
                logger.warning(f"NewsDigest: Returning stale {cache_key}")
                entry.is_stale = True
                return entry.data
            
            return None
    
    def _normalize_articles(self, articles: List[Dict]) -> List[Dict]:
        """Нормализация статей для единообразного формата"""
        normalized = []
        
        for article in articles:
            # Пропускаем статьи без заголовка или URL
            if not article.get("title") or not article.get("link"):
                continue
            
            normalized.append({
                "title": article.get("title", ""),
                "description": article.get("description", "") or article.get("content", ""),
                "url": article.get("link", ""),
                "source": article.get("source_id", "Источник"),
                "author": article.get("creator", [""])[0] if article.get("creator") else "",
                "published_at": article.get("pubDate", ""),
                "image_url": article.get("image_url", ""),
                "category": article.get("category", [""])[0] if article.get("category") else "",
            })
        
        return normalized
    
    # === СВЕЖИЕ ДАННЫЕ ===
    
    async def refresh_all(self) -> Dict[str, bool]:
        """
        Принудительное обновление основных лент.
        Вызывается по расписанию.
        """
        logger.info("NewsDigest: Starting refresh")
        
        results = {}
        
        # Обновляем русские новости по категориям
        tasks = [
            ("ru_top", self.get_latest_news(language="ru", category="top")),
            ("ru_world", self.get_latest_news(language="ru", category="world")),
            ("ru_technology", self.get_latest_news(language="ru", category="technology")),
            ("ru_business", self.get_latest_news(language="ru", category="business")),
            ("ru_science", self.get_latest_news(language="ru", category="science")),
        ]
        
        for name, task in tasks:
            try:
                result = await task
                results[name] = result is not None
            except Exception as e:
                logger.error(f"NewsDigest: Error refreshing {name}: {e}")
                results[name] = False
        
        success = sum(results.values())
        logger.info(f"NewsDigest: Refresh complete ({success}/{len(results)} sources)")
        
        return results
    
    # === ФОРМАТИРОВАНИЕ ===
    
    def get_news_digest(
        self,
        language: str = "ru",
        category: str = "top",
        max_items: int = 5
    ) -> str:
        """
        Формирование текста новостей для Telegram.
        БЕЗ запросов к API — только из кэша!
        
        Args:
            language: Код языка
            category: Категория
            max_items: Максимальное количество новостей
        """
        cache_key = f"headlines_{language}"
        if category:
            cache_key = f"headlines_{language}_{category}"
        
        entry = self._cache.get(cache_key)
        
        if not entry or not entry.data:
            logger.warning(f"NewsDigest: No cached data for {cache_key}, available keys: {list(self._cache.keys())}")
            return "📰 <b>Новости</b>\n\n❌ Данные временно недоступны"
        
        lang_name = NEWS_LANGUAGES.get(language, language.upper())
        category_name = NEWS_CATEGORIES.get(category, category)
        
        lines = [f"📰 <b>Новости</b> • {lang_name} • {category_name}"]
        
        if entry.is_stale:
            lines.append("⚠️ <i>Данные могут быть устаревшими</i>")
        
        lines.append("")
        
        for i, article in enumerate(entry.data[:max_items], 1):
            title = article.get("title", "")
            source = article.get("source", "Источник")
            url = article.get("url", "#")
            
            # Экранируем HTML
            import html as html_module
            title = html_module.escape(title[:100] + "..." if len(title) > 100 else title)
            source = html_module.escape(source)
            
            lines.append(f"{i}. <a href=\"{url}\">{title}</a>")
            lines.append(f"   <i>{source}</i>\n")
        
        # Время обновления
        if entry.fetched_at:
            age = int(time.time() - entry.fetched_at)
            if age < 60:
                age_str = f"{age}с назад"
            elif age < 3600:
                age_str = f"{age // 60}мин назад"
            else:
                age_str = f"{age // 3600}ч назад"
            lines.append(f"🕐 <i>Обновлено: {age_str}</i>")
        
        return "\n".join(lines)
    
    def get_combined_digest(self, max_per_category: int = 3) -> str:
        """
        Комбинированный дайджест из разных категорий.
        БЕЗ запросов к API — только из кэша!
        """
        lines = ["📰 <b>Новости дня</b>\n"]
        
        has_any = False
        
        # Главные новости
        top_entry = self._cache.get("headlines_ru_top")
        if not top_entry or not top_entry.data:
            top_entry = self._cache.get("headlines_ru")
        
        if top_entry and top_entry.data:
            has_any = True
            lines.append("📰 <b>Главное:</b>")
            for article in top_entry.data[:max_per_category]:
                title = article.get("title", "")[:80]
                url = article.get("url", "#")
                import html as html_module
                title = html_module.escape(title)
                lines.append(f" • <a href=\"{url}\">{title}</a>")
            lines.append("")
        
        # Мировые новости
        world_entry = self._cache.get("headlines_ru_world")
        if world_entry and world_entry.data:
            has_any = True
            lines.append("🌍 <b>В мире:</b>")
            for article in world_entry.data[:max_per_category]:
                title = article.get("title", "")[:80]
                url = article.get("url", "#")
                import html as html_module
                title = html_module.escape(title)
                lines.append(f" • <a href=\"{url}\">{title}</a>")
            lines.append("")
        
        # Технологии
        tech_entry = self._cache.get("headlines_ru_technology")
        if tech_entry and tech_entry.data:
            has_any = True
            lines.append("💻 <b>Технологии:</b>")
            for article in tech_entry.data[:max_per_category]:
                title = article.get("title", "")[:80]
                url = article.get("url", "#")
                import html as html_module
                title = html_module.escape(title)
                lines.append(f" • <a href=\"{url}\">{title}</a>")
            lines.append("")
        
        # Бизнес
        biz_entry = self._cache.get("headlines_ru_business")
        if biz_entry and biz_entry.data:
            has_any = True
            lines.append("💼 <b>Бизнес:</b>")
            for article in biz_entry.data[:max_per_category]:
                title = article.get("title", "")[:80]
                url = article.get("url", "#")
                import html as html_module
                title = html_module.escape(title)
                lines.append(f" • <a href=\"{url}\">{title}</a>")
            lines.append("")
        
        if not has_any:
            logger.warning(f"NewsDigest: No cached data for combined digest, available keys: {list(self._cache.keys())}")
            return "📰 <b>Новости</b>\n\n❌ Данные временно недоступны. Попробуйте позже."
        
        # Метрики
        lines.append(f"📊 API: {self._metrics.daily_calls}/200 запросов сегодня")
        
        return "\n".join(lines)
    
    # === МЕТРИКИ ===
    
    def get_metrics(self) -> Dict[str, Any]:
        """Получение метрик использования API"""
        return {
            "total_calls": self._metrics.total_calls,
            "daily_calls": self._metrics.daily_calls,
            "daily_limit": 200,
            "remaining": 200 - self._metrics.daily_calls,
            "cache_entries": len(self._cache),
            "cache_status": {
                key: {
                    "valid": self._is_cache_valid(key),
                    "age_seconds": int(time.time() - entry.fetched_at) if entry else None,
                    "is_stale": entry.is_stale if entry else None,
                    "articles_count": len(entry.data) if entry and entry.data else 0
                }
                for key, entry in self._cache.items()
            }
        }
