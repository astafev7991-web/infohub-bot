"""
NewsDigest — dual API: NewsAPI.org (100/день) + NewsData.io (200/день) = 300 запросов.

Роутинг категорий:
- NewsAPI.org: /everything + q-запрос на русском + sortBy=publishedAt
- NewsData.io: language=ru, поддерживаемые категории: world, politics, business, entertainment, sports, top

Автофоллбэк при превышении лимита одного из API.
"""
import json
import asyncio
import logging
import time
import html as html_module
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

import aiohttp

from config import NEWSAPI_KEY, NEWSDATA_API_KEY

logger = logging.getLogger(__name__)

# === КОНФИГУРАЦИЯ ===
NEWSAPI_BASE = "https://newsapi.org/v2"
NEWSDATA_BASE = "https://newsdata.io/api/1"

NEWSAPI_DAILY_LIMIT = 100
NEWSDATA_DAILY_LIMIT = 200

CACHE_TTL = 60 * 60  # 1 час
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)

NEWS_CATEGORIES = {
    "general":       "📰 Главное",
    "world":         "🌍 В мире",
    "technology":    "💻 Технологии",
    "business":      "💼 Бизнес",
    "science":       "🔬 Наука",
    "health":        "🏥 Здоровье",
    "sports":        "⚽ Спорт",
    "entertainment": "🎬 Развлечения",
    "politics":      "🏛️ Политика",
}

# Роутинг категорий по API
# sports перенесён в NewsData — там есть category=sports на free tier
API_ROUTING = {
    "newsapi":  ["general", "technology", "science", "health"],
    "newsdata": ["world", "politics", "business", "entertainment", "sports"],
}

# NewsAPI /everything — работает на всех тарифах включая free
NEWSAPI_CONFIG = {
    "general":    {"endpoint": "/everything", "params": {"q": "новости Россия",          "language": "ru", "sortBy": "publishedAt"}},
    "technology": {"endpoint": "/everything", "params": {"q": "технологии ит инновации",   "language": "ru", "sortBy": "publishedAt"}},
    "science":    {"endpoint": "/everything", "params": {"q": "наука исследования",       "language": "ru", "sortBy": "publishedAt"}},
    "health":     {"endpoint": "/everything", "params": {"q": "здоровье медицина",        "language": "ru", "sortBy": "publishedAt"}},
}

# Фоллбэк на NewsData для NewsAPI-категорий
NEWSAPI_TO_NEWSDATA_FALLBACK = {
    "general":    {"params": {"language": "ru", "category": "top"}},
    "technology": {"params": {"language": "ru", "category": "top"}},
    "science":    {"params": {"language": "ru", "category": "top"}},
    "health":     {"params": {"language": "ru", "category": "top"}},
}

# NewsData config — поддерживаемые категории на free tier
NEWSDATA_CONFIG = {
    "world":         {"params": {"language": "ru", "category": "world"}},
    "politics":      {"params": {"language": "ru", "category": "politics"}},
    "business":      {"params": {"language": "ru", "category": "business"}},
    "entertainment": {"params": {"language": "ru", "category": "entertainment"}},
    "sports":        {"params": {"language": "ru", "category": "sports"}},
}

REFRESH_CATEGORIES = [
    "general", "technology", "science", "world", "politics",
    "business", "health", "sports", "entertainment"
]


@dataclass
class CacheEntry:
    data: List[Dict]
    fetched_at: float
    source_api: str
    is_stale: bool = False


@dataclass
class APIMetrics:
    total_calls: int = 0
    daily_calls: int = 0
    last_reset_date: str = ""
    limit: int = 100

    def reset_if_new_day(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if self.last_reset_date != today:
            self.daily_calls = 0
            self.last_reset_date = today

    def can_make_request(self) -> bool:
        self.reset_if_new_day()
        return self.daily_calls < self.limit

    def increment(self):
        self.daily_calls += 1
        self.total_calls += 1

    def remaining(self) -> int:
        self.reset_if_new_day()
        return max(0, self.limit - self.daily_calls)


class NewsDigest:
    """
    Dual API: NewsAPI.org (/everything) + NewsData.io = 300 запросов/день.
    """

    def __init__(self, cache_path: Path, newsapi_key: str = None, newsdata_key: str = None):
        self.cache_path = cache_path
        self.newsapi_key = newsapi_key or NEWSAPI_KEY
        self.newsdata_key = newsdata_key or NEWSDATA_API_KEY

        self._cache: Dict[str, CacheEntry] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        self._rate_lock = asyncio.Lock()

        self._newsapi_metrics = APIMetrics(limit=NEWSAPI_DAILY_LIMIT)
        self._newsdata_metrics = APIMetrics(limit=NEWSDATA_DAILY_LIMIT)

        self._load_cache_from_file()

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

    # === КЭШ ===

    def _load_cache_from_file(self):
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
                        source_api=entry.get("source_api", "unknown"),
                        is_stale=entry.get("is_stale", False),
                    )
            if "metrics" in data:
                m = data["metrics"]
                if "newsapi" in m:
                    self._newsapi_metrics.total_calls = m["newsapi"].get("total_calls", 0)
                    self._newsapi_metrics.daily_calls = m["newsapi"].get("daily_calls", 0)
                    self._newsapi_metrics.last_reset_date = m["newsapi"].get("last_reset_date", "")
                if "newsdata" in m:
                    self._newsdata_metrics.total_calls = m["newsdata"].get("total_calls", 0)
                    self._newsdata_metrics.daily_calls = m["newsdata"].get("daily_calls", 0)
                    self._newsdata_metrics.last_reset_date = m["newsdata"].get("last_reset_date", "")
            logger.info(f"NewsDigest: Loaded {len(self._cache)} cache entries")
        except Exception as e:
            logger.warning(f"NewsDigest: Failed to load cache: {e}")

    def _save_cache_to_file(self):
        try:
            data = {
                "cache": {
                    key: {
                        "data": entry.data,
                        "fetched_at": entry.fetched_at,
                        "source_api": entry.source_api,
                        "is_stale": entry.is_stale,
                    }
                    for key, entry in self._cache.items()
                },
                "metrics": {
                    "newsapi": {
                        "total_calls": self._newsapi_metrics.total_calls,
                        "daily_calls": self._newsapi_metrics.daily_calls,
                        "last_reset_date": self._newsapi_metrics.last_reset_date,
                    },
                    "newsdata": {
                        "total_calls": self._newsdata_metrics.total_calls,
                        "daily_calls": self._newsdata_metrics.daily_calls,
                        "last_reset_date": self._newsdata_metrics.last_reset_date,
                    },
                }
            }
            tmp = self.cache_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            tmp.replace(self.cache_path)
        except Exception as e:
            logger.error(f"NewsDigest: Failed to save cache: {e}")

    def _is_cache_valid(self, key: str) -> bool:
        if key not in self._cache:
            return False
        return time.time() - self._cache[key].fetched_at < CACHE_TTL

    def _set_cache(self, key: str, data: List[Dict], source_api: str):
        self._cache[key] = CacheEntry(data=data, fetched_at=time.time(), source_api=source_api)
        self._save_cache_to_file()

    # === API ЗАПРОСЫ ===

    async def _fetch_newsapi(self, endpoint: str, params: Dict) -> Optional[List[Dict]]:
        async with self._rate_lock:
            if not self._newsapi_metrics.can_make_request():
                logger.warning(f"NewsAPI: Daily limit reached ({self._newsapi_metrics.daily_calls}/{NEWSAPI_DAILY_LIMIT})")
                return None
            self._newsapi_metrics.increment()

        if not self.newsapi_key:
            logger.warning("NewsAPI: key not configured")
            return None

        try:
            session = await self._get_session()
            full_params = dict(params)
            full_params["apiKey"] = self.newsapi_key
            full_params.setdefault("pageSize", 10)

            url = f"{NEWSAPI_BASE}{endpoint}"
            async with session.get(url, params=full_params) as resp:
                if resp.status in (426, 429, 401):
                    logger.warning(f"NewsAPI: HTTP {resp.status}")
                    return None
                resp.raise_for_status()
                data = await resp.json()
                if data.get("status") != "ok":
                    logger.warning(
                        f"NewsAPI: status={data.get('status')}, "
                        f"code={data.get('code')}, msg={data.get('message')}"
                    )
                    return None
                articles = data.get("articles", [])
                logger.info(
                    f"NewsAPI: ✓ {endpoint} → {len(articles)} статей "
                    f"({self._newsapi_metrics.daily_calls}/{NEWSAPI_DAILY_LIMIT})"
                )
                normalized = self._normalize_newsapi(articles)
                return normalized if normalized else None
        except Exception as e:
            logger.error(f"NewsAPI error: {e}")
            return None

    async def _fetch_newsdata(self, params: Dict) -> Optional[List[Dict]]:
        async with self._rate_lock:
            if not self._newsdata_metrics.can_make_request():
                logger.warning(f"NewsData: Daily limit reached ({self._newsdata_metrics.daily_calls}/{NEWSDATA_DAILY_LIMIT})")
                return None
            self._newsdata_metrics.increment()

        if not self.newsdata_key:
            logger.warning("NewsData: key not configured")
            return None

        try:
            session = await self._get_session()
            full_params = dict(params)
            full_params["apikey"] = self.newsdata_key

            url = f"{NEWSDATA_BASE}/latest"
            async with session.get(url, params=full_params) as resp:
                if resp.status in (429, 401):
                    logger.warning(f"NewsData: HTTP {resp.status}")
                    return None
                resp.raise_for_status()
                data = await resp.json()
                if data.get("status") != "success":
                    logger.warning(
                        f"NewsData: status={data.get('status')}, "
                        f"msg={data.get('message', {})}"
                    )
                    return None
                articles = data.get("results", [])
                logger.info(
                    f"NewsData: ✓ /latest → {len(articles)} статей "
                    f"({self._newsdata_metrics.daily_calls}/{NEWSDATA_DAILY_LIMIT})"
                )
                normalized = self._normalize_newsdata(articles)
                return normalized if normalized else None
        except Exception as e:
            logger.error(f"NewsData error: {e}")
            return None

    def _normalize_newsapi(self, articles: List[Dict]) -> List[Dict]:
        result = []
        for a in articles:
            title = a.get("title") or ""
            url = a.get("url") or ""
            if not title or not url or title == "[Removed]":
                continue
            source = ""
            if isinstance(a.get("source"), dict):
                source = a["source"].get("name", "")
            result.append({
                "title":        title,
                "description":  a.get("description") or "",
                "url":          url,
                "source":       source,
                "published_at": a.get("publishedAt") or "",
                "image_url":    a.get("urlToImage") or "",
            })
        return result

    def _normalize_newsdata(self, articles: List[Dict]) -> List[Dict]:
        result = []
        for a in articles:
            title = a.get("title") or ""
            url = a.get("link") or ""
            if not title or not url:
                continue
            result.append({
                "title":        title,
                "description":  a.get("description") or a.get("content") or "",
                "url":          url,
                "source":       a.get("source_id", "NewsData"),
                "published_at": a.get("pubDate") or "",
                "image_url":    a.get("image_url") or "",
            })
        return result

    # === ПОЛУЧЕНИЕ НОВОСТЕЙ ===

    async def get_latest_news(self, category: str = "general") -> Optional[List[Dict]]:
        cache_key = f"news_{category}"

        if self._is_cache_valid(cache_key):
            return self._cache[cache_key].data

        async with self._lock:
            if self._is_cache_valid(cache_key):
                return self._cache[cache_key].data

            if category in API_ROUTING["newsapi"]:
                cfg = NEWSAPI_CONFIG.get(category)
                if cfg:
                    articles = await self._fetch_newsapi(cfg["endpoint"], cfg["params"])
                    if articles:
                        self._set_cache(cache_key, articles, "newsapi")
                        return articles
                    # Фоллбэк на NewsData (category=top)
                    logger.info(f"NewsAPI: 0 статей для '{category}', NewsData fallback...")
                    fb = NEWSAPI_TO_NEWSDATA_FALLBACK.get(category)
                    if fb and self._newsdata_metrics.can_make_request():
                        articles = await self._fetch_newsdata(fb["params"])
                        if articles:
                            self._set_cache(cache_key, articles, "newsdata_fallback")
                            return articles

            elif category in API_ROUTING["newsdata"]:
                cfg = NEWSDATA_CONFIG.get(category)
                if cfg:
                    articles = await self._fetch_newsdata(cfg["params"])
                    if articles:
                        self._set_cache(cache_key, articles, "newsdata")
                        return articles

            # Сталь кэш как последний резерв
            entry = self._cache.get(cache_key)
            if entry and entry.data:
                logger.warning(f"NewsDigest: Returning stale cache for {category}")
                entry.is_stale = True
                return entry.data

            return None

    async def refresh_all(self) -> Dict[str, bool]:
        newsapi_rem = self._newsapi_metrics.remaining()
        newsdata_rem = self._newsdata_metrics.remaining()
        logger.info(
            f"NewsDigest: refresh_all — NewsAPI: {newsapi_rem}/{NEWSAPI_DAILY_LIMIT}, "
            f"NewsData: {newsdata_rem}/{NEWSDATA_DAILY_LIMIT}"
        )

        if newsapi_rem < 1 and newsdata_rem < 1:
            logger.warning("NewsDigest: Both APIs exhausted")
            return {"skipped": True}

        cats_to_fetch = []
        for cat in REFRESH_CATEGORIES:
            if cat in API_ROUTING["newsapi"] and newsapi_rem > 0:
                cats_to_fetch.append(cat)
                newsapi_rem -= 1
            elif cat in API_ROUTING["newsdata"] and newsdata_rem > 0:
                cats_to_fetch.append(cat)
                newsdata_rem -= 1

        if not cats_to_fetch:
            logger.warning("NewsDigest: No categories available within limits")
            return {"skipped": True}

        coroutines = [self.get_latest_news(cat) for cat in cats_to_fetch]
        raw = await asyncio.gather(*coroutines, return_exceptions=True)

        results = {}
        for cat, res in zip(cats_to_fetch, raw):
            if isinstance(res, Exception):
                logger.error(f"NewsDigest: Error {cat}: {res}")
                results[cat] = False
            else:
                results[cat] = res is not None

        ok = sum(1 for v in results.values() if v is True)
        logger.info(f"NewsDigest: refresh_all done ({ok}/{len(cats_to_fetch)} ok)")
        return results

    # === ПУБЛИЧНЫЕ МЕТОДЫ ===

    def get_cached_articles(self, language: str = "ru", category: str = "general", max_items: int = 5) -> List[Dict]:
        alias = {"top": "general"}
        cat = alias.get(category, category)
        key = f"news_{cat}"
        entry = self._cache.get(key)
        if not entry or not entry.data:
            return []
        return entry.data[:max_items]

    def get_news_digest(self, language: str = "ru", category: str = "general", max_items: int = 5) -> str:
        alias = {"top": "general"}
        cat = alias.get(category, category)
        key = f"news_{cat}"
        entry = self._cache.get(key)

        if not entry or not entry.data:
            return (
                f"📰 <b>{NEWS_CATEGORIES.get(cat, cat)}</b>\n\n"
                "❌ Данные временно недоступны. Попробуйте позже."
            )

        return self._format_digest(entry, cat, max_items)

    def get_combined_digest(self, max_per_category: int = 3) -> str:
        lines = ["📰 <b>Новости дня</b>\n"]
        has_any = False

        priority = ["general", "world", "technology", "science"]
        for cat in priority:
            entry = self._cache.get(f"news_{cat}")
            if entry and entry.data:
                has_any = True
                label = NEWS_CATEGORIES.get(cat, cat)
                lines.append(f"{label}:")
                for a in entry.data[:max_per_category]:
                    lines.extend(self._render_article(a, max_len=80))
                lines.append("")

        if not has_any:
            return "📰 <b>Новости</b>\n\n❌ Данные временно недоступны. Попробуйте позже."

        return "\n".join(lines)

    @staticmethod
    def _render_article(article: Dict, max_len: int = 100, numbered: bool = False, index: int = 0) -> List[str]:
        title_raw = article.get("title", "")
        title = html_module.escape(title_raw[:max_len] + "..." if len(title_raw) > max_len else title_raw)
        url = article.get("url", "#")
        source = html_module.escape(article.get("source", ""))
        if numbered:
            line = f"{index}. <a href=\"{url}\">{title}</a>"
            return [line, f"   <i>{source}</i>\n"] if source else [line, ""]
        return [f" • <a href=\"{url}\">{title}</a>"]

    def _format_digest(self, entry: CacheEntry, category: str, max_items: int = 5) -> str:
        label = NEWS_CATEGORIES.get(category, category)
        lines = [f"📰 <b>Новости</b> • {label}"]

        if entry.is_stale:
            lines.append("⚠️ <i>Данные могут быть устаревшими</i>")
        lines.append("")

        for i, a in enumerate(entry.data[:max_items], 1):
            lines.extend(self._render_article(a, max_len=100, numbered=True, index=i))

        return "\n".join(lines)

    def get_metrics(self) -> Dict[str, Any]:
        cache_status = {
            key: {
                "valid": self._is_cache_valid(key),
                "age_min": round((time.time() - e.fetched_at) / 60, 1),
                "source_api": e.source_api,
                "is_stale": e.is_stale,
                "count": len(e.data) if e.data else 0,
            }
            for key, e in self._cache.items()
        }
        return {
            "newsapi": {
                "total_calls": self._newsapi_metrics.total_calls,
                "daily_calls": self._newsapi_metrics.daily_calls,
                "daily_limit": NEWSAPI_DAILY_LIMIT,
                "daily_remaining": self._newsapi_metrics.remaining(),
            },
            "newsdata": {
                "total_calls": self._newsdata_metrics.total_calls,
                "daily_calls": self._newsdata_metrics.daily_calls,
                "daily_limit": NEWSDATA_DAILY_LIMIT,
                "daily_remaining": self._newsdata_metrics.remaining(),
            },
            "cache_entries": len(self._cache),
            "cache_status": cache_status,
            "hourly_remaining": self._newsapi_metrics.remaining() + self._newsdata_metrics.remaining(),
            "hourly_limit": NEWSAPI_DAILY_LIMIT + NEWSDATA_DAILY_LIMIT,
            "daily_remaining": self._newsapi_metrics.remaining() + self._newsdata_metrics.remaining(),
            "daily_limit": NEWSAPI_DAILY_LIMIT + NEWSDATA_DAILY_LIMIT,
            "total_calls": self._newsapi_metrics.total_calls + self._newsdata_metrics.total_calls,
        }