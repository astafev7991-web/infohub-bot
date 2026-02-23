"""
Класс MarketDigest — сводка крипторынка с агрессивным кэшированием.
Соблюдает лимиты CoinGecko: 30 запросов/мин (Free tier).

API:
- CoinGecko: /search/trending (тренды), /global (доминирование, кап)
- Alternative.me: /fng/ (Fear & Greed Index)
"""
import json
import asyncio
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

import aiohttp

logger = logging.getLogger(__name__)


# === КОНФИГУРАЦИЯ ===
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
FNG_API_URL = "https://api.alternative.me/fng/"

# Время жизни кэша (секунды)
CACHE_TTL = {
    "trending": 5 * 60,      # 5 минут
    "global": 10 * 60,       # 10 минут
    "fng": 60 * 60,          # 60 минут
}

# Таймауты запросов
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)


@dataclass
class CacheEntry:
    """Запись в кэше"""
    data: Any
    fetched_at: float  # timestamp
    is_stale: bool = False  # устаревшие данные (fallback)
    api_calls: int = 0  # сколько раз дёргали API


@dataclass
class APIMetrics:
    """Метрики использования API"""
    coingecko_calls: int = 0
    fng_calls: int = 0
    last_reset: float = field(default_factory=time.time)
    
    def reset_if_needed(self):
        """Сброс счётчиков каждую минуту"""
        if time.time() - self.last_reset > 60:
            self.coingecko_calls = 0
            self.fng_calls = 0
            self.last_reset = time.time()


class MarketDigest:
    """
    Сводка крипторынка с агрессивным кэшированием.
    
    Особенности:
    - Кэш в памяти + JSON-файл (fallback)
    - Один запрос к API → раздача всем юзерам
    - Логирование реальных вызовов API
    - Graceful degradation при ошибках
    """
    
    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self._cache: Dict[str, CacheEntry] = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()
        self._metrics = APIMetrics()
        
        # Загружаем кэш из файла при инициализации
        self._load_cache_from_file()

    # === ИНИЦИАЛИЗАЦИЯ ===
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получение HTTP-сессии"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=REQUEST_TIMEOUT,
                connector=aiohttp.TCPConnector(limit=5)
            )
        return self._session

    async def close(self):
        """Закрытие сессии"""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("MarketDigest: HTTP session closed")

    # === КЭШИРОВАНИЕ ===
    
    def _load_cache_from_file(self):
        """Загрузка кэша из JSON-файла"""
        if not self.cache_path.exists():
            logger.debug("MarketDigest: No cache file, starting fresh")
            return
        
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            for key, entry in data.items():
                if isinstance(entry, dict) and "data" in entry:
                    self._cache[key] = CacheEntry(
                        data=entry["data"],
                        fetched_at=entry.get("fetched_at", 0),
                        is_stale=entry.get("is_stale", False),
                        api_calls=entry.get("api_calls", 0)
                    )
            
            logger.info(f"MarketDigest: Loaded {len(self._cache)} cache entries from file")
            
        except Exception as e:
            logger.warning(f"MarketDigest: Failed to load cache: {e}")

    def _save_cache_to_file(self):
        """Сохранение кэша в JSON-файл"""
        try:
            data = {}
            for key, entry in self._cache.items():
                data[key] = {
                    "data": entry.data,
                    "fetched_at": entry.fetched_at,
                    "is_stale": entry.is_stale,
                    "api_calls": entry.api_calls
                }
            
            temp_path = self.cache_path.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            temp_path.replace(self.cache_path)
            
        except Exception as e:
            logger.error(f"MarketDigest: Failed to save cache: {e}")

    def _is_cache_valid(self, key: str) -> bool:
        """Проверка валидности кэша"""
        if key not in self._cache:
            return False
        
        entry = self._cache[key]
        ttl = CACHE_TTL.get(key, 300)
        age = time.time() - entry.fetched_at
        
        return age < ttl

    def _get_cached(self, key: str) -> Optional[CacheEntry]:
        """Получение данных из кэша"""
        return self._cache.get(key)

    def _set_cached(self, key: str, data: Any, is_stale: bool = False):
        """Сохранение данных в кэш"""
        entry = CacheEntry(
            data=data,
            fetched_at=time.time(),
            is_stale=is_stale,
            api_calls=self._cache.get(key, CacheEntry(data=None, fetched_at=0)).api_calls + 1
        )
        self._cache[key] = entry
        self._save_cache_to_file()

    # === API ЗАПРОСЫ ===
    
    async def _fetch_coingecko(self, endpoint: str) -> Optional[Dict]:
        """
        Запрос к CoinGecko API с учётом лимитов.
        Логирует каждый реальный вызов.
        """
        self._metrics.reset_if_needed()
        
        # Проверка лимитов (30 запросов/мин)
        if self._metrics.coingecko_calls >= 28:  # запас 2 запроса
            logger.warning("MarketDigest: CoinGecko rate limit approaching, skipping")
            return None
        
        try:
            session = await self._get_session()
            url = f"{COINGECKO_BASE}{endpoint}"
            
            self._metrics.coingecko_calls += 1
            logger.info(f"MarketDigest: CoinGecko API call #{self._metrics.coingecko_calls} → {endpoint}")
            
            async with session.get(url) as resp:
                if resp.status == 429:
                    logger.warning("MarketDigest: CoinGecko rate limit hit (429)")
                    return None
                resp.raise_for_status()
                return await resp.json()
                
        except aiohttp.ClientError as e:
            logger.error(f"MarketDigest: CoinGecko error ({endpoint}): {e}")
            return None
        except Exception as e:
            logger.error(f"MarketDigest: Unexpected error ({endpoint}): {e}")
            return None

    async def _fetch_fng(self) -> Optional[Dict]:
        """
        Запрос к Alternative.me Fear & Greed API.
        Это НЕ CoinGecko, отдельный лимит.
        """
        self._metrics.reset_if_needed()
        
        try:
            session = await self._get_session()
            
            self._metrics.fng_calls += 1
            logger.info(f"MarketDigest: FNG API call #{self._metrics.fng_calls}")
            
            async with session.get(FNG_API_URL) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return data.get("data", [{}])[0] if data else None
                
        except aiohttp.ClientError as e:
            logger.error(f"MarketDigest: FNG API error: {e}")
            return None
        except Exception as e:
            logger.error(f"MarketDigest: FNG unexpected error: {e}")
            return None

    # === МЕТОДЫ ДАННЫХ ===
    
    async def get_trending(self) -> Optional[Dict]:
        """
        Получение топ-7 трендовых монет.
        Кэш: 5 минут.
        """
        key = "trending"
        
        # Возвращаем из кэша если валиден
        if self._is_cache_valid(key):
            entry = self._get_cached(key)
            logger.debug(f"MarketDigest: Returning cached trending (age: {time.time() - entry.fetched_at:.0f}s)")
            return entry.data
        
        # Пробуем обновить
        async with self._lock:
            # Double-check после блокировки
            if self._is_cache_valid(key):
                return self._get_cached(key).data
            
            data = await self._fetch_coingecko("/search/trending")
            
            if data:
                self._set_cached(key, data, is_stale=False)
                return data
            
            # Fallback: возвращаем устаревший кэш
            entry = self._get_cached(key)
            if entry:
                logger.warning("MarketDigest: Returning stale trending data (API failed)")
                entry.is_stale = True
                return entry.data
            
            return None

    async def get_global(self) -> Optional[Dict]:
        """
        Получение глобальных данных (капитализация, доминирование).
        Кэш: 10 минут.
        """
        key = "global"
        
        if self._is_cache_valid(key):
            entry = self._get_cached(key)
            logger.debug(f"MarketDigest: Returning cached global (age: {time.time() - entry.fetched_at:.0f}s)")
            return entry.data
        
        async with self._lock:
            if self._is_cache_valid(key):
                return self._get_cached(key).data
            
            data = await self._fetch_coingecko("/global")
            
            if data:
                self._set_cached(key, data.get("data", data), is_stale=False)
                return self._cache[key].data
            
            entry = self._get_cached(key)
            if entry:
                logger.warning("MarketDigest: Returning stale global data (API failed)")
                entry.is_stale = True
                return entry.data
            
            return None

    async def get_fng(self) -> Optional[Dict]:
        """
        Получение индекса Fear & Greed.
        Кэш: 60 минут.
        """
        key = "fng"
        
        if self._is_cache_valid(key):
            entry = self._get_cached(key)
            logger.debug(f"MarketDigest: Returning cached FNG (age: {time.time() - entry.fetched_at:.0f}s)")
            return entry.data
        
        async with self._lock:
            if self._is_cache_valid(key):
                return self._get_cached(key).data
            
            data = await self._fetch_fng()
            
            if data:
                self._set_cached(key, data, is_stale=False)
                return data
            
            entry = self._get_cached(key)
            if entry:
                logger.warning("MarketDigest: Returning stale FNG data (API failed)")
                entry.is_stale = True
                return entry.data
            
            return None

    # === СВЕЖИЕ ДАННЫЕ (для фонового обновления) ===
    
    async def refresh_all(self) -> Dict[str, bool]:
        """
        Принудительное обновление всех данных.
        Вызывается по расписанию (каждые 5 минут).
        
        Returns:
            Dict[str, bool]: Статус обновления каждого источника
        """
        logger.info("MarketDigest: Starting full refresh")
        
        results = {}
        
        # Запускаем параллельно (но с учётом лимитов)
        trending_task = asyncio.create_task(self.get_trending())
        global_task = asyncio.create_task(self.get_global())
        fng_task = asyncio.create_task(self.get_fng())
        
        trending, global_data, fng = await asyncio.gather(
            trending_task, global_task, fng_task, return_exceptions=True
        )
        
        results["trending"] = trending is not None and not isinstance(trending, Exception)
        results["global"] = global_data is not None and not isinstance(global_data, Exception)
        results["fng"] = fng is not None and not isinstance(fng, Exception)
        
        success = sum(results.values())
        logger.info(f"MarketDigest: Refresh complete ({success}/3 sources)")
        
        return results

    # === ФОРМАТИРОВАНИЕ ДАЙДЖЕСТА ===
    
    def get_digest(self) -> str:
        """
        Формирование текста дайджеста для Telegram.
        ВНИМАНИЕ: Не делает запросов к API — только из кэша!
        
        Returns:
            str: Готовый текст для отправки в Telegram
        """
        lines = ["🌍 <b>Крипто-дайджест</b>"]
        
        # Проверяем наличие устаревших данных
        has_stale = any(
            self._cache.get(key, CacheEntry(data=None, fetched_at=0)).is_stale 
            for key in ["trending", "global", "fng"]
        )
        
        if has_stale:
            lines.append("⚠️ <i>Некоторые данные могут быть устаревшими</i>\n")
        else:
            lines.append("")
        
        # === ГЛОБАЛЬНЫЕ ДАННЫЕ ===
        global_entry = self._cache.get("global")
        if global_entry and global_entry.data:
            data = global_entry.data
            
            # Капитализация
            total_cap = data.get("total_market_cap", {}).get("usd")
            cap_change = data.get("market_cap_change_percentage_24h_usd")
            
            if total_cap:
                cap_str = self._format_large_number(total_cap)
                change_str = f" ({cap_change:+.1f}%)" if cap_change else ""
                lines.append(f"💰 <b>Капитализация:</b> ${cap_str}{change_str}")
            
            # Доминирование
            btc_dom = data.get("market_cap_percentage", {}).get("btc")
            eth_dom = data.get("market_cap_percentage", {}).get("eth")
            
            if btc_dom and eth_dom:
                lines.append(f"₿ <b>BTC.D:</b> {btc_dom:.1f}%  ⬡ <b>ETH.D:</b> {eth_dom:.1f}%")
            
            lines.append("")
        
        # === FEAR & GREED ===
        fng_entry = self._cache.get("fng")
        if fng_entry and fng_entry.data:
            fng_data = fng_entry.data
            value = fng_data.get("value")
            classification = fng_data.get("value_classification", "N/A")
            
            if value:
                emoji = self._get_fng_emoji(int(value))
                lines.append(f"😨 <b>Fear & Greed:</b> {value} ({emoji} {classification})")
                lines.append("")
        
        # === ТРЕНДОВЫЕ МОНЕТЫ ===
        trending_entry = self._cache.get("trending")
        if trending_entry and trending_entry.data:
            coins = trending_entry.data.get("coins", [])
            
            if coins:
                trending_names = []
                for coin in coins[:7]:
                    item = coin.get("item", {})
                    symbol = item.get("symbol", "")
                    if symbol:
                        trending_names.append(symbol)
                
                if trending_names:
                    lines.append(f"🔥 <b>В тренде:</b> {', '.join(trending_names)}")
        
        # === ВРЕМЯ ОБНОВЛЕНИЯ ===
        newest = max(
            (entry.fetched_at for entry in self._cache.values() if entry),
            default=0
        )
        if newest:
            age = int(time.time() - newest)
            if age < 60:
                age_str = f"{age}с назад"
            elif age < 3600:
                age_str = f"{age // 60}мин назад"
            else:
                age_str = f"{age // 3600}ч назад"
            lines.append(f"\n🕐 <i>Обновлено: {age_str}</i>")
        
        return "\n".join(lines)

    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===
    
    @staticmethod
    def _format_large_number(value: float) -> str:
        """Форматирование больших чисел (1.2T, 856B, etc.)"""
        if value >= 1_000_000_000_000:
            return f"{value / 1_000_000_000_000:.1f}T"
        elif value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.1f}B"
        elif value >= 1_000_000:
            return f"{value / 1_000_000:.1f}M"
        else:
            return f"{value:,.0f}"

    @staticmethod
    def _get_fng_emoji(value: int) -> str:
        """Эмодзи для Fear & Greed Index"""
        if value <= 20:
            return "😱"
        elif value <= 40:
            return "😰"
        elif value <= 60:
            return "😐"
        elif value <= 80:
            return "😊"
        else:
            return "🤑"

    def get_metrics(self) -> Dict[str, Any]:
        """Получение метрик использования API"""
        return {
            "coingecko_calls_last_minute": self._metrics.coingecko_calls,
            "fng_calls_last_minute": self._metrics.fng_calls,
            "cache_entries": len(self._cache),
            "cache_status": {
                key: {
                    "valid": self._is_cache_valid(key),
                    "age_seconds": int(time.time() - entry.fetched_at) if entry else None,
                    "is_stale": entry.is_stale if entry else None
                }
                for key, entry in self._cache.items()
            }
        }
