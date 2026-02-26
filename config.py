"""
Конфигурация проекта ИнфоХаб
"""
import os
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def get_required_env(key: str, default: str = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        logger.error(f"❌ Переменная '{key}' не задана!")
        raise ValueError(f"Missing required environment variable: {key}")
    return value.strip()


def get_optional_env(key: str, default: str = "", var_type: type = str) -> Any:
    value = os.getenv(key, default)
    if var_type == bool:
        return value.lower() in ("true", "1", "yes", "on")
    try:
        return var_type(value) if value else var_type(default)
    except (ValueError, TypeError):
        logger.warning(f"Invalid value for {key}: '{value}', using default: {default}")
        return var_type(default)


def validate_hour(value: int, name: str) -> int:
    if not 0 <= value <= 23:
        raise ValueError(f"{name} must be between 0 and 23")
    return value


def validate_minute(value: int, name: str) -> int:
    if not 0 <= value <= 59:
        raise ValueError(f"{name} must be between 0 and 59")
    return value


# === BOT TOKEN ===
try:
    BOT_TOKEN = get_required_env("BOT_TOKEN")
    if not BOT_TOKEN or ":" not in BOT_TOKEN:
        raise ValueError("Invalid BOT_TOKEN format")
    logger.info("✅ BOT_TOKEN загружен")
except ValueError as e:
    logger.critical(f"🛑 {e}")
    BOT_TOKEN = ""

# === LOGGING ===
LOG_LEVEL = get_optional_env("LOG_LEVEL", "INFO", str).upper()
if LOG_LEVEL not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
    LOG_LEVEL = "INFO"

# === SCHEDULER ===
UPDATE_INTERVAL = get_optional_env("UPDATE_INTERVAL", "1800", int)
if UPDATE_INTERVAL < 60:
    UPDATE_INTERVAL = 60

DAILY_BROADCAST_HOUR = validate_hour(
    get_optional_env("DAILY_BROADCAST_HOUR", "9", int), "DAILY_BROADCAST_HOUR"
)
DAILY_BROADCAST_MINUTE = validate_minute(
    get_optional_env("DAILY_BROADCAST_MINUTE", "0", int), "DAILY_BROADCAST_MINUTE"
)
ENABLE_BACKGROUND_REFRESH = get_optional_env("ENABLE_BACKGROUND_REFRESH", "true", bool)

# === ADMIN ===
ADMIN_ID = get_optional_env("ADMIN_ID", "0", int)
if ADMIN_ID:
    logger.info(f"✅ ADMIN_ID: {ADMIN_ID}")
else:
    logger.warning("⚠️ ADMIN_ID не задан — /api недоступна")

# === API KEYS ===
NEWSAPI_KEY = get_optional_env("NEWSAPI_KEY", "", str)
if NEWSAPI_KEY:
    logger.info("✅ NEWSAPI_KEY загружен")
else:
    logger.warning("⚠️ NEWSAPI_KEY не задан — часть новостей недоступна")

NEWSDATA_API_KEY = get_optional_env("NEWSDATA_API_KEY", "", str)
if NEWSDATA_API_KEY:
    logger.info("✅ NEWSDATA_API_KEY загружен")
else:
    logger.warning("⚠️ NEWSDATA_API_KEY не задан — часть новостей недоступна")

if not NEWSAPI_KEY and not NEWSDATA_API_KEY:
    logger.error("🛑 Оба ключа новостей не заданы — новости полностью недоступны")

# === PATHS ===
PROJECT_ROOT = Path(__file__).parent.resolve()
DB_PATH = PROJECT_ROOT / "users.db"
CACHE_PATH = PROJECT_ROOT / "cache.json"
CACHE_TTL_SECONDS = UPDATE_INTERVAL
MARKET_CACHE_PATH = PROJECT_ROOT / "market_cache.json"
NEWS_CACHE_PATH = PROJECT_ROOT / "news_cache.json"

# === API ENDPOINTS ===
OPEN_METEO_BASE = "https://api.open-meteo.com/v1"
DEFAULT_LAT, DEFAULT_LON = 55.7558, 37.6173
COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_PARAMS = {
    "ids": "bitcoin,ethereum,tether",
    "vs_currencies": "usd,rub",
    "include_24hr_change": "true"
}
EXCHANGE_RATE_URL = "https://api.exchangerate-api.com/v4/latest/RUB"

# === UI TEXTS ===
PREMIUM_PROMO_TEXT = "🔥 Хочешь новости чаще и без рекламы? Скоро Premium!"
DONATE_BUTTON_URL = get_optional_env("DONATE_BUTTON_URL", "", str)
if not DONATE_BUTTON_URL:
    logger.warning("⚠️ DONATE_BUTTON_URL не задан")

# === CATEGORIES ===
BASE_CATEGORIES = {
    "weather": "🌤 Погода",
    "crypto": "💰 Криптовалюты",
    "fiat": "💱 Курсы валют",
}

NEWS_CATEGORIES = {
    "news_top":           "📰 Главное",
    "news_world":         "🌍 В мире",
    "news_technology":    "💻 Технологии",
    "news_business":      "💼 Бизнес",
    "news_science":       "🔬 Наука",
    "news_health":        "🏥 Здоровье",
    "news_sports":        "⚽ Спорт",
    "news_entertainment": "🎬 Развлечения",
    "news_politics":      "🏛️ Политика",
    "news_all":           "📊 Все новости",
}

CATEGORIES = {**BASE_CATEGORIES, **NEWS_CATEGORIES}

# === CITIES ===
CITY_COORDINATES = {
    "москва":           (55.7558, 37.6173),
    "санкт-петербург":  (59.9343, 30.3351),
    "новосибирск":      (55.0084, 82.9357),
    "екатеринбург":     (56.8389, 60.6057),
    "казань":           (55.7887, 49.1221),
    "нижний новгород":  (56.3269, 44.0059),
    "красноярск":       (56.0153, 92.8932),
    "челябинск":        (55.1644, 61.4368),
    "самара":           (53.1955, 50.1018),
    "уфа":              (54.7388, 55.9721),
    "ростов-на-дону":   (47.2313, 39.7233),
    "краснодар":        (45.0393, 38.9806),
    "омск":             (54.9885, 73.3242),
    "воронеж":          (51.6608, 39.2003),
    "пермь":            (58.0105, 56.2502),
    "волгоград":        (48.7080, 44.5133)
}

# === RATE LIMITING ===
RATE_LIMIT_SECONDS = 2
MAX_RETRIES = 3

logger.info(f"📋 Config: LOG_LEVEL={LOG_LEVEL}, UPDATE_INTERVAL={UPDATE_INTERVAL}s")
logger.info(f"📋 Broadcast: {DAILY_BROADCAST_HOUR:02d}:{DAILY_BROADCAST_MINUTE:02d} MSK")
logger.info(f"📋 Cities: {len(CITY_COORDINATES)}")