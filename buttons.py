# buttons.py
"""
Константы текстов для кнопок клавиатуры.
"""

# === Главное меню ===
BTN_MAIN_DIGEST = "📊 Мой дайджест"
BTN_NEWS = "📰 Новости"
BTN_SETTINGS = "⚙️ Настройки"
BTN_CHANGE_CITY = "🌍 Сменить город"

# === Новости ===
BTN_CRYPTO_DIGEST = "💰 Крипто-дайджест"
BTN_NEWS_TOP = "📰 Главное"
BTN_NEWS_WORLD = "🌍 В мире"
BTN_NEWS_TECH = "💻 Технологии"
BTN_NEWS_BIZ = "💼 Бизнес"
BTN_NEWS_SCI = "🔬 Наука"
BTN_NEWS_HEALTH = "🏥 Здоровье"
BTN_NEWS_SPORT = "⚽ Спорт"
BTN_NEWS_ENT = "🎬 Развлечения"
BTN_NEWS_POL = "🏛️ Политика"
BTN_NEWS_ALL = "📊 Все новости"

# === Настройки ===
SECTION_MAIN = "─── Основное ───"
SECTION_NEWS = "─── Новости ───"
SECTION_BROADCAST = "─── Рассылка ───"

BTN_WEATHER = "🌤 Погода"
BTN_CRYPTO = "💰 Криптовалюты"
BTN_FIAT = "💱 Курсы валют"

BTN_TIME_BACK = "🔙 Назад в настройки"

# === Общее ===
BTN_BACK_MENU = "🔙 Назад в меню"
BTN_REFRESH_CRYPTO = "🔄 Обновить крипто"

# === Маппинг для обработчиков ===
SETTINGS_BUTTON_MAP = {
    BTN_WEATHER: "weather",
    BTN_CRYPTO: "crypto",
    BTN_FIAT: "fiat",
    BTN_NEWS_TOP: "news_top",
    BTN_NEWS_WORLD: "news_world",
    BTN_NEWS_TECH: "news_technology",
    BTN_NEWS_BIZ: "news_business",
    BTN_NEWS_SCI: "news_science",
    BTN_NEWS_HEALTH: "news_health",
    BTN_NEWS_SPORT: "news_sports",
    BTN_NEWS_ENT: "news_entertainment",
    BTN_NEWS_POL: "news_politics",
    BTN_NEWS_ALL: "news_all",
}

NEWS_BUTTON_MAP = {
    BTN_NEWS_TOP: "top",
    BTN_NEWS_WORLD: "world",
    BTN_NEWS_TECH: "technology",
    BTN_NEWS_BIZ: "business",
    BTN_NEWS_SCI: "science",
    BTN_NEWS_HEALTH: "health",
    BTN_NEWS_SPORT: "sports",
    BTN_NEWS_ENT: "entertainment",
    BTN_NEWS_POL: "politics",
}