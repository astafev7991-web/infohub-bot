"""
Построители reply-клавиатур для ИнфоХаб.
Все функции чистые — не зависят от db/api/bot.
"""
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from config import BASE_CATEGORIES, NEWS_CATEGORIES, CITY_COORDINATES
from database import BROADCAST_HOURS


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📊 Мой дайджест")],
            [KeyboardButton(text="📰 Новости")],
            [KeyboardButton(text="⚙️ Настройки"), KeyboardButton(text="🌍 Сменить город")],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def get_settings_keyboard(user_prefs: dict, broadcast_hour: int = 9) -> ReplyKeyboardMarkup:
    """Меню настроек с разделами Основное / Новости / Рассылка."""
    buttons = []

    buttons.append([KeyboardButton(text="─── Основное ───")])
    for cat_key, cat_name in BASE_CATEGORIES.items():
        is_enabled = user_prefs.get(cat_key, True)
        status = "✅" if is_enabled else "❌"
        buttons.append([KeyboardButton(text=f"{status} {cat_name}")])

    buttons.append([KeyboardButton(text="─── Новости ───")])
    news_row: list[KeyboardButton] = []
    for cat_key, cat_name in NEWS_CATEGORIES.items():
        is_enabled = user_prefs.get(cat_key, True)
        status = "✅" if is_enabled else "❌"
        news_row.append(KeyboardButton(text=f"{status} {cat_name}"))
        if len(news_row) == 2:
            buttons.append(news_row)
            news_row = []
    if news_row:
        buttons.append(news_row)

    buttons.append([KeyboardButton(text="─── Рассылка ───")])
    buttons.append([KeyboardButton(text=f"⏰ Время: {broadcast_hour:02d}:00 МСК")])
    buttons.append([KeyboardButton(text="🔙 Назад в меню")])

    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_time_keyboard(current_hour: int = 9) -> ReplyKeyboardMarkup:
    """Выбор часа ежедневной рассылки (утро 6-12, вечер 18-21)."""
    buttons: list[list[KeyboardButton]] = []
    row: list[KeyboardButton] = []

    for hour in [6, 7, 8, 9, 10, 11, 12, 18, 19, 20, 21]:
        marker = "✓ " if hour == current_hour else ""
        row.append(KeyboardButton(text=f"{marker}{hour:02d}:00"))
        if len(row) == 4:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    buttons.append([KeyboardButton(text="🔙 Назад в настройки")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_city_keyboard() -> ReplyKeyboardMarkup:
    """Список первых 8 городов из конфига."""
    buttons: list[list[KeyboardButton]] = []
    row: list[KeyboardButton] = []
    for city in list(CITY_COORDINATES.keys())[:8]:
        row.append(KeyboardButton(text=city.title()))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([KeyboardButton(text="🔙 Назад в меню")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_news_keyboard() -> ReplyKeyboardMarkup:
    """Меню выбора новостной категории."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Крипто-дайджест")],
            [KeyboardButton(text="📰 Главное"), KeyboardButton(text="🌍 В мире")],
            [KeyboardButton(text="💻 Технологии"), KeyboardButton(text="💼 Бизнес")],
            [KeyboardButton(text="🔬 Наука"), KeyboardButton(text="🏥 Здоровье")],
            [KeyboardButton(text="⚽ Спорт"), KeyboardButton(text="🎬 Развлечения")],
            [KeyboardButton(text="🏛️ Политика"), KeyboardButton(text="📊 Все новости")],
            [KeyboardButton(text="🔙 Назад в меню")],
        ],
        resize_keyboard=True,
    )


def get_crypto_keyboard() -> ReplyKeyboardMarkup:
    """Меню крипто-раздела."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔄 Обновить крипто")],
            [KeyboardButton(text="🔙 Назад в меню")],
        ],
        resize_keyboard=True,
    )
