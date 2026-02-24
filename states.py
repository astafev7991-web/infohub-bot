"""
FSM-состояния пользователя для ИнфоХаб.
Заменяет словарь _user_state в BotApp — состояния теперь персистентны
между запросами и переживают рестарт (при использовании RedisStorage).
"""
from aiogram.fsm.state import State, StatesGroup


class UserState(StatesGroup):
    """Экраны (состояния) интерфейса бота."""
    main = State()       # Главное меню
    settings = State()   # Настройки категорий
    time = State()       # Выбор времени рассылки
    city = State()       # Выбор города
    news = State()       # Меню новостей
    crypto = State()     # Меню крипто
