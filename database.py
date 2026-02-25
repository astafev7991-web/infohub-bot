"""Модуль работы с SQLite базой данных"""
import aiosqlite
from pathlib import Path
from typing import Optional, List, Dict
import logging
import re
from config import DB_PATH, CATEGORIES, BASE_CATEGORIES, NEWS_CATEGORIES

logger = logging.getLogger(__name__)

# Доступные времена для рассылки (часы по МСК)
BROADCAST_HOURS = [6, 7, 8, 9, 10, 11, 12, 18, 19, 20, 21]

# Срок действия реферальной ссылки (дни)
REFERRAL_EXPIRE_DAYS = 7


class Database:
    """Асинхронная работа с SQLite базой данных"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        """Инициализация базы данных с миграциями"""
        async with aiosqlite.connect(self.db_path) as db:
            # Проверяем существование таблиц и их структуру
            cursor = await db.execute("""
                SELECT name FROM sqlite_master WHERE type='table' AND name='preferences'
            """)
            prefs_exists = await cursor.fetchone()
            
            if prefs_exists:
                # Проверяем структуру таблицы preferences
                cursor = await db.execute("PRAGMA table_info(preferences)")
                columns = [row[1] for row in await cursor.fetchall()]
                
                # Если нет колонки category - пересоздаём таблицу
                if 'category' not in columns:
                    logger.warning("Table 'preferences' has wrong structure, recreating...")
                    await db.execute("DROP TABLE IF EXISTS preferences")
                    await db.commit()  # Важно: коммитим удаление!
                    prefs_exists = False
            
            # Таблица версий для миграций
            await db.execute("""
                CREATE TABLE IF NOT EXISTS schema_versions (
                    version INTEGER PRIMARY KEY
                )
            """)
            
            cursor = await db.execute("SELECT MAX(version) FROM schema_versions")
            row = await cursor.fetchone()
            current_version = row[0] if row and row[0] is not None else 0

            if current_version < 1:
                await self._create_schema_v1(db)
                await db.execute("INSERT INTO schema_versions (version) VALUES (1)")
                await db.commit()  # Коммитим после создания схемы
                
            if current_version < 2:
                await self._migrate_to_v2(db)
                await db.execute("INSERT INTO schema_versions (version) VALUES (2)")
                await db.commit()
                
            if current_version < 3:
                await self._migrate_to_v3(db)
                await db.execute("INSERT INTO schema_versions (version) VALUES (3)")
                await db.commit()
                
            if current_version < 4:
                await self._migrate_to_v4(db)
                await db.execute("INSERT INTO schema_versions (version) VALUES (4)")
                await db.commit()
                
            logger.info(f"База данных инициализирована: {self.db_path} (версия {max(current_version, 4)})")

    async def _create_schema_v1(self, db: aiosqlite.Connection) -> None:
        """Создание схемы версии 1"""
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                city TEXT DEFAULT 'москва',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_premium INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS preferences (
                user_id INTEGER,
                category TEXT,
                is_enabled INTEGER DEFAULT 1,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                PRIMARY KEY (user_id, category)
            )
        """)
        # Индексы для оптимизации
        await db.execute("CREATE INDEX IF NOT EXISTS idx_preferences_user ON preferences(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_premium ON users(is_premium)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_city ON users(city)")
        logger.debug("Schema v1 created")

    async def _migrate_to_v2(self, db: aiosqlite.Connection) -> None:
        """Миграция к версии 2 — добавление времени рассылки и новых категорий"""
        # Добавляем колонку broadcast_hour
        try:
            await db.execute("""
                ALTER TABLE users ADD COLUMN broadcast_hour INTEGER DEFAULT 9
            """)
        except Exception:
            pass  # Колонка уже существует
            
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_broadcast_hour ON users(broadcast_hour)")
        
        # Удаляем старые категории (news, joke)
        await db.execute("DELETE FROM preferences WHERE category IN ('news', 'joke')")
        
        # Добавляем новые категории новостей всем пользователям
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_top', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_world', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_technology', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_business', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_science', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_health', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_sports', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_entertainment', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_politics', 1 FROM users
        """)
        await db.execute("""
            INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
            SELECT user_id, 'news_all', 1 FROM users
        """)
        
        logger.debug("Schema v2 created - added broadcast_hour and news categories")

    async def _migrate_to_v3(self, db: aiosqlite.Connection) -> None:
        """Миграция к версии 3 — гарантированное добавление всех категорий"""
        # Удаляем старые категории (news, joke) если остались
        await db.execute("DELETE FROM preferences WHERE category IN ('news', 'joke')")
        
        # Добавляем ВСЕ категории из CATEGORIES всем пользователям
        for cat_key in CATEGORIES.keys():
            await db.execute("""
                INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
                SELECT user_id, ?, 1 FROM users
            """, (cat_key,))
            
        logger.debug("Schema v3 created - ensured all categories exist")

    async def _migrate_to_v4(self, db: aiosqlite.Connection) -> None:
        """Миграция к версии 4 — реферальная система"""
        # Добавляем колонку crypto_unlocked
        try:
            await db.execute("""
                ALTER TABLE users ADD COLUMN crypto_unlocked INTEGER DEFAULT 0
            """)
        except Exception:
            pass  # Колонка уже существует
            
        # Создаём таблицу рефералов
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER NOT NULL,
                referred_id INTEGER NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                FOREIGN KEY (referred_id) REFERENCES users(user_id)
            )
        """)
        
        await db.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referred ON referrals(referred_id)")
        
        logger.debug("Schema v4 created - added referral system")

    @staticmethod
    def _validate_city(city: str) -> str:
        """
        Валидация названия города.
        
        Args:
            city: Название города
            
        Returns:
            Нормализованное название города
            
        Raises:
            ValueError: Если название некорректно
        """
        if not city:
            raise ValueError("Город не указан")
        
        # Нормализация
        city = city.strip().lower()[:50]
        
        # Проверка на пустоту после обрезки
        if not city:
            raise ValueError("Город не указан")
        
        # Проверка символов: только кириллица, латиница, пробелы, дефис, цифры
        if not re.match(r'^[a-zA-Zа-яА-ЯёЁ0-9\s\-]+$', city):
            raise ValueError("Город может содержать только буквы, цифры, пробелы и дефис")
        
        # Проверка на SQL-инъекции (дополнительная защита)
        dangerous_patterns = ['--', '/*', '*/', ';', 'drop', 'delete', 'insert', 'update']
        if any(pattern in city for pattern in dangerous_patterns):
            raise ValueError("Недопустимые символы в названии города")
        
        return city

    @staticmethod
    def _validate_username(username: Optional[str]) -> str:
        """Валидация и нормализация username"""
        if not username:
            return "anon"
        # Убираем @ если есть
        username = username.lstrip('@').strip()
        # Ограничиваем длину
        return username[:50] if username else "anon"

    @staticmethod
    def _validate_first_name(first_name: Optional[str]) -> str:
        """Валидация и нормализация имени"""
        if not first_name:
            return "User"
        return first_name.strip()[:100] if first_name.strip() else "User"

    async def add_user(
        self, 
        user_id: int, 
        username: Optional[str], 
        first_name: Optional[str], 
        city: str = "москва"
    ) -> None:
        """
        Добавление или обновление пользователя.
        
        Args:
            user_id: Telegram ID пользователя
            username: Username в Telegram
            first_name: Имя пользователя
            city: Город (по умолчанию Москва)
        """
        # Валидация
        try:
            city = self._validate_city(city)
        except ValueError:
            city = "москва"
        
        username = self._validate_username(username)
        first_name = self._validate_first_name(first_name)
        
        async with aiosqlite.connect(self.db_path) as db:
            # Upsert пользователя
            await db.execute("""
                INSERT INTO users (user_id, username, first_name, city)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    city = excluded.city
            """, (user_id, username, first_name, city))
            
            # Создаём дефолтные настройки если их нет (по одной категории)
            for cat_key in CATEGORIES.keys():
                await db.execute("""
                    INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
                    VALUES (?, ?, 1)
                """, (user_id, cat_key))
            
            await db.commit()
            logger.debug(f"User {user_id} added/updated")

    async def update_city(self, user_id: int, city: str) -> None:
        """
        Обновление города пользователя.
        
        Args:
            user_id: Telegram ID пользователя
            city: Новый город
            
        Raises:
            ValueError: Если город некорректен
        """
        city = self._validate_city(city)
        
        async with aiosqlite.connect(self.db_path) as db:
            result = await db.execute(
                "UPDATE users SET city = ? WHERE user_id = ?", 
                (city, user_id)
            )
            await db.commit()
            
            if result.rowcount == 0:
                logger.warning(f"User {user_id} not found for city update")

    async def toggle_preference(self, user_id: int, category: str, enabled: bool) -> None:
        """
        Переключение категории в настройках.
        
        Args:
            user_id: Telegram ID пользователя
            category: Категория (weather, crypto, fiat, news_*)
            enabled: Включена или выключена
            
        Raises:
            ValueError: Если категория неизвестна
        """
        if category not in CATEGORIES:
            raise ValueError(f"Неизвестная категория: {category}")
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO preferences (user_id, category, is_enabled) 
                VALUES (?, ?, ?)
                ON CONFLICT(user_id, category) DO UPDATE SET 
                    is_enabled = excluded.is_enabled
            """, (user_id, category, 1 if enabled else 0))
            await db.commit()
            logger.info(f"User {user_id}: {category} set to {enabled}")

    async def get_user_preferences(self, user_id: int) -> Dict[str, bool]:
        """
        Получение настроек пользователя.
        
        Args:
            user_id: Telegram ID пользователя
            
        Returns:
            Словарь {категория: включена} — все категории с дефолтными значениями
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT category, is_enabled FROM preferences WHERE user_id = ?", 
                (user_id,)
            )
            rows = await cursor.fetchall()
            
            # Начинаем с дефолтных значений для ВСЕХ категорий
            result = {cat: True for cat in CATEGORIES}
            
            # Обновляем тем, что есть в базе
            for row in rows:
                cat_key = row[0]
                is_enabled = bool(row[1])
                if cat_key in CATEGORIES:  # Только известные категории
                    result[cat_key] = is_enabled
            
            logger.debug(f"User {user_id} preferences: {result}")
            return result

    async def get_user_city(self, user_id: int) -> str:
        """
        Получение города пользователя.
        
        Args:
            user_id: Telegram ID пользователя
            
        Returns:
            Название города (по умолчанию 'москва')
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT city FROM users WHERE user_id = ?", 
                (user_id,)
            )
            row = await cursor.fetchone()
            return row[0] if row and row[0] else "москва"

    async def get_all_active_users(self) -> List[Dict]:
        """
        Получение всех активных пользователей с их настройками.
        
        Returns:
            Список словарей с информацией о пользователях
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT 
                    u.user_id, 
                    u.username, 
                    u.first_name, 
                    u.city, 
                    u.is_premium,
                    p.category, 
                    p.is_enabled
                FROM users u
                LEFT JOIN preferences p ON u.user_id = p.user_id
            """)
            
            users_map: Dict[int, Dict] = {}
            async for row in cursor:
                uid = row["user_id"]
                if uid not in users_map:
                    users_map[uid] = {
                        "user_id": uid,
                        "username": row["username"],
                        "first_name": row["first_name"],
                        "city": row["city"],
                        "is_premium": bool(row["is_premium"]),
                        "preferences": {cat: True for cat in CATEGORIES}  # Дефолтные
                    }
                
                if row["category"]:
                    users_map[uid]["preferences"][row["category"]] = bool(row["is_enabled"])
            
            return list(users_map.values())

    # === РЕФЕРАЛЬНАЯ СИСТЕМА ===
    
    async def is_crypto_unlocked(self, user_id: int) -> bool:
        """Проверка разблокировки крипто-дайджеста"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT crypto_unlocked FROM users WHERE user_id = ?",
                (user_id,)
            )
            row = await cursor.fetchone()
            return bool(row[0]) if row else False
    
    async def unlock_crypto(self, user_id: int) -> None:
        """Разблокировка крипто-дайджеста"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET crypto_unlocked = 1 WHERE user_id = ?",
                (user_id,)
            )
            await db.commit()
            logger.info(f"User {user_id}: crypto unlocked")
    
    async def add_referral(self, referrer_id: int, referred_id: int) -> bool:
        """
        Добавление реферала.
        
        Args:
            referrer_id: ID того, кто пригласил
            referred_id: ID того, кого пригласили
            
        Returns:
            True если реферал добавлен успешно, False если уже существует
        """
        # Защита: нельзя быть своим рефералом
        if referrer_id == referred_id:
            logger.warning(f"Self-referral attempt: {referrer_id}")
            return False
        
        async with aiosqlite.connect(self.db_path) as db:
            # Проверяем, что пользователь ещё не чей-то реферал
            cursor = await db.execute(
                "SELECT id FROM referrals WHERE referred_id = ?",
                (referred_id,)
            )
            if await cursor.fetchone():
                logger.debug(f"User {referred_id} is already a referral")
                return False
            
            # Добавляем реферала
            try:
                await db.execute("""
                    INSERT INTO referrals (referrer_id, referred_id)
                    VALUES (?, ?)
                """, (referrer_id, referred_id))
                
                # Разблокируем крипто пригласившему
                await db.execute(
                    "UPDATE users SET crypto_unlocked = 1 WHERE user_id = ?",
                    (referrer_id,)
                )
                
                await db.commit()
                logger.info(f"Referral added: {referrer_id} <- {referred_id}")
                return True
            except Exception as e:
                logger.error(f"Error adding referral: {e}")
                return False
    
    async def get_referral_count(self, user_id: int) -> int:
        """Количество рефералов пользователя"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?",
                (user_id,)
            )
            row = await cursor.fetchone()
            return row[0] if row else 0
    
    async def is_already_referred(self, user_id: int) -> bool:
        """Проверка что пользователь уже чей-то реферал"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT id FROM referrals WHERE referred_id = ?",
                (user_id,)
            )
            return bool(await cursor.fetchone())

    async def mark_premium(self, user_id: int, is_premium: bool) -> None:
        """
        Установка премиум-статуса пользователя.
        
        Args:
            user_id: Telegram ID пользователя
            is_premium: True для премиум, False для обычного
        """
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET is_premium = ? WHERE user_id = ?", 
                (1 if is_premium else 0, user_id)
            )
            await db.commit()

    async def get_user_count(self) -> int:
        """Получение общего количества пользователей"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_premium_user_count(self) -> int:
        """Получение количества премиум-пользователей"""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_premium = 1")
            row = await cursor.fetchone()
            return row[0] if row else 0
    
    async def set_broadcast_hour(self, user_id: int, hour: int) -> None:
        """
        Установка времени рассылки для пользователя.
        
        Args:
            user_id: Telegram ID пользователя
            hour: Час рассылки (6-21 по МСК)
        """
        if hour not in BROADCAST_HOURS:
            raise ValueError(f"Недопустимый час: {hour}. Допустимые: {BROADCAST_HOURS}")
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE users SET broadcast_hour = ? WHERE user_id = ?",
                (hour, user_id)
            )
            await db.commit()
            logger.debug(f"User {user_id} broadcast hour set to {hour}")
    
    async def get_broadcast_hour(self, user_id: int) -> int:
        """
        Получение времени рассылки пользователя.
        
        Args:
            user_id: Telegram ID пользователя
            
        Returns:
            Час рассылки (по умолчанию 9)
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT broadcast_hour FROM users WHERE user_id = ?",
                (user_id,)
            )
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 9
    
    async def get_users_by_broadcast_hour(self, hour: int) -> List[Dict]:
        """
        Получение пользователей для рассылки в указанный час.
        
        Args:
            hour: Час рассылки
            
        Returns:
            Список пользователей с их настройками
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("""
                SELECT 
                    u.user_id, 
                    u.username, 
                    u.first_name, 
                    u.city, 
                    u.is_premium,
                    p.category, 
                    p.is_enabled
                FROM users u
                LEFT JOIN preferences p ON u.user_id = p.user_id
                WHERE u.broadcast_hour = ?
            """, (hour,))
            
            users_map: Dict[int, Dict] = {}
            async for row in cursor:
                uid = row["user_id"]
                if uid not in users_map:
                    users_map[uid] = {
                        "user_id": uid,
                        "username": row["username"],
                        "first_name": row["first_name"],
                        "city": row["city"],
                        "is_premium": bool(row["is_premium"]),
                        "preferences": {cat: True for cat in CATEGORIES}  # Дефолтные
                    }
                
                if row["category"]:
                    users_map[uid]["preferences"][row["category"]] = bool(row["is_enabled"])
            
            return list(users_map.values())