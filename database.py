"""Модуль работы с SQLite базой данных"""
import aiosqlite
from pathlib import Path
from typing import Optional, List, Dict
import logging
import re
from config import DB_PATH, CATEGORIES

logger = logging.getLogger(__name__)


class Database:
    """Асинхронная работа с SQLite базой данных"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        """Инициализация базы данных с миграциями"""
        async with aiosqlite.connect(self.db_path) as db:
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
                
            await db.commit()
            logger.info(f"База данных инициализирована: {self.db_path} (версия {max(current_version, 1)})")

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
            
            # Создаём дефолтные настройки если их нет
            await db.execute("""
                INSERT OR IGNORE INTO preferences (user_id, category, is_enabled)
                SELECT ?, category, 1 FROM (
                    SELECT 'weather' as category UNION ALL
                    SELECT 'crypto' UNION ALL
                    SELECT 'fiat' UNION ALL
                    SELECT 'news' UNION ALL
                    SELECT 'joke'
                )
            """, (user_id,))
            
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
            category: Категория (weather, crypto, fiat, news, joke)
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

    async def get_user_preferences(self, user_id: int) -> Dict[str, bool]:
        """
        Получение настроек пользователя.
        
        Args:
            user_id: Telegram ID пользователя
            
        Returns:
            Словарь {категория: включена}
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT category, is_enabled FROM preferences WHERE user_id = ?", 
                (user_id,)
            )
            rows = await cursor.fetchall()
            
            # Если настроек нет — возвращаем дефолтные
            if not rows:
                return {cat: True for cat in CATEGORIES}
            
            return {row[0]: bool(row[1]) for row in rows}

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
                        "preferences": {}
                    }
                
                if row["category"]:
                    users_map[uid]["preferences"][row["category"]] = bool(row["is_enabled"])
            
            # Заполняем дефолтными настройками если пусто
            for user in users_map.values():
                if not user["preferences"]:
                    user["preferences"] = {cat: True for cat in CATEGORIES}
                    
            return list(users_map.values())

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