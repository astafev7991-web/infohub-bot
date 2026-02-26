"""Модуль работы с SQLite базой данных"""
import aiosqlite
from pathlib import Path
from typing import Optional, List, Dict
import logging
import re
from config import DB_PATH, CATEGORIES, BASE_CATEGORIES, NEWS_CATEGORIES

logger = logging.getLogger(__name__)

BROADCAST_HOURS = [6, 7, 8, 9, 10, 11, 12, 18, 19, 20, 21]
REFERRAL_EXPIRE_DAYS = 7

# FIX: допустимые состояния меню — валидация в set_user_state
VALID_STATES = {"main", "news", "settings", "city", "time", "crypto"}


class Database:
    """Асинхронная работа с SQLite.

    FIX: единое постоянное соединение (self._conn) — открывается в init(), закрывается в close().
    Нет накладных расходов на открытие/закрытие при каждом запросе.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: Optional[aiosqlite.Connection] = None

    async def init(self) -> None:
        """Инициализация БД и подключение"""
        self._conn = await aiosqlite.connect(self.db_path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._run_migrations()
        logger.info(f"База данных инициализирована: {self.db_path}")

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None
            logger.debug("Database: connection closed")

    async def _run_migrations(self) -> None:
        """Запуск миграций по версиям"""
        db = self._conn

        cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='preferences'")
        prefs_exists = await cursor.fetchone()
        if prefs_exists:
            cursor = await db.execute("PRAGMA table_info(preferences)")
            columns = [row[1] for row in await cursor.fetchall()]
            if 'category' not in columns:
                logger.warning("Table 'preferences' has wrong structure, recreating...")
                await db.execute("DROP TABLE IF EXISTS preferences")
                await db.commit()

        await db.execute("""
            CREATE TABLE IF NOT EXISTS schema_versions (version INTEGER PRIMARY KEY)
        """)

        cursor = await db.execute("SELECT MAX(version) FROM schema_versions")
        row = await cursor.fetchone()
        current_version = row[0] if row and row[0] is not None else 0

        migrations = [
            (1, self._schema_v1),
            (2, self._migrate_v2),
            (3, self._migrate_v3),
            (4, self._migrate_v4),
            (5, self._migrate_v5),
        ]
        for version, fn in migrations:
            if current_version < version:
                await fn(db)
                await db.execute("INSERT INTO schema_versions (version) VALUES (?)", (version,))
                await db.commit()
                logger.info(f"Database: migrated to v{version}")

    async def _schema_v1(self, db) -> None:
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
        await db.execute("CREATE INDEX IF NOT EXISTS idx_preferences_user ON preferences(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_premium ON users(is_premium)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_city ON users(city)")

    async def _migrate_v2(self, db) -> None:
        try:
            await db.execute("ALTER TABLE users ADD COLUMN broadcast_hour INTEGER DEFAULT 9")
        except Exception:
            pass
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_broadcast_hour ON users(broadcast_hour)")
        await db.execute("DELETE FROM preferences WHERE category IN ('news', 'joke')")
        news_cats = ['news_top', 'news_world', 'news_technology', 'news_business',
                     'news_science', 'news_health', 'news_sports', 'news_entertainment',
                     'news_politics', 'news_all']
        for cat in news_cats:
            await db.execute(
                "INSERT OR IGNORE INTO preferences (user_id, category, is_enabled) SELECT user_id, ?, 1 FROM users",
                (cat,)
            )

    async def _migrate_v3(self, db) -> None:
        await db.execute("DELETE FROM preferences WHERE category IN ('news', 'joke')")
        for cat_key in CATEGORIES:
            await db.execute(
                "INSERT OR IGNORE INTO preferences (user_id, category, is_enabled) SELECT user_id, ?, 1 FROM users",
                (cat_key,)
            )

    async def _migrate_v4(self, db) -> None:
        try:
            await db.execute("ALTER TABLE users ADD COLUMN crypto_unlocked INTEGER DEFAULT 0")
        except Exception:
            pass
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

    async def _migrate_v5(self, db) -> None:
        """Миграция v5: персистентное состояние меню пользователя"""
        try:
            await db.execute("ALTER TABLE users ADD COLUMN last_state TEXT DEFAULT 'main'")
        except Exception:
            pass
        logger.debug("Database: v5 — added last_state column")

    # === ВСПОМОГАТЕЛЬНЫЕ ===

    @staticmethod
    def _validate_city(city: str) -> str:
        if not city:
            raise ValueError("Город не указан")
        city = city.strip().lower()[:50]
        if not city:
            raise ValueError("Город не указан")
        if not re.match(r'^[a-zA-Zа-яА-Яёё0-9\s\-]+$', city):
            raise ValueError("Город может содержать только буквы, цифры, пробелы и дефис")
        dangerous = ['--', '/*', '*/', ';', 'drop', 'delete', 'insert', 'update']
        if any(p in city for p in dangerous):
            raise ValueError("Недопустимые символы")
        return city

    @staticmethod
    def _validate_username(username: Optional[str]) -> str:
        if not username:
            return "anon"
        return username.lstrip('@').strip()[:50] or "anon"

    @staticmethod
    def _validate_first_name(first_name: Optional[str]) -> str:
        if not first_name:
            return "User"
        return first_name.strip()[:100] or "User"

    # === ПОЛЬЗОВАТЕЛИ ===

    async def add_user(
        self,
        user_id: int,
        username: Optional[str],
        first_name: Optional[str],
        city: str = "москва"
    ) -> None:
        try:
            city = self._validate_city(city)
        except ValueError:
            city = "москва"
        username = self._validate_username(username)
        first_name = self._validate_first_name(first_name)

        await self._conn.execute("""
            INSERT INTO users (user_id, username, first_name, city)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                city = excluded.city
        """, (user_id, username, first_name, city))

        await self._conn.executemany(
            "INSERT OR IGNORE INTO preferences (user_id, category, is_enabled) VALUES (?, ?, 1)",
            [(user_id, cat_key) for cat_key in CATEGORIES]
        )
        await self._conn.commit()
        logger.debug(f"User {user_id} added/updated")

    async def update_city(self, user_id: int, city: str) -> None:
        city = self._validate_city(city)
        result = await self._conn.execute(
            "UPDATE users SET city = ? WHERE user_id = ?", (city, user_id)
        )
        await self._conn.commit()
        if result.rowcount == 0:
            logger.warning(f"User {user_id} not found for city update")

    async def toggle_preference(self, user_id: int, category: str, enabled: bool) -> None:
        if category not in CATEGORIES:
            raise ValueError(f"Неизвестная категория: {category}")
        await self._conn.execute("""
            INSERT INTO preferences (user_id, category, is_enabled)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, category) DO UPDATE SET is_enabled = excluded.is_enabled
        """, (user_id, category, 1 if enabled else 0))
        await self._conn.commit()
        logger.info(f"User {user_id}: {category} set to {enabled}")

    async def get_user_preferences(self, user_id: int) -> Dict[str, bool]:
        cursor = await self._conn.execute(
            "SELECT category, is_enabled FROM preferences WHERE user_id = ?", (user_id,)
        )
        rows = await cursor.fetchall()
        result = {cat: True for cat in CATEGORIES}
        for row in rows:
            cat_key = row[0]
            if cat_key in CATEGORIES:
                result[cat_key] = bool(row[1])
        logger.debug(f"User {user_id} preferences loaded")
        return result

    async def get_user_city(self, user_id: int) -> str:
        cursor = await self._conn.execute(
            "SELECT city FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] else "москва"

    # === ПЕРСИСТЕНТНОЕ СОСТОЯНИЕ МЕНЮ ===

    async def get_user_state(self, user_id: int) -> str:
        """Получить текущее состояние меню пользователя.
        Возвращает 'main' если пользователь не найден.
        """
        cursor = await self._conn.execute(
            "SELECT last_state FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        state = row[0] if row and row[0] else "main"
        # Защита от невалидных значений в БД (например, после ручной правки)
        return state if state in VALID_STATES else "main"

    async def set_user_state(self, user_id: int, state: str) -> None:
        """Сохранить состояние меню пользователя.

        FIX 1: валидация — только допустимые состояния из VALID_STATES.
        FIX 2: INSERT OR IGNORE обеспечивает создание строки если user_id
                ещё нет в таблице (пользователь написал без /start).
        """
        if state not in VALID_STATES:
            raise ValueError(f"Недопустимое состояние: '{state}'. Допустимые: {VALID_STATES}")

        # Гарантируем наличие строки, затем обновляем
        await self._conn.execute(
            "INSERT OR IGNORE INTO users (user_id, last_state) VALUES (?, ?)",
            (user_id, state)
        )
        await self._conn.execute(
            "UPDATE users SET last_state = ? WHERE user_id = ?",
            (state, user_id)
        )
        await self._conn.commit()

    # === БРОАДКАСТ ===

    async def _fetch_users(self, where_clause: str = "", params: tuple = ()) -> List[Dict]:
        """
        FIX: общая базовая логика JOIN+агрегации —
        устраняет дублирование между get_all_active_users и get_users_by_broadcast_hour.
        """
        sql = """
            SELECT u.user_id, u.username, u.first_name, u.city, u.is_premium,
                   p.category, p.is_enabled
            FROM users u
            LEFT JOIN preferences p ON u.user_id = p.user_id
        """
        if where_clause:
            sql += f" WHERE {where_clause}"

        cursor = await self._conn.execute(sql, params)
        users_map: Dict[int, Dict] = {}
        async for row in cursor:
            uid = row[0]
            if uid not in users_map:
                users_map[uid] = {
                    "user_id": uid,
                    "username": row[1],
                    "first_name": row[2],
                    "city": row[3],
                    "is_premium": bool(row[4]),
                    "preferences": {cat: True for cat in CATEGORIES}
                }
            if row[5]:
                users_map[uid]["preferences"][row[5]] = bool(row[6])
        return list(users_map.values())

    async def get_all_active_users(self) -> List[Dict]:
        return await self._fetch_users()

    async def get_users_by_broadcast_hour(self, hour: int) -> List[Dict]:
        return await self._fetch_users("u.broadcast_hour = ?", (hour,))

    # === РЕФЕРАЛЬНАЯ СИСТЕМА ===

    async def is_crypto_unlocked(self, user_id: int) -> bool:
        cursor = await self._conn.execute(
            "SELECT crypto_unlocked FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return bool(row[0]) if row else False

    async def unlock_crypto(self, user_id: int) -> None:
        await self._conn.execute(
            "UPDATE users SET crypto_unlocked = 1 WHERE user_id = ?", (user_id,)
        )
        await self._conn.commit()
        logger.info(f"User {user_id}: crypto unlocked")

    async def add_referral(self, referrer_id: int, referred_id: int) -> bool:
        if referrer_id == referred_id:
            return False
        cursor = await self._conn.execute(
            "SELECT id FROM referrals WHERE referred_id = ?", (referred_id,)
        )
        if await cursor.fetchone():
            return False
        try:
            await self._conn.execute(
                "INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
                (referrer_id, referred_id)
            )
            await self._conn.execute(
                "UPDATE users SET crypto_unlocked = 1 WHERE user_id = ?", (referrer_id,)
            )
            await self._conn.commit()
            logger.info(f"Referral added: {referrer_id} <- {referred_id}")
            return True
        except Exception as e:
            logger.error(f"Error adding referral: {e}")
            return False

    async def get_referral_count(self, user_id: int) -> int:
        cursor = await self._conn.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def is_already_referred(self, user_id: int) -> bool:
        cursor = await self._conn.execute(
            "SELECT id FROM referrals WHERE referred_id = ?", (user_id,)
        )
        return bool(await cursor.fetchone())

    async def mark_premium(self, user_id: int, is_premium: bool) -> None:
        await self._conn.execute(
            "UPDATE users SET is_premium = ? WHERE user_id = ?",
            (1 if is_premium else 0, user_id)
        )
        await self._conn.commit()

    async def get_user_count(self) -> int:
        cursor = await self._conn.execute("SELECT COUNT(*) FROM users")
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def get_premium_user_count(self) -> int:
        cursor = await self._conn.execute("SELECT COUNT(*) FROM users WHERE is_premium = 1")
        row = await cursor.fetchone()
        return row[0] if row else 0

    async def set_broadcast_hour(self, user_id: int, hour: int) -> None:
        if hour not in BROADCAST_HOURS:
            raise ValueError(f"Недопустимый час: {hour}")
        await self._conn.execute(
            "UPDATE users SET broadcast_hour = ? WHERE user_id = ?", (hour, user_id)
        )
        await self._conn.commit()

    async def get_broadcast_hour(self, user_id: int) -> int:
        cursor = await self._conn.execute(
            "SELECT broadcast_hour FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] else 9