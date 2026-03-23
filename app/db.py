"""SQLite: init, read/write portfolio and asset classes."""
import os
import sqlite3
from pathlib import Path
from typing import List, Optional

from app.models import AssetClass, AssetSubclass, Position, Storage, Transaction

DB_PATH = os.environ.get("PORTFOLIO_DB", str(Path(__file__).resolve().parent.parent / "data" / "portfolio.db"))

# Начальный список мест хранения (полные названия). Добавляются в БД, если такого имени ещё нет.
# Ожидаемые суммы по классам (= сумма подклассов ниже); в БД у классов target_pct выставляется только расчётом.
ASSET_CLASS_DEFAULT_PCT = {
    "Акции": 55.80,
    "Недвижимость": 9.30,
    "Облигации": 23.25,
    "Товары": 4.65,
    "Криптовалюта": 7.00,
}

# (класс, подкласс, целевой % портфеля, sort_order)
ASSET_SUBCLASS_DEFAULT_ROWS = [
    ("Акции", "Акции США", 14.880, 1),
    ("Акции", "Акции развитых стран кроме США", 10.230, 2),
    ("Акции", "Акции Еврозоны", 0.0, 3),
    ("Акции", "Акции РФ", 12.090, 4),
    ("Акции", "Акции Китая", 12.090, 5),
    ("Акции", "Акции развивающихся стран кроме Китая", 6.510, 6),
    ("Акции", "Акции развивающихся стран", 0.0, 7),
    ("Недвижимость", "Недвижимость США", 4.650, 1),
    ("Недвижимость", "Весь мир кроме США", 4.650, 2),
    ("Облигации", "Гособлигации США", 4.650, 1),
    ("Облигации", "Корпоративные облигации США", 6.510, 2),
    ("Облигации", "Гособлигации РФ", 2.790, 3),
    ("Облигации", "Корпоративные облигации РФ", 4.650, 4),
    ("Облигации", "Облигации всего мира кроме США", 4.650, 5),
    ("Товары", "Золото (Иностранный брокер)", 2.325, 1),
    ("Товары", "Золото (Российский брокер)", 2.325, 2),
    ("Криптовалюта", "BTC+ETH", 4.200, 1),
    ("Криптовалюта", "Прочая криптовалюта", 2.800, 2),
]


DEFAULT_STORAGE_NAMES_ORDERED = [
    "Interactive Brokers",  # IB
    "Freedom Finance",  # FF
    "Т-Банк",  # ТФ
    "Т-Банк ИИС Тип А",  # ТФ ИИС А
    "БКС",  # БКС
    "Bybit",
    "MetaMask",
    "Trust Wallet",
    "Tangem",
]


def _ensure_seed_storages(conn: sqlite3.Connection) -> None:
    """Вставить стартовые места хранения, если их ещё нет (имя уникально)."""
    for order, name in enumerate(DEFAULT_STORAGE_NAMES_ORDERED):
        exists = conn.execute("SELECT 1 FROM storages WHERE name = ?", (name,)).fetchone()
        if exists is None:
            conn.execute(
                "INSERT INTO storages (name, sort_order) VALUES (?, ?)",
                (name, order),
            )
    conn.commit()


def _remove_legacy_default_named_storage(conn: sqlite3.Connection) -> None:
    """Убрать устаревшее место «По умолчанию»: перенести сделки на другое место и удалить строку."""
    legacy = conn.execute(
        "SELECT id FROM storages WHERE name = ?", ("По умолчанию",)
    ).fetchone()
    if legacy is None:
        return
    legacy_id = int(legacy["id"])
    replacement = conn.execute(
        "SELECT id FROM storages WHERE id != ? ORDER BY sort_order, id LIMIT 1",
        (legacy_id,),
    ).fetchone()
    if replacement is None:
        conn.execute(
            "UPDATE storages SET name = ?, sort_order = 0 WHERE id = ?",
            (DEFAULT_STORAGE_NAMES_ORDERED[0], legacy_id),
        )
        conn.commit()
        return
    rep_id = int(replacement["id"])
    conn.execute(
        "UPDATE transactions SET storage_id = ? WHERE storage_id = ?",
        (rep_id, legacy_id),
    )
    conn.execute("DELETE FROM storages WHERE id = ?", (legacy_id,))
    conn.commit()


def _ensure_data_dir():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    _ensure_data_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS asset_classes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                target_pct REAL NOT NULL DEFAULT 0,
                sort_order INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS asset_subclasses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset_class_id INTEGER NOT NULL REFERENCES asset_classes(id),
                name TEXT NOT NULL,
                target_pct REAL NOT NULL DEFAULT 0,
                sort_order INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS instruments (
                ticker TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                provider_symbol TEXT
            );
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                amount REAL NOT NULL,
                asset_subclass_id INTEGER NOT NULL REFERENCES asset_subclasses(id),
                currency TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                amount REAL NOT NULL,
                asset_subclass_id INTEGER NOT NULL REFERENCES asset_subclasses(id),
                currency TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS storages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                sort_order INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS historical_quotes (
                ticker TEXT NOT NULL,
                quote_date TEXT NOT NULL,
                provider TEXT NOT NULL,
                provider_symbol TEXT,
                price REAL,
                currency TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (ticker, quote_date)
            );
        """)
        conn.commit()
        # Стартовые места хранения (IB, Тинькофф, кошельки…) + колонка storage_id у сделок
        _ensure_seed_storages(conn)
        _remove_legacy_default_named_storage(conn)
        try:
            conn.execute("SELECT storage_id FROM transactions LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE transactions ADD COLUMN storage_id INTEGER")
            conn.commit()
            default_sid_row = conn.execute("SELECT id FROM storages ORDER BY id LIMIT 1").fetchone()
            default_sid = int(default_sid_row[0]) if default_sid_row else 1
            conn.execute(
                "UPDATE transactions SET storage_id = ? WHERE storage_id IS NULL",
                (default_sid,),
            )
            conn.commit()
        # instruments.asset_subclass_id — подкласс по тикеру (настройка пользователя)
        try:
            conn.execute("SELECT asset_subclass_id FROM instruments LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE instruments ADD COLUMN asset_subclass_id INTEGER")
            conn.commit()
        # instruments.buy_blocked — тикер запрещён к покупке в ребалансировке
        try:
            conn.execute("SELECT buy_blocked FROM instruments LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE instruments ADD COLUMN buy_blocked INTEGER NOT NULL DEFAULT 0")
            conn.commit()
        # Migrate existing positions into transactions (one tx per position)
        cur = conn.execute("SELECT COUNT(*) FROM transactions")
        if cur.fetchone()[0] == 0:
            cur = conn.execute("SELECT ticker, amount, asset_subclass_id, currency FROM positions")
            rows = cur.fetchall()
            default_sid_row = conn.execute("SELECT id FROM storages ORDER BY id LIMIT 1").fetchone()
            default_sid = int(default_sid_row[0]) if default_sid_row else 1
            for r in rows:
                conn.execute(
                    """INSERT INTO transactions (ticker, amount, asset_subclass_id, currency, storage_id)
                       VALUES (?, ?, ?, ?, ?)""",
                    (r[0], r[1], r[2], r[3], default_sid),
                )
            if rows:
                conn.commit()
    finally:
        conn.close()


def seed_asset_classes_if_empty():
    conn = get_conn()
    try:
        cur = conn.execute("SELECT COUNT(*) FROM asset_classes")
        if cur.fetchone()[0] > 0:
            return
        # From spreadsheet: Акции, Недвижимость, Облигации, Товары, Криптовалюта
        conn.executemany(
            "INSERT INTO asset_classes (name, target_pct, sort_order) VALUES (?, ?, ?)",
            [
                ("Акции", 0.0, 1),
                ("Недвижимость", 0.0, 2),
                ("Облигации", 0.0, 3),
                ("Товары", 0.0, 4),
                ("Криптовалюта", 0.0, 5),
            ],
        )
        id_by_class = {
            r["name"]: int(r["id"])
            for r in conn.execute("SELECT id, name FROM asset_classes")
        }
        subclasses = [
            (id_by_class[cname], sname, pct, so)
            for cname, sname, pct, so in ASSET_SUBCLASS_DEFAULT_ROWS
        ]
        conn.executemany(
            "INSERT INTO asset_subclasses (asset_class_id, name, target_pct, sort_order) VALUES (?, ?, ?, ?)",
            subclasses,
        )
        _reconcile_asset_class_targets_in_conn(conn)
        conn.commit()
    finally:
        conn.close()


def _reconcile_asset_class_targets_in_conn(conn: sqlite3.Connection) -> None:
    """target_pct класса = сумма target_pct подклассов этого класса."""
    for row in conn.execute("SELECT id FROM asset_classes"):
        cid = int(row["id"])
        tot = conn.execute(
            "SELECT COALESCE(SUM(target_pct), 0) FROM asset_subclasses WHERE asset_class_id = ?",
            (cid,),
        ).fetchone()[0]
        conn.execute(
            "UPDATE asset_classes SET target_pct = ? WHERE id = ?",
            (round(float(tot), 3), cid),
        )


def reconcile_asset_class_targets() -> None:
    """Пересчитать доли всех классов из подклассов (после миграций / на старте)."""
    conn = get_conn()
    try:
        _reconcile_asset_class_targets_in_conn(conn)
        conn.commit()
    finally:
        conn.close()


def apply_default_target_percentages_if_unset() -> None:
    """
    Если сумма целевых долей подклассов ≈ 0, проставить распределение из констант
    и пересчитать классы.
    """
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT COALESCE(SUM(target_pct), 0) AS s FROM asset_subclasses"
        ).fetchone()
        if row is None or float(row["s"]) > 0.01:
            return
        n = conn.execute("SELECT COUNT(*) FROM asset_classes").fetchone()[0]
        if n == 0:
            return
        for cname, sname, pct, _so in ASSET_SUBCLASS_DEFAULT_ROWS:
            conn.execute(
                """UPDATE asset_subclasses SET target_pct = ?
                   WHERE name = ? AND asset_class_id = (
                       SELECT id FROM asset_classes WHERE name = ?
                   )""",
                (pct, sname, cname),
            )
        _reconcile_asset_class_targets_in_conn(conn)
        conn.commit()
    finally:
        conn.close()


ALLOCATION_SHEET_MIGRATION_ID = "allocation_user_sheet_2025"
# Подклассы «Товары» для золота: «Золото (Иностранный брокер)» / «Золото (Российский брокер)».
TOVARY_BROKER_SUBCLASS_NAMES_MIGRATION_ID = "subclass_tovary_broker_names_2026"
# Если уже применялась старая версия миграции с короткими именами — довести до «Золото (…)».
ZOLOTO_BROKER_PARENS_MIGRATION_ID = "subclass_zoloto_broker_parens_2026"
CRYPTO_SUBCLASS_CANONICAL_MIGRATION_ID = "subclass_crypto_canonical_2026"
CRYPTO_TWO_SUBCLASSES_MIGRATION_ID = "subclass_crypto_two_subclasses_2026"


def _normalize_tovary_zoloto_broker_subclass_names_in_conn(conn: sqlite3.Connection) -> None:
    """Единый вид: «Золото (Иностранный брокер)» / «Золото (Российский брокер)»."""
    tid = conn.execute(
        "SELECT id FROM asset_classes WHERE name = 'Товары' LIMIT 1"
    ).fetchone()
    if not tid:
        return
    tid_i = int(tid["id"])
    for old, new in (
        ("Золото (сегмент 1)", "Золото (Иностранный брокер)"),
        ("Золото (сегмент 2)", "Золото (Российский брокер)"),
        ("Иностранный брокер", "Золото (Иностранный брокер)"),
        ("Российский брокер", "Золото (Российский брокер)"),
    ):
        conn.execute(
            "UPDATE asset_subclasses SET name = ? WHERE asset_class_id = ? AND name = ?",
            (new, tid_i, old),
        )
    z = conn.execute(
        "SELECT id FROM asset_subclasses WHERE asset_class_id = ? AND name = 'Золото'",
        (tid_i,),
    ).fetchone()
    if z and conn.execute(
        "SELECT 1 FROM asset_subclasses WHERE asset_class_id = ? AND name = 'Золото (Иностранный брокер)'",
        (tid_i,),
    ).fetchone() is None:
        conn.execute(
            "UPDATE asset_subclasses SET name = 'Золото (Иностранный брокер)', target_pct = 2.325, sort_order = 1 WHERE id = ?",
            (int(z["id"]),),
        )


def apply_tovary_broker_subclass_names_migration() -> None:
    """Однократно: нормализация имён подклассов золота под «Товары»."""
    conn = get_conn()
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _schema_migrations (id TEXT PRIMARY KEY)"
        )
        if conn.execute(
            "SELECT 1 FROM _schema_migrations WHERE id = ?",
            (TOVARY_BROKER_SUBCLASS_NAMES_MIGRATION_ID,),
        ).fetchone():
            return
        _normalize_tovary_zoloto_broker_subclass_names_in_conn(conn)
        conn.execute(
            "INSERT INTO _schema_migrations (id) VALUES (?)",
            (TOVARY_BROKER_SUBCLASS_NAMES_MIGRATION_ID,),
        )
        conn.commit()
    finally:
        conn.close()


def apply_zoloto_broker_parens_subclass_migration() -> None:
    """Однократно: после коротких имён «… брокер» → «Золото (… брокер)»."""
    conn = get_conn()
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _schema_migrations (id TEXT PRIMARY KEY)"
        )
        if conn.execute(
            "SELECT 1 FROM _schema_migrations WHERE id = ?",
            (ZOLOTO_BROKER_PARENS_MIGRATION_ID,),
        ).fetchone():
            return
        _normalize_tovary_zoloto_broker_subclass_names_in_conn(conn)
        conn.execute(
            "INSERT INTO _schema_migrations (id) VALUES (?)",
            (ZOLOTO_BROKER_PARENS_MIGRATION_ID,),
        )
        conn.commit()
    finally:
        conn.close()


def apply_crypto_subclass_canonical_migration() -> None:
    """
    Однократно: привести крипто-подклассы к единому виду «Прочая криптовалюта».
    - объединяет дубликаты «Прочие крипто» / «Прочая криптовалюты»;
    - переносит ссылки из transactions/instruments;
    - удаляет тикерные подклассы (Bitcoin/Ethereum/...) после переноса.
    """
    conn = get_conn()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS _schema_migrations (id TEXT PRIMARY KEY)")
        if conn.execute(
            "SELECT 1 FROM _schema_migrations WHERE id = ?",
            (CRYPTO_SUBCLASS_CANONICAL_MIGRATION_ID,),
        ).fetchone():
            return

        crypto_cls = conn.execute(
            "SELECT id FROM asset_classes WHERE name = 'Криптовалюта' LIMIT 1"
        ).fetchone()
        if crypto_cls is None:
            conn.execute(
                "INSERT INTO _schema_migrations (id) VALUES (?)",
                (CRYPTO_SUBCLASS_CANONICAL_MIGRATION_ID,),
            )
            conn.commit()
            return

        crypto_class_id = int(crypto_cls["id"])
        canonical_name = "Прочая криптовалюта"

        # Сначала нормализуем явные варианты имени дубликата.
        for old_name in ("Прочая криптовалюты", "Прочие крипто"):
            conn.execute(
                "UPDATE asset_subclasses SET name = ? WHERE asset_class_id = ? AND name = ?",
                (canonical_name, crypto_class_id, old_name),
            )

        rows = conn.execute(
            """SELECT id, target_pct, sort_order
               FROM asset_subclasses
               WHERE asset_class_id = ? AND name = ?
               ORDER BY id""",
            (crypto_class_id, canonical_name),
        ).fetchall()

        canonical_id: Optional[int] = None
        if rows:
            canonical_id = int(rows[0]["id"])
            if len(rows) > 1:
                merged_target = round(sum(float(r["target_pct"]) for r in rows), 3)
                best_sort = min(int(r["sort_order"]) for r in rows)
                dup_ids = [int(r["id"]) for r in rows[1:]]
                q_marks = ",".join("?" for _ in dup_ids)
                conn.execute(
                    f"UPDATE transactions SET asset_subclass_id = ? WHERE asset_subclass_id IN ({q_marks})",
                    (canonical_id, *dup_ids),
                )
                conn.execute(
                    f"UPDATE instruments SET asset_subclass_id = ? WHERE asset_subclass_id IN ({q_marks})",
                    (canonical_id, *dup_ids),
                )
                conn.execute(
                    f"DELETE FROM asset_subclasses WHERE id IN ({q_marks})",
                    tuple(dup_ids),
                )
                conn.execute(
                    "UPDATE asset_subclasses SET target_pct = ?, sort_order = ? WHERE id = ?",
                    (merged_target, best_sort, canonical_id),
                )
        else:
            inserted = conn.execute(
                """INSERT INTO asset_subclasses (asset_class_id, name, target_pct, sort_order)
                   VALUES (?, ?, ?, ?)""",
                (crypto_class_id, canonical_name, 0.0, 1),
            )
            canonical_id = int(inserted.lastrowid)

        ticker_named_crypto = ("Bitcoin", "Ethereum", "Solana", "Avalanche", "BNB", "Proton")
        for n in ticker_named_crypto:
            row = conn.execute(
                "SELECT id, target_pct FROM asset_subclasses WHERE asset_class_id = ? AND name = ? LIMIT 1",
                (crypto_class_id, n),
            ).fetchone()
            if row is None:
                continue
            sid = int(row["id"])
            pct = float(row["target_pct"] or 0.0)
            conn.execute(
                "UPDATE transactions SET asset_subclass_id = ? WHERE asset_subclass_id = ?",
                (canonical_id, sid),
            )
            conn.execute(
                "UPDATE instruments SET asset_subclass_id = ? WHERE asset_subclass_id = ?",
                (canonical_id, sid),
            )
            conn.execute("DELETE FROM asset_subclasses WHERE id = ?", (sid,))
            conn.execute(
                "UPDATE asset_subclasses SET target_pct = ROUND(target_pct + ?, 3) WHERE id = ?",
                (pct, canonical_id),
            )

        conn.execute(
            "UPDATE asset_subclasses SET name = ?, sort_order = 1 WHERE id = ?",
            (canonical_name, canonical_id),
        )
        _reconcile_asset_class_targets_in_conn(conn)
        conn.execute(
            "INSERT INTO _schema_migrations (id) VALUES (?)",
            (CRYPTO_SUBCLASS_CANONICAL_MIGRATION_ID,),
        )
        conn.commit()
    finally:
        conn.close()


def apply_crypto_two_subclasses_migration() -> None:
    """
    Однократно: привести крипто-класс к 2 подклассам:
    - BTC+ETH
    - Прочая криптовалюта
    Любые остальные крипто-подклассы объединяются в «Прочая криптовалюта».
    """
    conn = get_conn()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS _schema_migrations (id TEXT PRIMARY KEY)")
        if conn.execute(
            "SELECT 1 FROM _schema_migrations WHERE id = ?",
            (CRYPTO_TWO_SUBCLASSES_MIGRATION_ID,),
        ).fetchone():
            return

        cls = conn.execute(
            "SELECT id FROM asset_classes WHERE name = 'Криптовалюта' LIMIT 1"
        ).fetchone()
        if cls is None:
            conn.execute(
                "INSERT INTO _schema_migrations (id) VALUES (?)",
                (CRYPTO_TWO_SUBCLASSES_MIGRATION_ID,),
            )
            conn.commit()
            return
        crypto_class_id = int(cls["id"])

        # Нормализуем старые варианты названий before grouping.
        for old in ("Прочие крипто", "Прочая криптовалюты"):
            conn.execute(
                "UPDATE asset_subclasses SET name = 'Прочая криптовалюта' WHERE asset_class_id = ? AND name = ?",
                (crypto_class_id, old),
            )

        def _ensure_row(name: str, sort_order: int, default_pct: float) -> int:
            rows = conn.execute(
                """SELECT id, target_pct
                   FROM asset_subclasses
                   WHERE asset_class_id = ? AND name = ?
                   ORDER BY id""",
                (crypto_class_id, name),
            ).fetchall()
            if rows:
                sid = int(rows[0]["id"])
                if len(rows) > 1:
                    dup_ids = [int(r["id"]) for r in rows[1:]]
                    q_marks = ",".join("?" for _ in dup_ids)
                    merged_pct = round(sum(float(r["target_pct"] or 0.0) for r in rows), 3)
                    conn.execute(
                        f"UPDATE transactions SET asset_subclass_id = ? WHERE asset_subclass_id IN ({q_marks})",
                        (sid, *dup_ids),
                    )
                    conn.execute(
                        f"UPDATE instruments SET asset_subclass_id = ? WHERE asset_subclass_id IN ({q_marks})",
                        (sid, *dup_ids),
                    )
                    conn.execute(
                        f"DELETE FROM asset_subclasses WHERE id IN ({q_marks})",
                        tuple(dup_ids),
                    )
                    conn.execute(
                        "UPDATE asset_subclasses SET target_pct = ? WHERE id = ?",
                        (merged_pct, sid),
                    )
                conn.execute(
                    "UPDATE asset_subclasses SET sort_order = ? WHERE id = ?",
                    (sort_order, sid),
                )
                return sid

            cur = conn.execute(
                """INSERT INTO asset_subclasses (asset_class_id, name, target_pct, sort_order)
                   VALUES (?, ?, ?, ?)""",
                (crypto_class_id, name, default_pct, sort_order),
            )
            return int(cur.lastrowid)

        btc_eth_id = _ensure_row("BTC+ETH", 1, 4.2)
        other_id = _ensure_row("Прочая криптовалюта", 2, 2.8)

        # Старые "по тикерам" названия: BTC/ETH -> BTC+ETH, остальные -> Прочая.
        legacy_to_target = {
            "Bitcoin": btc_eth_id,
            "Ethereum": btc_eth_id,
            "Solana": other_id,
            "Avalanche": other_id,
            "BNB": other_id,
            "Proton": other_id,
        }
        for legacy_name, target_id in legacy_to_target.items():
            row = conn.execute(
                "SELECT id, target_pct FROM asset_subclasses WHERE asset_class_id = ? AND name = ? LIMIT 1",
                (crypto_class_id, legacy_name),
            ).fetchone()
            if row is None:
                continue
            sid = int(row["id"])
            pct = float(row["target_pct"] or 0.0)
            conn.execute(
                "UPDATE transactions SET asset_subclass_id = ? WHERE asset_subclass_id = ?",
                (target_id, sid),
            )
            conn.execute(
                "UPDATE instruments SET asset_subclass_id = ? WHERE asset_subclass_id = ?",
                (target_id, sid),
            )
            conn.execute(
                "UPDATE asset_subclasses SET target_pct = ROUND(target_pct + ?, 3) WHERE id = ?",
                (pct, target_id),
            )
            conn.execute("DELETE FROM asset_subclasses WHERE id = ?", (sid,))

        # По тикерам явно выравниваем ссылки на нужный подкласс.
        conn.execute(
            """UPDATE transactions
               SET asset_subclass_id = ?
               WHERE UPPER(ticker) IN ('BTC', 'ETH')
                 AND asset_subclass_id IN (SELECT id FROM asset_subclasses WHERE asset_class_id = ?)""",
            (btc_eth_id, crypto_class_id),
        )
        conn.execute(
            """UPDATE transactions
               SET asset_subclass_id = ?
               WHERE UPPER(ticker) NOT IN ('BTC', 'ETH')
                 AND asset_subclass_id IN (SELECT id FROM asset_subclasses WHERE asset_class_id = ?)""",
            (other_id, crypto_class_id),
        )
        conn.execute(
            """UPDATE instruments
               SET asset_subclass_id = ?
               WHERE UPPER(ticker) IN ('BTC', 'ETH')
                 AND asset_subclass_id IN (SELECT id FROM asset_subclasses WHERE asset_class_id = ?)""",
            (btc_eth_id, crypto_class_id),
        )
        conn.execute(
            """UPDATE instruments
               SET asset_subclass_id = ?
               WHERE UPPER(ticker) NOT IN ('BTC', 'ETH')
                 AND asset_subclass_id IN (SELECT id FROM asset_subclasses WHERE asset_class_id = ?)""",
            (other_id, crypto_class_id),
        )

        # Любые прочие крипто-подклассы объединяем в «Прочая криптовалюта».
        extra_rows = conn.execute(
            """SELECT id, target_pct
               FROM asset_subclasses
               WHERE asset_class_id = ?
                 AND id NOT IN (?, ?)
               ORDER BY id""",
            (crypto_class_id, btc_eth_id, other_id),
        ).fetchall()
        for r in extra_rows:
            sid = int(r["id"])
            pct = float(r["target_pct"] or 0.0)
            conn.execute(
                "UPDATE transactions SET asset_subclass_id = ? WHERE asset_subclass_id = ?",
                (other_id, sid),
            )
            conn.execute(
                "UPDATE instruments SET asset_subclass_id = ? WHERE asset_subclass_id = ?",
                (other_id, sid),
            )
            conn.execute(
                "UPDATE asset_subclasses SET target_pct = ROUND(target_pct + ?, 3) WHERE id = ?",
                (pct, other_id),
            )
            conn.execute("DELETE FROM asset_subclasses WHERE id = ?", (sid,))

        conn.execute("UPDATE asset_subclasses SET name = 'BTC+ETH', sort_order = 1 WHERE id = ?", (btc_eth_id,))
        conn.execute(
            "UPDATE asset_subclasses SET name = 'Прочая криптовалюта', sort_order = 2 WHERE id = ?",
            (other_id,),
        )
        _reconcile_asset_class_targets_in_conn(conn)
        conn.execute(
            "INSERT INTO _schema_migrations (id) VALUES (?)",
            (CRYPTO_TWO_SUBCLASSES_MIGRATION_ID,),
        )
        conn.commit()
    finally:
        conn.close()


def apply_allocation_user_sheet_migration() -> None:
    """
    Однократно: переименования, новые подклассы, целевые % по таблице пользователя.
    """
    conn = get_conn()
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _schema_migrations (id TEXT PRIMARY KEY)"
        )
        if conn.execute(
            "SELECT 1 FROM _schema_migrations WHERE id = ?",
            (ALLOCATION_SHEET_MIGRATION_ID,),
        ).fetchone():
            return

        conn.execute(
            "UPDATE asset_subclasses SET name = 'Весь мир кроме США' "
            "WHERE name = 'Недвижимость весь мир кроме США'"
        )
        _normalize_tovary_zoloto_broker_subclass_names_in_conn(conn)

        id_by = {
            r["name"]: int(r["id"])
            for r in conn.execute("SELECT id, name FROM asset_classes")
        }
        for cname, sname, pct, so in ASSET_SUBCLASS_DEFAULT_ROWS:
            cid = id_by.get(cname)
            if cid is None:
                continue
            row = conn.execute(
                "SELECT id FROM asset_subclasses WHERE asset_class_id = ? AND name = ?",
                (cid, sname),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE asset_subclasses SET target_pct = ?, sort_order = ? WHERE id = ?",
                    (pct, so, int(row["id"])),
                )
            else:
                conn.execute(
                    """INSERT INTO asset_subclasses (asset_class_id, name, target_pct, sort_order)
                       VALUES (?, ?, ?, ?)""",
                    (cid, sname, pct, so),
                )

        conn.execute("UPDATE asset_subclasses SET target_pct = 0 WHERE name = 'Прочие крипто'")

        _reconcile_asset_class_targets_in_conn(conn)

        conn.execute(
            "INSERT INTO _schema_migrations (id) VALUES (?)",
            (ALLOCATION_SHEET_MIGRATION_ID,),
        )
        conn.commit()
    finally:
        conn.close()


# --- Asset classes ---
def list_asset_classes() -> List[AssetClass]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, name, target_pct, sort_order FROM asset_classes ORDER BY sort_order"
        ).fetchall()
        return [AssetClass(r["id"], r["name"], r["target_pct"], r["sort_order"]) for r in rows]
    finally:
        conn.close()


def list_asset_subclasses() -> List[AssetSubclass]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT id, asset_class_id, name, target_pct, sort_order
               FROM asset_subclasses ORDER BY asset_class_id, sort_order"""
        ).fetchall()
        return [
            AssetSubclass(r["id"], r["asset_class_id"], r["name"], r["target_pct"], r["sort_order"])
            for r in rows
        ]
    finally:
        conn.close()


def update_asset_subclass_target(subclass_id: int, target_pct: float):
    pct = round(float(target_pct), 3)
    conn = get_conn()
    try:
        conn.execute("UPDATE asset_subclasses SET target_pct = ? WHERE id = ?", (pct, subclass_id))
        _reconcile_asset_class_targets_in_conn(conn)
        conn.commit()
    finally:
        conn.close()


# --- Instruments (provider mapping) ---
def get_instrument_provider(ticker: str) -> Optional[tuple]:
    """Returns (provider, provider_symbol) or None if not set."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT provider, provider_symbol FROM instruments WHERE ticker = ?", (ticker.upper(),)
        ).fetchone()
        if row:
            return (row["provider"], row["provider_symbol"])
        return None
    finally:
        conn.close()


def set_instrument_provider(ticker: str, provider: str, provider_symbol: Optional[str] = None):
    conn = get_conn()
    try:
        conn.execute(
            """INSERT INTO instruments (ticker, provider, provider_symbol)
               VALUES (?, ?, ?) ON CONFLICT(ticker) DO UPDATE SET provider=excluded.provider, provider_symbol=excluded.provider_symbol""",
            (ticker.upper(), provider, provider_symbol or ""),
        )
        conn.commit()
    finally:
        conn.close()


def get_instrument_asset_subclass(ticker: str) -> Optional[int]:
    """Подкласс из настроек тикера (instruments), если задан."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT asset_subclass_id FROM instruments WHERE ticker = ?",
            (ticker.upper(),),
        ).fetchone()
        if row and row["asset_subclass_id"] is not None:
            return int(row["asset_subclass_id"])
        return None
    finally:
        conn.close()


def set_instrument_asset_subclass(ticker: str, asset_subclass_id: int):
    """Сохранить подкласс для тикера (upsert instruments, не затирая provider при обновлении)."""
    from app.services.prices import _detect_provider

    ticker = ticker.strip().upper()
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT provider, provider_symbol FROM instruments WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE instruments SET asset_subclass_id = ? WHERE ticker = ?",
                (asset_subclass_id, ticker),
            )
        else:
            prov, psym = _detect_provider(ticker)
            conn.execute(
                """INSERT INTO instruments (ticker, provider, provider_symbol, asset_subclass_id)
                   VALUES (?, ?, ?, ?)""",
                (ticker, prov, psym or "", asset_subclass_id),
            )
        conn.commit()
    finally:
        conn.close()


def is_ticker_buy_blocked(ticker: str) -> bool:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT buy_blocked FROM instruments WHERE ticker = ?",
            (ticker.upper(),),
        ).fetchone()
        return bool(row and int(row["buy_blocked"] or 0) == 1)
    finally:
        conn.close()


def list_buy_blocked_tickers() -> List[str]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT ticker FROM instruments WHERE COALESCE(buy_blocked, 0) = 1 ORDER BY ticker"
        ).fetchall()
        return [str(r["ticker"]).upper() for r in rows]
    finally:
        conn.close()


def set_ticker_buy_blocked(ticker: str, blocked: bool) -> None:
    """Пометить тикер как недоступный для покупки в ребалансировке."""
    from app.services.prices import _detect_provider

    t = ticker.strip().upper()
    if not t:
        return
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT ticker FROM instruments WHERE ticker = ?",
            (t,),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE instruments SET buy_blocked = ? WHERE ticker = ?",
                (1 if blocked else 0, t),
            )
        else:
            prov, psym = _detect_provider(t)
            conn.execute(
                """INSERT INTO instruments (ticker, provider, provider_symbol, buy_blocked)
                   VALUES (?, ?, ?, ?)""",
                (t, prov, psym or "", 1 if blocked else 0),
            )
        conn.commit()
    finally:
        conn.close()


def get_default_storage_id() -> int:
    """Первый id по sort_order (без пункта «По умолчанию» — только список DEFAULT + пользовательские)."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id FROM storages ORDER BY sort_order, id LIMIT 1"
        ).fetchone()
        if row:
            return int(row["id"])
        _ensure_seed_storages(conn)
        row = conn.execute(
            "SELECT id FROM storages ORDER BY sort_order, id LIMIT 1"
        ).fetchone()
        if row:
            return int(row["id"])
        raise RuntimeError("Не удалось инициализировать таблицу storages")
    finally:
        conn.close()


def list_storages() -> List[Storage]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, name, sort_order FROM storages ORDER BY sort_order, name"
        ).fetchall()
        if not rows:
            get_default_storage_id()
            rows = conn.execute(
                "SELECT id, name, sort_order FROM storages ORDER BY sort_order, name"
            ).fetchall()
        return [Storage(int(r["id"]), r["name"], int(r["sort_order"])) for r in rows]
    finally:
        conn.close()


def add_storage(name: str) -> int:
    """Добавить место хранения; при дубликате имени вернуть существующий id."""
    name = name.strip()
    if not name:
        raise ValueError("Название места хранения не может быть пустым")
    conn = get_conn()
    try:
        row = conn.execute("SELECT id FROM storages WHERE name = ?", (name,)).fetchone()
        if row:
            return int(row["id"])
        mx = conn.execute("SELECT COALESCE(MAX(sort_order), -1) AS m FROM storages").fetchone()
        nxt = int(mx["m"]) + 1 if mx else 0
        try:
            cur = conn.execute(
                "INSERT INTO storages (name, sort_order) VALUES (?, ?)",
                (name, nxt),
            )
            conn.commit()
            return int(cur.lastrowid)
        except sqlite3.IntegrityError:
            conn.rollback()
            row = conn.execute("SELECT id FROM storages WHERE name = ?", (name,)).fetchone()
            if row:
                return int(row["id"])
            raise
    finally:
        conn.close()


def list_distinct_tickers() -> List[str]:
    """Все тикеры из сделок и из справочника instruments."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT ticker FROM (
                 SELECT DISTINCT ticker FROM transactions
                 UNION
                 SELECT ticker FROM instruments
               ) ORDER BY ticker"""
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


# --- Historical quotes cache ---
def upsert_historical_quote(
    ticker: str,
    quote_date: str,
    provider: str,
    provider_symbol: Optional[str],
    price: Optional[float],
    currency: str,
) -> None:
    """Upsert one daily historical quote row."""
    t = ticker.strip().upper()
    conn = get_conn()
    try:
        conn.execute(
            """INSERT INTO historical_quotes (
                   ticker, quote_date, provider, provider_symbol, price, currency, updated_at
               )
               VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(ticker, quote_date) DO UPDATE SET
                   provider = excluded.provider,
                   provider_symbol = excluded.provider_symbol,
                   price = excluded.price,
                   currency = excluded.currency,
                   updated_at = datetime('now')""",
            (t, quote_date, provider, provider_symbol or "", price, currency.upper()),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_historical_quotes_bulk(rows: List[tuple]) -> None:
    """
    Bulk upsert historical quotes.
    Row format: (ticker, quote_date, provider, provider_symbol, price, currency)
    """
    if not rows:
        return
    conn = get_conn()
    try:
        conn.executemany(
            """INSERT INTO historical_quotes (
                   ticker, quote_date, provider, provider_symbol, price, currency, updated_at
               )
               VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(ticker, quote_date) DO UPDATE SET
                   provider = excluded.provider,
                   provider_symbol = excluded.provider_symbol,
                   price = excluded.price,
                   currency = excluded.currency,
                   updated_at = datetime('now')""",
            [
                (
                    str(t).strip().upper(),
                    str(d),
                    str(p),
                    (ps or ""),
                    (None if pr is None else float(pr)),
                    str(c).upper(),
                )
                for t, d, p, ps, pr, c in rows
            ],
        )
        conn.commit()
    finally:
        conn.close()


def list_cached_historical_quotes(
    ticker: str,
    date_from: str,
    date_to: str,
) -> List[tuple]:
    """
    Return cached historical quotes for ticker in inclusive date range.
    Output rows: (quote_date, price, currency, provider, provider_symbol)
    """
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT quote_date, price, currency, provider, provider_symbol
               FROM historical_quotes
               WHERE ticker = ? AND quote_date >= ? AND quote_date <= ?
               ORDER BY quote_date""",
            (ticker.strip().upper(), date_from, date_to),
        ).fetchall()
        return [
            (
                str(r["quote_date"]),
                (None if r["price"] is None else float(r["price"])),
                str(r["currency"]).upper(),
                str(r["provider"]),
                (r["provider_symbol"] or ""),
            )
            for r in rows
        ]
    finally:
        conn.close()


# --- Transactions ---
def add_transaction(
    ticker: str,
    amount: float,
    asset_subclass_id: int,
    currency: Optional[str] = None,
    storage_id: Optional[int] = None,
) -> int:
    sid = storage_id if storage_id is not None else get_default_storage_id()
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO transactions (ticker, amount, asset_subclass_id, currency, storage_id)
               VALUES (?, ?, ?, ?, ?)""",
            (ticker.strip().upper(), amount, asset_subclass_id, currency, sid),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def list_transactions() -> List[Transaction]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT t.id, t.ticker, t.amount, t.asset_subclass_id, t.currency, t.created_at,
                      t.storage_id, s.name AS storage_name
               FROM transactions t
               LEFT JOIN storages s ON s.id = t.storage_id
               ORDER BY t.created_at DESC"""
        ).fetchall()
        default_sid = get_default_storage_id()
        return [
            Transaction(
                r["id"],
                r["ticker"],
                r["amount"],
                r["asset_subclass_id"],
                r["currency"],
                r["created_at"],
                int(r["storage_id"]) if r["storage_id"] is not None else default_sid,
                r["storage_name"],
            )
            for r in rows
        ]
    finally:
        conn.close()


def get_subclass_id_by_name(name: str) -> Optional[int]:
    """ID подкласса по точному имени (как в seed)."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id FROM asset_subclasses WHERE name = ? LIMIT 1",
            (name,),
        ).fetchone()
        return int(row["id"]) if row else None
    finally:
        conn.close()


def get_first_subclass_id() -> int:
    """Минимальный id подкласса — последний запасной вариант."""
    conn = get_conn()
    try:
        row = conn.execute("SELECT MIN(id) AS m FROM asset_subclasses").fetchone()
        if row and row["m"] is not None:
            return int(row["m"])
        raise RuntimeError("asset_subclasses is empty")
    finally:
        conn.close()


def get_latest_transaction_subclass(ticker: str) -> Optional[int]:
    conn = get_conn()
    try:
        row = conn.execute(
            """SELECT asset_subclass_id FROM transactions
               WHERE ticker = ? ORDER BY created_at DESC LIMIT 1""",
            (ticker.upper(),),
        ).fetchone()
        return int(row["asset_subclass_id"]) if row else None
    finally:
        conn.close()


def resolve_asset_subclass_id(ticker: str) -> int:
    """
    Подкласс для сделок/отображения: настройка → последняя сделка → эвристика по тикеру → дефолт.
    Всегда возвращает валидный id.
    """
    from app.services.subclass_inference import DEFAULT_SUBCLASS_NAME, infer_subclass_name

    t = ticker.upper().strip()
    cfg = get_instrument_asset_subclass(t)
    if cfg is not None:
        return cfg
    last = get_latest_transaction_subclass(t)
    if last is not None:
        return last
    guessed_name = infer_subclass_name(t)
    if guessed_name:
        sid = get_subclass_id_by_name(guessed_name)
        if sid is not None:
            return sid
    fallback = get_subclass_id_by_name(DEFAULT_SUBCLASS_NAME)
    if fallback is not None:
        return fallback
    return get_first_subclass_id()


def get_asset_subclass_for_ticker(ticker: str) -> int:
    """Подкласс для тикера (всегда задан: ручные настройки, история, авто или дефолт)."""
    return resolve_asset_subclass_id(ticker)


# --- Aggregated positions (sum of transactions per ticker + storage) ---
def list_aggregated_positions() -> List[Position]:
    """Сумма по паре (тикер, место хранения); подкласс — как у тикера в целом. Остаток > 0."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT t.ticker, t.storage_id, MAX(s.name) AS storage_name,
                      SUM(t.amount) AS total,
                      COALESCE(
                        (SELECT i.asset_subclass_id FROM instruments i
                         WHERE i.ticker = t.ticker AND i.asset_subclass_id IS NOT NULL),
                        (SELECT asset_subclass_id FROM transactions t2
                         WHERE t2.ticker = t.ticker ORDER BY created_at DESC LIMIT 1)
                      ) AS asset_subclass_id
               FROM transactions t
               LEFT JOIN storages s ON s.id = t.storage_id
               GROUP BY t.ticker, t.storage_id
               HAVING total > 0
               ORDER BY t.ticker, storage_name"""
        ).fetchall()
        out = []
        for r in rows:
            sid_sub = r["asset_subclass_id"]
            if sid_sub is None:
                sid_sub = resolve_asset_subclass_id(r["ticker"])
            st_id = int(r["storage_id"]) if r["storage_id"] is not None else get_default_storage_id()
            st_name = (r["storage_name"] or "").strip() or "—"
            out.append(
                Position(
                    0,
                    r["ticker"],
                    r["total"],
                    int(sid_sub),
                    None,
                    st_id,
                    st_name,
                )
            )
        return out
    finally:
        conn.close()


def list_positions() -> List[Position]:
    """Позиции по паре (тикер + место хранения) — продажа, вкладка «По местам»."""
    return list_aggregated_positions()


def list_positions_by_ticker() -> List[Position]:
    """Одна строка на тикер: суммарное количество по всем местам хранения (для сводной таблицы)."""
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT t.ticker,
                      SUM(t.amount) AS total,
                      COALESCE(
                        (SELECT i.asset_subclass_id FROM instruments i
                         WHERE i.ticker = t.ticker AND i.asset_subclass_id IS NOT NULL),
                        (SELECT asset_subclass_id FROM transactions t2
                         WHERE t2.ticker = t.ticker ORDER BY created_at DESC LIMIT 1)
                      ) AS asset_subclass_id
               FROM transactions t
               GROUP BY t.ticker
               HAVING total > 0
               ORDER BY t.ticker"""
        ).fetchall()
        out: List[Position] = []
        for r in rows:
            sid_sub = r["asset_subclass_id"]
            if sid_sub is None:
                sid_sub = resolve_asset_subclass_id(r["ticker"])
            out.append(
                Position(
                    0,
                    r["ticker"],
                    float(r["total"]),
                    int(sid_sub),
                    None,
                    0,
                    "",
                )
            )
        return out
    finally:
        conn.close()
