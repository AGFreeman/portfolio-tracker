"""SQLite: init, read/write portfolio and asset classes."""
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from app.models import (
    AssetClass,
    AssetSubclass,
    CashFlow,
    Position,
    Storage,
    Transaction,
)

DB_PATH = os.environ.get("PORTFOLIO_DB", str(Path(__file__).resolve().parent.parent / "data" / "portfolio.db"))
_CASH_FLOWS_TABLE = "cash_flows"
_LEGACY_CASH_FLOWS_TABLE = "portfolio_cash_flows"

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
    ("Акции", "Акции РФ", 12.090, 3),
    ("Акции", "Акции Китая", 12.090, 4),
    ("Акции", "Акции развивающихся стран кроме Китая", 6.510, 5),
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
_PORTFOLIO_TABLE = "portfolio"


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


def _ensure_portfolio_table(conn: sqlite3.Connection) -> None:
    """Текущие позиции по паре (тикер, место хранения) + флаги blocked/main."""
    has_legacy = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'portfolio_instrument_storages' LIMIT 1"
    ).fetchone()
    has_portfolio = conn.execute(
        f"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '{_PORTFOLIO_TABLE}' LIMIT 1"
    ).fetchone()
    if has_legacy and not has_portfolio:
        conn.execute(f"ALTER TABLE portfolio_instrument_storages RENAME TO {_PORTFOLIO_TABLE}")
        conn.commit()
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_PORTFOLIO_TABLE} (
            ticker TEXT NOT NULL,
            storage_id INTEGER NOT NULL REFERENCES storages(id),
            blocked INTEGER NOT NULL DEFAULT 0,
            main INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (ticker, storage_id)
        )
        """
    )
    conn.commit()
    cols = {
        str(r["name"]).lower()
        for r in conn.execute(f"PRAGMA table_info({_PORTFOLIO_TABLE})").fetchall()
    }
    if "main" not in cols:
        conn.execute(f"ALTER TABLE {_PORTFOLIO_TABLE} ADD COLUMN main INTEGER NOT NULL DEFAULT 0")
        conn.commit()


def _sync_portfolio_table(conn: sqlite3.Connection) -> None:
    """Синхронизировать таблицу блокировок со списком текущих позиций (> 0)."""
    conn.execute(
        f"""
        INSERT INTO {_PORTFOLIO_TABLE} (ticker, storage_id, blocked, main)
        SELECT p.ticker, p.storage_id, 0,
               COALESCE((SELECT MAX(COALESCE(b2.main, 0))
                         FROM {_PORTFOLIO_TABLE} b2
                         WHERE b2.ticker = p.ticker), 0)
        FROM (
            SELECT t.ticker AS ticker, t.storage_id AS storage_id, SUM(t.amount) AS total
            FROM transactions t
            GROUP BY t.ticker, t.storage_id
            HAVING total > 0
        ) p
        LEFT JOIN {_PORTFOLIO_TABLE} b
               ON b.ticker = p.ticker AND b.storage_id = p.storage_id
        WHERE b.ticker IS NULL
        """
    )
    conn.execute(
        f"""
        DELETE FROM {_PORTFOLIO_TABLE}
        WHERE (ticker, storage_id) NOT IN (
            SELECT t.ticker, t.storage_id
            FROM transactions t
            GROUP BY t.ticker, t.storage_id
            HAVING SUM(t.amount) > 0
        )
        """
    )
    conn.commit()


def _migrate_legacy_ticker_blocks_to_storage(conn: sqlite3.Connection) -> None:
    """Перенести старые флаги блокировки (в instruments) в таблицу по местам хранения."""
    cols = {
        str(r["name"]).lower()
        for r in conn.execute("PRAGMA table_info(instruments)").fetchall()
    }
    legacy_col = None
    if "blocked" in cols:
        legacy_col = "blocked"
    elif "buy_blocked" in cols:
        legacy_col = "buy_blocked"
    if legacy_col is None:
        return
    rows = conn.execute(
        f"SELECT ticker FROM instruments WHERE COALESCE({legacy_col}, 0) = 1 ORDER BY ticker"
    ).fetchall()
    for r in rows:
        ticker = str(r["ticker"]).upper()
        conn.execute(
            """
            UPDATE portfolio
            SET blocked = 1, updated_at = datetime('now')
            WHERE ticker = ?
            """,
            (ticker,),
        )
    conn.commit()


def _migrate_legacy_instruments_main_to_portfolio(conn: sqlite3.Connection) -> None:
    """Перенести старый instruments.main в portfolio.main (по тикеру)."""
    cols = {
        str(r["name"]).lower()
        for r in conn.execute("PRAGMA table_info(instruments)").fetchall()
    }
    if "main" not in cols:
        return
    conn.execute(
        f"""
        UPDATE {_PORTFOLIO_TABLE}
        SET main = COALESCE(
            (
                SELECT COALESCE(i.main, 0)
                FROM instruments i
                WHERE i.ticker = {_PORTFOLIO_TABLE}.ticker
                LIMIT 1
            ),
            main
        )
        WHERE ticker IN (
            SELECT ticker FROM instruments WHERE COALESCE(main, 0) = 1
        )
        """
    )
    conn.commit()


def _drop_legacy_block_columns_from_instruments(conn: sqlite3.Connection) -> None:
    """Удалить устаревшие колонки блокировок из instruments (blocked/buy_blocked)."""
    cols = [
        str(r["name"])
        for r in conn.execute("PRAGMA table_info(instruments)").fetchall()
    ]
    to_drop = [c for c in ("blocked", "buy_blocked") if c in cols]
    for col in to_drop:
        try:
            conn.execute(f"ALTER TABLE instruments DROP COLUMN {col}")
        except sqlite3.OperationalError:
            # Older SQLite may not support DROP COLUMN.
            # In that case we keep legacy column physically, but app logic no longer uses it.
            continue
        conn.commit()
        cols = [
            str(r["name"])
            for r in conn.execute("PRAGMA table_info(instruments)").fetchall()
        ]


def _ensure_data_dir():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)


def get_conn() -> sqlite3.Connection:
    _ensure_data_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _transactions_has_asset_subclass_column(conn: sqlite3.Connection) -> bool:
    cols = conn.execute("PRAGMA table_info(transactions)").fetchall()
    return any(str(r["name"]) == "asset_subclass_id" for r in cols)


def _instruments_has_asset_subclass_column(conn: sqlite3.Connection) -> bool:
    cols = conn.execute("PRAGMA table_info(instruments)").fetchall()
    return any(str(r["name"]) == "asset_subclass_id" for r in cols)


def add_cash_flow(amount: float, direction: str, currency: str, flow_date: str) -> int:
    """Запись о вводе/выводе денег; out хранится как отрицательный amount."""
    if float(amount) <= 0:
        raise ValueError("amount must be positive")
    d = (direction or "").strip().lower()
    if d not in ("in", "out"):
        raise ValueError("direction must be 'in' or 'out'")
    ccy = (currency or "RUB").strip().upper() or "RUB"
    fd = (flow_date or "").strip()[:10]
    if len(fd) < 10:
        raise ValueError("flow_date must be YYYY-MM-DD")
    conn = get_conn()
    try:
        signed_amount = float(amount) if d == "in" else -float(amount)
        cur = conn.execute(
            f"""INSERT INTO {_CASH_FLOWS_TABLE} (amount, currency, flow_date)
               VALUES (?, ?, ?)""",
            (signed_amount, ccy, fd),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def list_cash_flows() -> List[CashFlow]:
    conn = get_conn()
    try:
        rows = conn.execute(
            f"""SELECT id, amount, currency, flow_date
               FROM {_CASH_FLOWS_TABLE}
               ORDER BY flow_date DESC, id DESC"""
        ).fetchall()
        return [
            CashFlow(
                int(r["id"]),
                float(r["amount"]),
                str(r["currency"]),
                str(r["flow_date"]),
            )
            for r in rows
        ]
    finally:
        conn.close()


def delete_cash_flow(flow_id: int) -> None:
    conn = get_conn()
    try:
        conn.execute(f"DELETE FROM {_CASH_FLOWS_TABLE} WHERE id = ?", (int(flow_id),))
        conn.commit()
    finally:
        conn.close()


def init_db():
    conn = get_conn()
    try:
        conn.executescript(
            """
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
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                amount REAL NOT NULL,
                transaction_type TEXT NOT NULL DEFAULT 'trade',
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
            CREATE TABLE IF NOT EXISTS cash_flows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                flow_date TEXT NOT NULL,
                CHECK (amount != 0)
            );
        """
        )
        conn.commit()
        # Стартовые места хранения (IB, Тинькофф, кошельки…) + колонка storage_id у сделок
        _ensure_seed_storages(conn)
        _remove_legacy_default_named_storage(conn)
        _ensure_portfolio_table(conn)
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
        _sync_portfolio_table(conn)
        # transactions.transaction_type — тип операции: trade | transfer | split | bond_redemption | conversion_blocked
        try:
            conn.execute("SELECT transaction_type FROM transactions LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute(
                "ALTER TABLE transactions ADD COLUMN transaction_type TEXT NOT NULL DEFAULT 'trade'"
            )
            conn.commit()
        # instruments.asset_subclass_id — подкласс по тикеру (настройка пользователя)
        try:
            conn.execute("SELECT asset_subclass_id FROM instruments LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute("ALTER TABLE instruments ADD COLUMN asset_subclass_id INTEGER")
            conn.commit()
        _migrate_legacy_ticker_blocks_to_storage(conn)
        _migrate_legacy_instruments_main_to_portfolio(conn)
        _drop_legacy_block_columns_from_instruments(conn)
        # Rename legacy table if it still exists from older schema name.
        has_legacy_cf = conn.execute(
            f"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '{_LEGACY_CASH_FLOWS_TABLE}' LIMIT 1"
        ).fetchone()
        has_new_cf = conn.execute(
            f"SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '{_CASH_FLOWS_TABLE}' LIMIT 1"
        ).fetchone()
        if has_legacy_cf and not has_new_cf:
            conn.execute(f"ALTER TABLE {_LEGACY_CASH_FLOWS_TABLE} RENAME TO {_CASH_FLOWS_TABLE}")
            conn.commit()
        elif has_legacy_cf and has_new_cf:
            conn.execute(
                f"""
                INSERT INTO {_CASH_FLOWS_TABLE} (id, amount, currency, flow_date)
                SELECT p.id, p.amount, p.currency, p.flow_date
                FROM {_LEGACY_CASH_FLOWS_TABLE} p
                LEFT JOIN {_CASH_FLOWS_TABLE} c ON c.id = p.id
                WHERE c.id IS NULL
                """
            )
            conn.execute(f"DROP TABLE {_LEGACY_CASH_FLOWS_TABLE}")
            conn.commit()

        # cash_flows: signed amount only (out < 0), drop legacy columns direction/created_at.
        cf_cols = conn.execute("PRAGMA table_info(cash_flows)").fetchall()
        cf_col_names = {str(r["name"]) for r in cf_cols}
        if "direction" in cf_col_names or "created_at" in cf_col_names:
            conn.execute("ALTER TABLE cash_flows RENAME TO cash_flows_old")
            conn.execute(
                """
                CREATE TABLE cash_flows (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    flow_date TEXT NOT NULL,
                    CHECK (amount != 0)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO cash_flows (id, amount, currency, flow_date)
                SELECT
                    id,
                    CASE
                        WHEN LOWER(COALESCE(direction, 'in')) = 'out' THEN -ABS(amount)
                        ELSE ABS(amount)
                    END AS amount,
                    currency,
                    flow_date
                FROM cash_flows_old
                WHERE amount IS NOT NULL
                """
            )
            conn.execute("DROP TABLE cash_flows_old")
            conn.commit()
        # Migrate legacy positions table into transactions (one tx per position).
        has_positions = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'positions' LIMIT 1"
        ).fetchone()
        cur = conn.execute("SELECT COUNT(*) FROM transactions")
        if has_positions and cur.fetchone()[0] == 0:
            rows = conn.execute(
                "SELECT ticker, amount, asset_subclass_id FROM positions"
            ).fetchall()
            default_sid_row = conn.execute("SELECT id FROM storages ORDER BY id LIMIT 1").fetchone()
            default_sid = int(default_sid_row[0]) if default_sid_row else 1
            for r in rows:
                conn.execute(
                    "UPDATE instruments SET asset_subclass_id = ? WHERE ticker = ?",
                    (r[2], r[0]),
                )
                conn.execute(
                    """INSERT INTO transactions (ticker, amount, storage_id, transaction_type)
                       VALUES (?, ?, ?, 'trade')""",
                    (r[0], r[1], default_sid),
                )
            if rows:
                conn.commit()
        _sync_portfolio_table(conn)
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
REMOVE_LEGACY_EQUITY_SUBCLASSES_MIGRATION_ID = "subclass_remove_legacy_equity_2026"


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


def apply_remove_legacy_equity_subclasses_migration() -> None:
    """
    Однократно: убрать подклассы акций, которые больше не используются:
    - «Акции Еврозоны»
    - «Акции развивающихся стран»
    Их ссылки и целевая доля переносятся в «Акции развивающихся стран кроме Китая».
    """
    conn = get_conn()
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS _schema_migrations (id TEXT PRIMARY KEY)"
        )
        if conn.execute(
            "SELECT 1 FROM _schema_migrations WHERE id = ?",
            (REMOVE_LEGACY_EQUITY_SUBCLASSES_MIGRATION_ID,),
        ).fetchone():
            return

        eq_cls = conn.execute(
            "SELECT id FROM asset_classes WHERE name = 'Акции' LIMIT 1"
        ).fetchone()
        if eq_cls is None:
            conn.execute(
                "INSERT INTO _schema_migrations (id) VALUES (?)",
                (REMOVE_LEGACY_EQUITY_SUBCLASSES_MIGRATION_ID,),
            )
            conn.commit()
            return

        equity_class_id = int(eq_cls["id"])
        keep_name = "Акции развивающихся стран кроме Китая"
        keep_row = conn.execute(
            "SELECT id, target_pct FROM asset_subclasses WHERE asset_class_id = ? AND name = ? LIMIT 1",
            (equity_class_id, keep_name),
        ).fetchone()
        if keep_row is None:
            inserted = conn.execute(
                """INSERT INTO asset_subclasses (asset_class_id, name, target_pct, sort_order)
                   VALUES (?, ?, ?, ?)""",
                (equity_class_id, keep_name, 6.510, 5),
            )
            keep_id = int(inserted.lastrowid)
            keep_pct = 6.510
        else:
            keep_id = int(keep_row["id"])
            keep_pct = float(keep_row["target_pct"] or 0.0)

        remove_names = ("Акции Еврозоны", "Акции развивающихся стран")
        removed_rows = conn.execute(
            """SELECT id, target_pct
               FROM asset_subclasses
               WHERE asset_class_id = ?
                 AND name IN (?, ?)
               ORDER BY id""",
            (equity_class_id, *remove_names),
        ).fetchall()
        remove_ids = [int(r["id"]) for r in removed_rows if int(r["id"]) != keep_id]
        moved_pct = sum(
            float(r["target_pct"] or 0.0)
            for r in removed_rows
            if int(r["id"]) != keep_id
        )

        if remove_ids:
            q_marks = ",".join("?" for _ in remove_ids)
            conn.execute(
                f"UPDATE transactions SET asset_subclass_id = ? WHERE asset_subclass_id IN ({q_marks})",
                (keep_id, *remove_ids),
            )
            conn.execute(
                f"UPDATE instruments SET asset_subclass_id = ? WHERE asset_subclass_id IN ({q_marks})",
                (keep_id, *remove_ids),
            )
            conn.execute(
                f"DELETE FROM asset_subclasses WHERE id IN ({q_marks})",
                tuple(remove_ids),
            )
            conn.execute(
                "UPDATE asset_subclasses SET target_pct = ROUND(?, 3) WHERE id = ?",
                (keep_pct + moved_pct, keep_id),
            )

        for name, sort_order in (
            ("Акции США", 1),
            ("Акции развитых стран кроме США", 2),
            ("Акции РФ", 3),
            ("Акции Китая", 4),
            ("Акции развивающихся стран кроме Китая", 5),
        ):
            conn.execute(
                """UPDATE asset_subclasses
                   SET sort_order = ?
                   WHERE asset_class_id = ? AND name = ?""",
                (sort_order, equity_class_id, name),
            )

        _reconcile_asset_class_targets_in_conn(conn)
        conn.execute(
            "INSERT INTO _schema_migrations (id) VALUES (?)",
            (REMOVE_LEGACY_EQUITY_SUBCLASSES_MIGRATION_ID,),
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
    t = (ticker or "").upper().strip()
    if not t:
        return None
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT provider, provider_symbol FROM instruments WHERE ticker = ?", (t,)
        ).fetchone()
        if row:
            return (row["provider"], row["provider_symbol"])
        return None
    finally:
        conn.close()


def set_instrument_provider(ticker: str, provider: str, provider_symbol: Optional[str] = None):
    t = (ticker or "").upper().strip()
    if not t:
        return
    conn = get_conn()
    try:
        conn.execute(
            """INSERT INTO instruments (ticker, provider, provider_symbol)
               VALUES (?, ?, ?) ON CONFLICT(ticker) DO UPDATE SET provider=excluded.provider, provider_symbol=excluded.provider_symbol""",
            (t, provider, provider_symbol or ""),
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


def get_instrument_main_map(tickers: Sequence[str]) -> Dict[str, bool]:
    """
    Вернуть признак portfolio.main по набору тикеров.
    Для отсутствующих строк — False.
    """
    uniq = sorted({(t or "").strip().upper() for t in tickers if (t or "").strip()})
    if not uniq:
        return {}
    q_marks = ",".join(["?"] * len(uniq))
    conn = get_conn()
    try:
        _sync_portfolio_table(conn)
        rows = conn.execute(
            f"""SELECT ticker, MAX(COALESCE(main, 0)) AS main
                FROM {_PORTFOLIO_TABLE}
                WHERE ticker IN ({q_marks})
                GROUP BY ticker""",
            tuple(uniq),
        ).fetchall()
        out: Dict[str, bool] = {t: False for t in uniq}
        for r in rows:
            out[str(r["ticker"]).upper()] = bool(int(r["main"] or 0) == 1)
        return out
    finally:
        conn.close()


def set_ticker_main_flag(ticker: str, is_main: bool) -> None:
    """Установить флаг main для всех текущих мест хранения тикера."""
    t = ticker.strip().upper()
    if not t:
        return
    conn = get_conn()
    try:
        _sync_portfolio_table(conn)
        conn.execute(
            f"""
            UPDATE {_PORTFOLIO_TABLE}
            SET main = ?, updated_at = datetime('now')
            WHERE ticker = ?
            """,
            (1 if is_main else 0, t),
        )
        conn.commit()
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
        _sync_portfolio_table(conn)
        row = conn.execute(
            """
            SELECT 1
            FROM portfolio
            WHERE ticker = ? AND COALESCE(blocked, 0) = 1
            LIMIT 1
            """,
            (ticker.upper(),),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def list_buy_blocked_tickers(main_only: bool = False) -> List[str]:
    conn = get_conn()
    try:
        _sync_portfolio_table(conn)
        if main_only:
            rows = conn.execute(
                """
                SELECT DISTINCT ticker
                FROM portfolio
                WHERE COALESCE(blocked, 0) = 1
                  AND COALESCE(main, 0) = 1
                ORDER BY ticker
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT DISTINCT ticker
                FROM portfolio
                WHERE COALESCE(blocked, 0) = 1
                ORDER BY ticker
                """
            ).fetchall()
        return [str(r["ticker"]).upper() for r in rows]
    finally:
        conn.close()


def set_ticker_buy_blocked(ticker: str, blocked: bool) -> None:
    """Пометить все текущие места хранения тикера как (не)доступные для покупки."""
    t = ticker.strip().upper()
    if not t:
        return
    conn = get_conn()
    try:
        _sync_portfolio_table(conn)
        conn.execute(
            """
            UPDATE portfolio
            SET blocked = ?, updated_at = datetime('now')
            WHERE ticker = ?
            """,
            (1 if blocked else 0, t),
        )
        conn.commit()
    finally:
        conn.close()


def list_portfolio_blocks(main_only: bool = False) -> List[Dict[str, object]]:
    """Текущие позиции (тикер+место) и флаг блокировки покупки по каждой строке."""
    conn = get_conn()
    try:
        _sync_portfolio_table(conn)
        if main_only:
            rows = conn.execute(
                """
                SELECT b.ticker,
                       b.storage_id,
                       COALESCE(s.name, '—') AS storage_name,
                       COALESCE(b.blocked, 0) AS blocked
                FROM portfolio b
                LEFT JOIN storages s ON s.id = b.storage_id
                WHERE COALESCE(b.main, 0) = 1
                ORDER BY b.ticker, storage_name
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT b.ticker,
                       b.storage_id,
                       COALESCE(s.name, '—') AS storage_name,
                       COALESCE(b.blocked, 0) AS blocked
                FROM portfolio b
                LEFT JOIN storages s ON s.id = b.storage_id
                ORDER BY b.ticker, storage_name
                """
            ).fetchall()
        return [
            {
                "ticker": str(r["ticker"]).upper(),
                "storage_id": int(r["storage_id"]),
                "storage_name": str(r["storage_name"] or "—"),
                "blocked": int(r["blocked"] or 0) == 1,
            }
            for r in rows
        ]
    finally:
        conn.close()


def set_portfolio_blocked(ticker: str, storage_id: int, blocked: bool) -> None:
    """Поставить/снять блокировку покупки для пары (тикер, место хранения)."""
    t = ticker.strip().upper()
    sid = int(storage_id)
    conn = get_conn()
    try:
        _sync_portfolio_table(conn)
        conn.execute(
            """
            UPDATE portfolio
            SET blocked = ?, updated_at = datetime('now')
            WHERE ticker = ? AND storage_id = ?
            """,
            (1 if blocked else 0, t, sid),
        )
        conn.commit()
    finally:
        conn.close()


def list_portfolio_ticker_storage_blocks() -> List[Dict[str, object]]:
    """Backward-compatible alias; use list_portfolio_blocks()."""
    return list_portfolio_blocks()


def set_ticker_storage_buy_blocked(ticker: str, storage_id: int, blocked: bool) -> None:
    """Backward-compatible alias; use set_portfolio_blocked()."""
    set_portfolio_blocked(ticker=ticker, storage_id=storage_id, blocked=blocked)


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
    storage_id: Optional[int] = None,
    transaction_type: str = "trade",
) -> int:
    tx_type = (transaction_type or "trade").strip().lower()
    if tx_type not in ("trade", "transfer", "split", "bond_redemption", "conversion_blocked"):
        raise ValueError(
            "transaction_type must be 'trade', 'transfer', 'split', 'bond_redemption' or 'conversion_blocked'"
        )
    sid = storage_id if storage_id is not None else get_default_storage_id()
    t = ticker.strip().upper()
    set_instrument_asset_subclass(t, int(asset_subclass_id))
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO transactions (ticker, amount, storage_id, transaction_type)
               VALUES (?, ?, ?, ?)""",
            (t, amount, sid, tx_type),
        )
        _sync_portfolio_table(conn)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def add_bond_redemption_transaction(
    ticker: str,
    amount: float,
    asset_subclass_id: int,
    storage_id: Optional[int] = None,
) -> int:
    """
    Записать полное погашение облигации как отдельный тип операции.
    amount должен быть отрицательным (уменьшение позиции в штуках).
    """
    qty = float(amount)
    if qty >= 0:
        raise ValueError("bond redemption amount must be negative")
    return add_transaction(
        ticker=ticker,
        amount=qty,
        asset_subclass_id=asset_subclass_id,
        storage_id=storage_id,
        transaction_type="bond_redemption",
    )


def add_transfer_transaction(
    ticker: str,
    amount: float,
    asset_subclass_id: int,
    from_storage_id: int,
    to_storage_id: int,
) -> None:
    qty = float(amount)
    if qty <= 0:
        raise ValueError("amount must be positive")
    if int(from_storage_id) == int(to_storage_id):
        raise ValueError("from_storage_id and to_storage_id must differ")
    conn = get_conn()
    try:
        t = ticker.strip().upper()
        set_instrument_asset_subclass(t, int(asset_subclass_id))
        conn.execute(
            """INSERT INTO transactions (ticker, amount, storage_id, transaction_type)
               VALUES (?, ?, ?, 'transfer')""",
            (t, -qty, int(from_storage_id)),
        )
        conn.execute(
            """INSERT INTO transactions (ticker, amount, storage_id, transaction_type)
               VALUES (?, ?, ?, 'transfer')""",
            (t, qty, int(to_storage_id)),
        )
        _sync_portfolio_table(conn)
        conn.commit()
    finally:
        conn.close()


def list_transactions() -> List[Transaction]:
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT t.id, t.ticker, t.amount, t.transaction_type, t.created_at,
                      t.storage_id, s.name AS storage_name
               FROM transactions t
               LEFT JOIN storages s ON s.id = t.storage_id
               ORDER BY t.created_at DESC, t.id DESC"""
        ).fetchall()
        default_sid = get_default_storage_id()
        return [
            Transaction(
                r["id"],
                r["ticker"],
                r["amount"],
                resolve_asset_subclass_id(r["ticker"]),
                str(r["transaction_type"] or "trade"),
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
    return None


def resolve_asset_subclass_id(ticker: str) -> int:
    """
    Подкласс для сделок/отображения: настройка → эвристика по тикеру → дефолт.
    Всегда возвращает валидный id.
    """
    from app.services.subclass_inference import DEFAULT_SUBCLASS_NAME, infer_subclass_name

    t = ticker.upper().strip()
    cfg = get_instrument_asset_subclass(t)
    if cfg is not None:
        return cfg
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
        if _instruments_has_asset_subclass_column(conn):
            rows = conn.execute(
                """SELECT t.ticker, t.storage_id, MAX(s.name) AS storage_name,
                         SUM(t.amount) AS total,
                         (SELECT i.asset_subclass_id FROM instruments i
                          WHERE i.ticker = t.ticker AND i.asset_subclass_id IS NOT NULL) AS asset_subclass_id
                   FROM transactions t
                   LEFT JOIN storages s ON s.id = t.storage_id
                   GROUP BY t.ticker, t.storage_id
                   HAVING total > 0
                   ORDER BY t.ticker, storage_name"""
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT t.ticker, t.storage_id, MAX(s.name) AS storage_name,
                         SUM(t.amount) AS total
                   FROM transactions t
                   LEFT JOIN storages s ON s.id = t.storage_id
                   GROUP BY t.ticker, t.storage_id
                   HAVING total > 0
                   ORDER BY t.ticker, storage_name"""
            ).fetchall()
        out = []
        for r in rows:
            sid_sub = r["asset_subclass_id"] if "asset_subclass_id" in r.keys() else None
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


def list_positions_by_ticker(main_only: bool = False) -> List[Position]:
    """Одна строка на тикер: суммарное количество по всем местам хранения (для сводной таблицы)."""
    conn = get_conn()
    try:
        _sync_portfolio_table(conn)
        where_main = "WHERE COALESCE(p.main, 0) = 1" if main_only else ""
        if _instruments_has_asset_subclass_column(conn):
            rows = conn.execute(
                f"""SELECT t.ticker,
                         SUM(t.amount) AS total,
                         (SELECT i.asset_subclass_id FROM instruments i
                          WHERE i.ticker = t.ticker AND i.asset_subclass_id IS NOT NULL) AS asset_subclass_id
                   FROM transactions t
                   LEFT JOIN {_PORTFOLIO_TABLE} p ON p.ticker = t.ticker AND p.storage_id = t.storage_id
                   {where_main}
                   GROUP BY t.ticker
                   HAVING total > 0
                   ORDER BY t.ticker"""
            ).fetchall()
        else:
            rows = conn.execute(
                f"""SELECT t.ticker,
                         SUM(t.amount) AS total
                   FROM transactions t
                   LEFT JOIN {_PORTFOLIO_TABLE} p ON p.ticker = t.ticker AND p.storage_id = t.storage_id
                   {where_main}
                   GROUP BY t.ticker
                   HAVING total > 0
                   ORDER BY t.ticker"""
            ).fetchall()
        out: List[Position] = []
        for r in rows:
            sid_sub = r["asset_subclass_id"] if "asset_subclass_id" in r.keys() else None
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
