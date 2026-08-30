import json, sqlite3, time, logging
from contextlib import contextmanager
from config import DB_FILE

log = logging.getLogger("bot.db")

def init_db():
    c = sqlite3.connect(DB_FILE, timeout=60)
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
        CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS markets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, slug TEXT, enabled INTEGER DEFAULT 1, last_probs TEXT DEFAULT '{}');
        CREATE TABLE IF NOT EXISTS market_history (id INTEGER PRIMARY KEY AUTOINCREMENT, market_id INTEGER, timestamp REAL, probs TEXT);
        
        CREATE TABLE IF NOT EXISTS active_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            is_demo INTEGER DEFAULT 0,
            slug TEXT, token_id TEXT, side TEXT, size REAL,
            sl INTEGER, tp INTEGER, entry_price REAL DEFAULT 0,
            question TEXT, outcome TEXT
        );
        
        CREATE TABLE IF NOT EXISTS trade_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            is_demo INTEGER DEFAULT 0,
            slug TEXT, question TEXT, outcome TEXT, side TEXT,
            size REAL, entry_price REAL, close_price REAL,
            pnl REAL, timestamp REAL
        );
    """)
    # Миграция: каждая торговая система пишет свою статистику отдельно.
    # strategy: '' (ручные сделки/старые записи), 'liquidations', 'trend'.
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(trade_history)").fetchall()]
        if "strategy" not in cols:
            c.execute("ALTER TABLE trade_history ADD COLUMN strategy TEXT DEFAULT ''")
    except Exception:
        pass
    
    for k, v in [("m_threshold","2.0"), ("m_interval","30"), ("market_notifications","1"), ("demo_mode", "0"), ("stats_count", "10")]:
        c.execute("INSERT OR IGNORE INTO settings (key,value) VALUES (?,?)",(k,v))
    c.commit(); c.close()

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_FILE, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try: yield conn; conn.commit()
    except: conn.rollback(); raise
    finally: conn.close()

def get_setting(k, d=""):
    with get_db() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?",(k,)).fetchone()
        return r["value"] if r else d

def set_setting(k, v):
    with get_db() as c: c.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)",(k,str(v)))

# --- РЫНКИ ---
def get_markets():
    with get_db() as c:
        res = []
        for r in c.execute("SELECT * FROM markets ORDER BY id").fetchall():
            m = dict(r); m["last_probs"] = json.loads(m.get("last_probs") or "{}"); res.append(m)
        return res

def get_market(mid):
    with get_db() as c:
        r = c.execute("SELECT * FROM markets WHERE id=?",(mid,)).fetchone()
        if r: m = dict(r); m["last_probs"] = json.loads(m.get("last_probs") or "{}"); return m
        return None

def add_market(name, slug):
    with get_db() as c: return c.execute("INSERT INTO markets (name,slug,enabled,last_probs) VALUES (?,?,1,'{}')",(name,slug)).lastrowid

def update_market(mid, **kw):
    if "last_probs" in kw and isinstance(kw["last_probs"],dict): kw["last_probs"]=json.dumps(kw["last_probs"])
    if not kw: return
    with get_db() as c: c.execute(f"UPDATE markets SET {','.join(f'{k}=?' for k in kw)} WHERE id=?", list(kw.values())+[mid])

def delete_market(mid):
    with get_db() as c:
        c.execute("DELETE FROM market_history WHERE market_id=?",(mid,))
        c.execute("DELETE FROM markets WHERE id=?",(mid,))

def get_market_history(mid, hours=24):
    with get_db() as c: return [[r["timestamp"],json.loads(r["probs"])] for r in c.execute("SELECT timestamp,probs FROM market_history WHERE market_id=? AND timestamp>? ORDER BY timestamp",(mid,time.time()-hours*3600)).fetchall()]

def add_market_history(mid, probs):
    with get_db() as c:
        c.execute("INSERT INTO market_history (market_id,timestamp,probs) VALUES (?,?,?)",(mid,time.time(),json.dumps(probs)))
        c.execute("DELETE FROM market_history WHERE market_id=? AND timestamp<?",(mid,time.time()-48*3600))

def get_market_by_slug(slug):
    with get_db() as c:
        r = c.execute("SELECT * FROM markets WHERE slug=?",(slug,)).fetchone()
        if r: m = dict(r); m["last_probs"] = json.loads(m.get("last_probs") or "{}"); return m
        return None

# --- ПОЗИЦИИ И ТОРГИ ---
def add_position(is_demo, slug, token_id, side, size, sl, tp, entry_price, question, outcome):
    with get_db() as c: c.execute("INSERT INTO active_positions (is_demo, slug, token_id, side, size, sl, tp, entry_price, question, outcome) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (is_demo, slug, token_id, side, size, sl, tp, entry_price, question, outcome))

def get_positions():
    with get_db() as c: return [dict(r) for r in c.execute("SELECT * FROM active_positions").fetchall()]

def get_position_by_id(pid):
    with get_db() as c:
        r = c.execute("SELECT * FROM active_positions WHERE id=?", (pid,)).fetchone()
        return dict(r) if r else None

def remove_position(pid):
    with get_db() as c: c.execute("DELETE FROM active_positions WHERE id=?", (pid,))

def update_position_limits(pid, sl, tp):
    with get_db() as c: c.execute("UPDATE active_positions SET sl=?, tp=? WHERE id=?", (sl, tp, pid))

def add_trade_history(is_demo, slug, question, outcome, side, size, entry_price, close_price, pnl, strategy=""):
    with get_db() as c: c.execute("INSERT INTO trade_history (is_demo, slug, question, outcome, side, size, entry_price, close_price, pnl, timestamp, strategy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (is_demo, slug, question, outcome, side, size, entry_price, close_price, pnl, time.time(), strategy or ""))

def _trade_strategy_match(row, strategy):
    """Фильтр строк trade_history по торговой системе.

    strategy=None      — все сделки;
    strategy='trend'   — только «Движение за рынком»;
    strategy='liquidations' — сделки стратегии ликвидаций; старые строки без
      метки стратегии относятся к ней, если это up/down-окно (исторически
      только она и писала в историю из автостратегий).
    """
    if not strategy:
        return True
    st = (row.get("strategy") or "").strip().lower()
    if strategy == "trend":
        return st == "trend"
    if strategy == "liquidations":
        return st == "liquidations" or (st == "" and "-updown-" in (row.get("slug") or ""))
    return st == strategy

def get_trade_statistics(is_demo, strategy=None):
    with get_db() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM trade_history WHERE is_demo=? ORDER BY timestamp DESC", (is_demo,)).fetchall()]
    return [r for r in rows if _trade_strategy_match(r, strategy)]

def clear_trade_statistics(is_demo, strategy=None):
    with get_db() as c:
        rows = c.execute("SELECT id, slug, strategy FROM trade_history WHERE is_demo=?", (is_demo,)).fetchall()
        for r in rows:
            if _trade_strategy_match(dict(r), strategy):
                c.execute("DELETE FROM trade_history WHERE id=?", (r[0],))
