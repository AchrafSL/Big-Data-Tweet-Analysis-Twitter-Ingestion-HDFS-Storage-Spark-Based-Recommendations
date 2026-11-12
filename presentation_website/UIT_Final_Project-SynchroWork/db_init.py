"""
db_init.py — SQLite database initialisation for DamsoStream.
Creates the schema (if it does not already exist) and seeds initial offer data.
Run directly:  python db_init.py
"""

import sqlite3
import os

DB_PATH = os.getenv("SQLITE_DB_PATH", "damsostream.db")


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS user (
    idCli        INTEGER PRIMARY KEY AUTOINCREMENT,
    Email        TEXT UNIQUE,
    Password     TEXT,
    FirstName    TEXT,
    LastName     TEXT,
    status       TEXT    DEFAULT 'unverified',
    role         TEXT    DEFAULT 'client',
    Date_Joined  TEXT,
    whatsapp     TEXT    DEFAULT '',
    pfpName      TEXT    DEFAULT 'user583abc_1649114257.png',
    favTheme     TEXT    DEFAULT '',
    favColor     TEXT    DEFAULT ''
);

CREATE TABLE IF NOT EXISTS offers (
    idOffer      INTEGER PRIMARY KEY AUTOINCREMENT,
    description  TEXT,
    image_Name   TEXT    DEFAULT 'default-product-image.png',
    Offer_price  REAL,
    name         TEXT,
    duration     INTEGER,
    category     TEXT    DEFAULT '',
    color        TEXT    DEFAULT '',
    theme        TEXT    DEFAULT '',
    producer_id  INTEGER,
    FOREIGN KEY (producer_id) REFERENCES user(idCli)
);

CREATE TABLE IF NOT EXISTS orders (
    idOrder          INTEGER PRIMARY KEY AUTOINCREMENT,
    idCli            INTEGER,
    StatOfTreatment  TEXT    DEFAULT 'Pending Treatment',
    PaymentStat      TEXT    DEFAULT 'Pending Payment',
    TotalPrice       REAL,
    FOREIGN KEY (idCli) REFERENCES user(idCli)
);

CREATE TABLE IF NOT EXISTS orderoffers (
    OrderOffers_Id  INTEGER PRIMARY KEY AUTOINCREMENT,
    idOrder         INTEGER,
    idOffer         INTEGER,
    FOREIGN KEY (idOrder) REFERENCES orders(idOrder),
    FOREIGN KEY (idOffer) REFERENCES offers(idOffer)
);

CREATE TABLE IF NOT EXISTS cart (
    idCart    INTEGER PRIMARY KEY AUTOINCREMENT,
    idCli     INTEGER UNIQUE,
    fullPrice REAL,
    FOREIGN KEY (idCli) REFERENCES user(idCli)
);

CREATE TABLE IF NOT EXISTS cartoffers (
    CartOffer_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    idCart        INTEGER,
    idOffer       INTEGER,
    FOREIGN KEY (idCart) REFERENCES cart(idCart),
    FOREIGN KEY (idOffer) REFERENCES offers(idOffer)
);

CREATE TABLE IF NOT EXISTS subscription (
    idSubscription     INTEGER PRIMARY KEY AUTOINCREMENT,
    idCli              INTEGER,
    idOffer            INTEGER,
    subscriptionStatus TEXT    DEFAULT 'Active',
    startDate          TEXT,
    endDate            TEXT,
    FOREIGN KEY (idCli)   REFERENCES user(idCli),
    FOREIGN KEY (idOffer) REFERENCES offers(idOffer)
);

CREATE TABLE IF NOT EXISTS comment (
    idComment    INTEGER PRIMARY KEY AUTOINCREMENT,
    idOffer      INTEGER NOT NULL,
    idCli        INTEGER NOT NULL,
    comment_TEXT TEXT    NOT NULL,
    Date_written TEXT,
    FOREIGN KEY (idOffer) REFERENCES offers(idOffer),
    FOREIGN KEY (idCli)   REFERENCES user(idCli)
);

CREATE TABLE IF NOT EXISTS ratings (
    idRating  INTEGER PRIMARY KEY AUTOINCREMENT,
    idCli     INTEGER NOT NULL,
    idOffer   INTEGER NOT NULL,
    rating    INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
    rated_at  TEXT,
    UNIQUE(idCli, idOffer),
    FOREIGN KEY (idCli)   REFERENCES user(idCli),
    FOREIGN KEY (idOffer) REFERENCES offers(idOffer)
);

CREATE TABLE IF NOT EXISTS trends (
    idTrend    INTEGER PRIMARY KEY AUTOINCREMENT,
    topic      TEXT    NOT NULL UNIQUE,
    frequency  INTEGER NOT NULL DEFAULT 0,
    is_mock    INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_user_email        ON user(Email);
CREATE INDEX IF NOT EXISTS idx_offers_name       ON offers(name);
CREATE INDEX IF NOT EXISTS idx_ratings_user      ON ratings(idCli);
CREATE INDEX IF NOT EXISTS idx_ratings_offer     ON ratings(idOffer);
CREATE INDEX IF NOT EXISTS idx_cart_user         ON cart(idCli);
CREATE INDEX IF NOT EXISTS idx_orders_user       ON orders(idCli);
CREATE INDEX IF NOT EXISTS idx_sub_user          ON subscription(idCli);
"""

SEED_OFFERS = [
    (14, 'Netflix account. Watch on Smart TVs, Playstation, Xbox, Chromecast, Apple TV, Blu-ray players, and more. Download your shows to watch offline. Save your favorites easily and always have something to watch. Watch everywhere. Stream unlimited movies and TV shows on your phone, tablet, laptop, and TV.',
     'Netflix accounts.jpg', 40.00, 'Netflix accounts', 30, 'entertainment', 'red', 'entertainment'),
    (15, 'Spotify, audio streaming service that offers users access to music tracks, podcasts, and other media through a subscription model.',
     'Spotify Premium.png', 70.00, 'Spotify Premium', 90, 'music', 'green', 'music'),
    (16, 'Prime Video offers a massive library of movies, series, and sports.',
     'Amazon Prime.jpg', 40.00, 'Amazon Prime', 30, 'entertainment', 'blue', 'entertainment'),
    (17, 'Hulu is the leading and most comprehensive all-in-one premium streaming service.',
     'Hulu Subscription.jpg', 50.00, 'Hulu Subscription', 30, 'entertainment', 'green', 'entertainment'),
    (18, 'Crunchyroll is an American entertainment company focused on the distribution and licensing of anime.',
     'Crunchyroll.jpg', 40.00, 'Crunchyroll', 30, 'anime', 'orange', 'anime'),
    (19, 'Snapchat+ gives you exclusive, experimental, and pre-release features.',
     'Snapchat+.jpg', 250.00, 'Snapchat+', 365, 'social', 'yellow', 'social'),
    (20, 'Adobe Creative Cloud includes more than 20 software applications for creators.',
     'Adobe Creative Cloud.png', 500.00, 'Adobe Creative Cloud', 365, 'productivity', 'red', 'productivity'),
    (21, 'Office 2021 features new co-authoring, inking tools, data types, functions, and more.',
     'Office 2021.png', 250.00, 'Office 2021', 365, 'productivity', 'blue', 'productivity'),
    (22, 'Microsoft 365 is a cloud-powered productivity platform with the latest apps.',
     'Office 365.png', 200.00, 'Office 365', 365, 'productivity', 'blue', 'productivity'),
    (23, 'Duolingo is the world\'s most popular language learning app.',
     'duolingo plus.jpg', 100.00, 'duolingo plus', 365, 'education', 'green', 'education'),
    (24, 'Grammarly is the AI writing partner for every stage of writing.',
     'grammarly.png', 100.00, 'grammarly', 365, 'productivity', 'green', 'productivity'),
    (25, 'NordVPN envelops all your online activities in a layer of encryption.',
     'NordVpn.jpg', 40.00, 'NordVpn', 30, 'security', 'blue', 'security'),
    (26, 'STARZPLAY allows you to watch anytime, anywhere on multiple devices.',
     'StarzPlay.jpg', 40.00, 'StarzPlay', 30, 'entertainment', 'purple', 'entertainment'),
    (27, 'OSN Group is a multinational premium entertainment company serving MENA.',
     'oSn+.jpg', 40.00, 'oSn+', 30, 'entertainment', 'gold', 'entertainment'),
    (28, 'IPTV works by transmitting television content over an internet connection.',
     'Iptv.jpg', 250.00, 'Iptv', 365, 'entertainment', 'gray', 'entertainment'),
    (29, 'Apple One bundles up to six Apple subscriptions for one lower monthly price.',
     'Apple one Premier.png', 300.00, 'Apple one Premier', 365, 'bundle', 'silver', 'bundle'),
    (30, 'Apple One provides access to Apple Music, Apple TV+, Apple Arcade, and iCloud storage.',
     'Apple one.png', 100.00, 'Apple one', 90, 'bundle', 'silver', 'bundle'),
    (31, 'Disney+ is the streaming home for Disney, Pixar, Marvel, Star Wars, and National Geographic.',
     'Disney+.jpg', 40.00, 'Disney+', 1, 'entertainment', 'blue', 'entertainment'),
    (32, 'NBA TV is an American sports-oriented pay television network owned by the NBA.',
     'NBA TV.jpg', 40.00, 'NBA TV', 30, 'sports', 'orange', 'sports'),
    (33, 'Canva is an online template editor app for creating social media graphics.',
     'Canva Pro.jpg', 70.00, 'Canva Pro', 365, 'design', 'purple', 'design'),
    (34, 'HBO Max is a streaming experience from HBO, Warner Bros., DC, Cartoon Network, and Max Originals.',
     'HBOmax.jpg', 40.00, 'HBOmax', 30, 'entertainment', 'purple', 'entertainment'),
]

SEED_TRENDS = [
    ('Netflix', 1200, 1),
    ('Spotify', 950, 1),
    ('Amazon Prime', 800, 1),
    ('Gaming', 750, 1),
    ('Anime', 700, 1),
    ('Disney+', 650, 1),
    ('Music Streaming', 620, 1),
    ('VPN', 580, 1),
    ('Office Productivity', 540, 1),
    ('Sports Streaming', 490, 1),
]


def determine_product_metadata(name="", description="", category="", color="", theme=""):
    """
    Populates missing metadata using sensible values based on product name and description.
    Ensures consistent lowercase for categories and themes.
    Preserves existing valid metadata without overwriting.
    """
    cat = (category or "").strip().lower()
    col = (color or "").strip().lower()
    thm = (theme or "").strip().lower()

    # Reuse existing fields if one is set and the other is missing
    if cat and not thm:
        thm = cat
    elif thm and not cat:
        cat = thm

    # If category or theme is still missing, deduce from name & description
    if not cat or not thm:
        text = f"{(name or '').lower()} {(description or '').lower()}"
        if any(w in text for w in ["vpn", "security", "encrypt", "firewall", "privacy"]):
            inferred_cat = "security"
            inferred_col = "blue"
        elif any(w in text for w in ["netflix", "prime", "hulu", "starzplay", "osn", "iptv", "disney", "hbomax", "movie", "tv", "stream", "show", "watch"]):
            inferred_cat = "entertainment"
            inferred_col = "red"
        elif any(w in text for w in ["spotify", "music", "audio", "song", "podcast"]):
            inferred_cat = "music"
            inferred_col = "green"
        elif any(w in text for w in ["crunchyroll", "anime", "manga"]):
            inferred_cat = "anime"
            inferred_col = "orange"
        elif any(w in text for w in ["snapchat", "social", "chat"]):
            inferred_cat = "social"
            inferred_col = "yellow"
        elif any(w in text for w in ["office", "adobe", "grammarly", "word", "excel", "cloud", "productivity", "software", "tool"]):
            inferred_cat = "productivity"
            inferred_col = "blue"
        elif any(w in text for w in ["duolingo", "education", "learn", "course", "language"]):
            inferred_cat = "education"
            inferred_col = "green"
        elif any(w in text for w in ["apple one", "bundle", "pack"]):
            inferred_cat = "bundle"
            inferred_col = "silver"
        elif any(w in text for w in ["nba", "sport", "game"]):
            inferred_cat = "sports"
            inferred_col = "orange"
        elif any(w in text for w in ["canva", "design", "graphic", "photo"]):
            inferred_cat = "design"
            inferred_col = "purple"
        else:
            inferred_cat = "general"
            inferred_col = "blue"

        if not cat:
            cat = inferred_cat
        if not thm:
            thm = inferred_cat
        if not col:
            col = inferred_col

    # If color is still missing, infer from theme/category
    if not col:
        theme_color_map = {
            "entertainment": "red",
            "music": "green",
            "anime": "orange",
            "social": "yellow",
            "productivity": "blue",
            "education": "green",
            "security": "blue",
            "bundle": "silver",
            "sports": "orange",
            "design": "purple",
        }
        col = theme_color_map.get(thm, theme_color_map.get(cat, "blue"))

    return cat, col, thm


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Create schema and seed data if tables are empty, and safely migrate columns."""
    conn = get_connection()
    try:
        conn.executescript(SCHEMA)

        # Migration: ensure category, color, and theme columns exist on offers
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(offers)")
        cols = [r[1] for r in cur.fetchall()]
        for col_name in ["category", "color", "theme"]:
            if col_name not in cols:
                cur.execute(f"ALTER TABLE offers ADD COLUMN {col_name} TEXT DEFAULT ''")
                conn.commit()

        # Seed offers if table is empty (INSERT OR IGNORE preserves existing data)
        cur.execute("SELECT COUNT(*) FROM offers")
        offer_count = cur.fetchone()[0]

        if offer_count == 0:
            cur.executemany(
                "INSERT OR IGNORE INTO offers (idOffer, description, image_Name, Offer_price, name, duration, category, color, theme) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                SEED_OFFERS
            )
            conn.commit()

        # Migrate and populate metadata for existing offers safely without overwriting valid data
        cur.execute("SELECT idOffer, name, description, category, color, theme FROM offers")
        for row in cur.fetchall():
            offer_id = row[0]
            name = row[1] or ""
            desc = row[2] or ""
            cat = row[3] or ""
            col = row[4] or ""
            thm = row[5] or ""
            new_cat, new_col, new_thm = determine_product_metadata(name, desc, cat, col, thm)
            if new_cat != cat or new_col != col or new_thm != thm:
                cur.execute(
                    "UPDATE offers SET category=?, color=?, theme=? WHERE idOffer=?",
                    (new_cat, new_col, new_thm, offer_id)
                )
        conn.commit()

        # Seed mock trends
        cur.execute("SELECT COUNT(*) FROM trends")
        trend_count = cur.fetchone()[0]

        if trend_count == 0:
            from datetime import datetime
            now = datetime.now().isoformat()
            cur.executemany(
                "INSERT OR IGNORE INTO trends (topic, frequency, is_mock, updated_at) VALUES (?,?,?,?)",
                [(t[0], t[1], t[2], now) for t in SEED_TRENDS]
            )

        # Migration: ensure producer_id exists on offers
        cur.execute("PRAGMA table_info(offers)")
        cols = [r[1] for r in cur.fetchall()]
        if "producer_id" not in cols:
            cur.execute("ALTER TABLE offers ADD COLUMN producer_id INTEGER REFERENCES user(idCli)")
            conn.commit()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_offers_producer ON offers(producer_id)")
        conn.commit()

        # Seed default verified producer account if missing
        cur.execute("SELECT idCli FROM user WHERE Email='producer@damsostream.com'")
        prod_row = cur.fetchone()
        if not prod_row:
            from datetime import datetime
            now = datetime.now().isoformat()
            cur.execute(
                "INSERT INTO user (Email, Password, FirstName, LastName, role, status, Date_Joined) "
                "VALUES (?,?,?,?,?,?,?)",
                ('producer@damsostream.com', 'Producer123!', 'Damso', 'Producer', 'producer', 'verified', now)
            )
            conn.commit()
            prod_id = cur.lastrowid
            cur.execute("INSERT OR IGNORE INTO cart (idCli) VALUES (?)", (prod_id,))
            conn.commit()
        else:
            prod_id = prod_row[0]

        # Ensure existing offers have an owner (default to producer_id)
        cur.execute("UPDATE offers SET producer_id=? WHERE producer_id IS NULL", (prod_id,))

        conn.commit()
        print(f"[db_init] Database initialised at: {os.path.abspath(DB_PATH)}")
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    print("[db_init] Done.")
