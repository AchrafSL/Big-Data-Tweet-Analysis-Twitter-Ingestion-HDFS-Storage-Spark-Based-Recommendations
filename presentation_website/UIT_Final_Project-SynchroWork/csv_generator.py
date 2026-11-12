"""
csv_generator.py — Generates the four CSV files required by the Big Data recommendation pipeline.
All data is read from the SQLite database. Files are written atomically (temp file → rename).

Output directory is configurable via CSV_OUTPUT_DIR environment variable.
Defaults to: ./csv_exports/
"""

import os
import csv
import sqlite3
import tempfile
import shutil
from datetime import datetime

from db_init import get_connection

CSV_OUTPUT_DIR = os.getenv("CSV_OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "csv_exports"))


def _ensure_output_dir():
    os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)


def _atomic_write(filename: str, rows: list, fieldnames: list) -> dict:
    """
    Write rows to a CSV file atomically.
    Returns a dict with 'path', 'count', 'timestamp', 'status', and optional 'error'.
    """
    _ensure_output_dir()
    dest_path = os.path.join(CSV_OUTPUT_DIR, filename)
    timestamp = datetime.now().isoformat(timespec="seconds")

    # Write to temp file in same directory so rename is atomic on most OSes
    try:
        fd, tmp_path = tempfile.mkstemp(dir=CSV_OUTPUT_DIR, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        except Exception:
            os.unlink(tmp_path)
            raise
        shutil.move(tmp_path, dest_path)
    except Exception as exc:
        return {
            "filename": filename,
            "path": dest_path,
            "count": 0,
            "timestamp": timestamp,
            "status": "error",
            "error": str(exc),
        }

    return {
        "filename": filename,
        "path": dest_path,
        "count": len(rows),
        "timestamp": timestamp,
        "status": "ok",
        "error": None,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Individual generators
# ─────────────────────────────────────────────────────────────────────────────

def generate_products_csv() -> dict:
    """
    products.csv  →  productId, name, category, color, theme
    Maps idOffer to productId and actual name column to name.
    """
    from db_init import determine_product_metadata

    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT idOffer, name, description, category, color, theme FROM offers ORDER BY idOffer"
        )
        rows = []
        for row in cur.fetchall():
            cat = (row["category"] or "").strip().lower()
            col = (row["color"] or "").strip().lower()
            thm = (row["theme"] or "").strip().lower()
            if not cat or not thm or not col:
                cat, col, thm = determine_product_metadata(
                    row["name"] or "",
                    row["description"] or "",
                    cat, col, thm
                )
            rows.append({
                "productId": row["idOffer"],
                "name":      row["name"] or "",
                "category":  cat,
                "color":     col,
                "theme":     thm,
            })
    finally:
        conn.close()

    return _atomic_write("products.csv", rows, ["productId", "name", "category", "color", "theme"])


def generate_users_csv() -> dict:
    """
    users.csv  →  userId, favTheme, favColor
    Only registered users. Empty string when preference not set.
    """
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT idCli, favTheme, favColor FROM user ORDER BY idCli"
        )
        rows = []
        for row in cur.fetchall():
            rows.append({
                "userId":   row["idCli"],
                "favTheme": row["favTheme"] or "",
                "favColor": row["favColor"] or "",
            })
    finally:
        conn.close()

    return _atomic_write("users.csv", rows, ["userId", "favTheme", "favColor"])


def generate_ratings_csv() -> dict:
    """
    ratings.csv  →  userId, productId, rating
    One row per user-product pair. Rating is 1–5.
    """
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT idCli, idOffer, rating FROM ratings ORDER BY idCli, idOffer"
        )
        rows = []
        for row in cur.fetchall():
            rows.append({
                "userId":    row["idCli"],
                "productId": row["idOffer"],
                "rating":    row["rating"],
            })
    finally:
        conn.close()

    return _atomic_write("ratings.csv", rows, ["userId", "productId", "rating"])


def generate_trends_csv() -> dict:
    """
    trends_mock.csv  →  topic, frequency
    Rows where is_mock=1 are clearly labelled as demo data via a comment in the
    'topic' field prefix — no, that would break schema.  Instead we note in
    the file header comment (not in CSV data row) and in the status result.
    Real pipeline results have is_mock=0.
    NOTE: This file may contain mock data. Check the 'status' field in the
    returned dict; it includes a 'mock_count' key for transparency.
    """
    conn = get_connection()
    try:
        cur = conn.execute(
            "SELECT topic, frequency, is_mock FROM trends ORDER BY frequency DESC"
        )
        rows = []
        mock_count = 0
        for row in cur.fetchall():
            rows.append({
                "topic":     row["topic"],
                "frequency": row["frequency"],
            })
            if row["is_mock"]:
                mock_count += 1
    finally:
        conn.close()

    result = _atomic_write("trends_mock.csv", rows, ["topic", "frequency"])
    result["mock_count"] = mock_count
    result["real_count"] = len(rows) - mock_count
    if mock_count > 0 and result["real_count"] == 0:
        result["warning"] = "All trend data is MOCK/DEMO — not sourced from real pipeline."
    elif mock_count > 0:
        result["warning"] = f"{mock_count} mock and {result['real_count']} real trend rows exported."
    return result


def generate_all_csvs() -> dict:
    """Generate all four CSV files and return a summary dict."""
    return {
        "products":  generate_products_csv(),
        "users":     generate_users_csv(),
        "ratings":   generate_ratings_csv(),
        "trends":    generate_trends_csv(),
        "output_dir": os.path.abspath(CSV_OUTPUT_DIR),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Trend import helper (called by admin when real pipeline results arrive)
# ─────────────────────────────────────────────────────────────────────────────

def import_real_trends(topic_frequency_list: list) -> dict:
    """
    Replace or insert real (non-mock) trend rows.
    topic_frequency_list: list of (topic, frequency) tuples from the pipeline.
    Returns count of upserted rows.
    """
    conn = get_connection()
    now = datetime.now().isoformat()
    try:
        count = 0
        for topic, frequency in topic_frequency_list:
            conn.execute(
                "INSERT INTO trends (topic, frequency, is_mock, updated_at) VALUES (?,?,0,?) "
                "ON CONFLICT(topic) DO UPDATE SET frequency=excluded.frequency, is_mock=0, updated_at=excluded.updated_at",
                (topic, frequency, now)
            )
            count += 1
        conn.commit()
    finally:
        conn.close()
    return {"upserted": count, "timestamp": now}


if __name__ == "__main__":
    from db_init import init_db
    init_db()
    result = generate_all_csvs()
    for key, val in result.items():
        if isinstance(val, dict):
            print(f"  {key}: {val.get('status','?')} - {val.get('count',0)} rows -> {val.get('path','')}")
            if val.get("warning"):
                print(f"    [!] {val['warning']}")
        else:
            print(f"  {key}: {val}")
