
import csv
import subprocess
from pathlib import Path

import shutil
from datetime import datetime

from flask import Flask, jsonify, request

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_FILE = BASE_DIR / "output" / "recommendations.csv"

app = Flask(__name__)


def save_csv(filename, rows, columns):
    with open(DATA_DIR / filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def run_command(command):
    subprocess.run(command, cwd=BASE_DIR, check=True)




def update_daily_trends():
    exports_dir = BASE_DIR.parent / "exports" / "daily"
    candidates = []

    for folder in exports_dir.iterdir():
        if not folder.is_dir():
            continue

        try:
            date = datetime.strptime(folder.name, "%Y-%m-%d").date()
        except ValueError:
            continue

        csv_files = sorted(folder.glob("part-*.csv"))
        if csv_files:
            candidates.append((date, csv_files[0]))

    if not candidates:
        raise FileNotFoundError(
            f"No daily trend exports found in {exports_dir}"
        )

    latest_date, latest_csv = max(candidates, key=lambda item: item[0])

    destination = DATA_DIR / "daily_trends.csv"
    shutil.copy2(latest_csv, destination)

    print(f"Updated daily trends from {latest_date}: {latest_csv}")





@app.post("/api/recommendations/refresh")
def refresh_recommendations():
    data = request.json

    DATA_DIR.mkdir(exist_ok=True)
    (BASE_DIR / "output").mkdir(exist_ok=True)

    save_csv(
        "products.csv",
        data["products"],
        ["productId", "name", "category", "color", "theme"],
    )
    save_csv(
        "users.csv",
        data["users"],
        ["userId", "favTheme", "favColor"],
    )
    save_csv(
        "ratings.csv",
        data["ratings"],
        ["userId", "productId", "rating"],
    )

    update_daily_trends()


    run_command(["python3", str(BASE_DIR / "generate_product_embeddings.py")])
    run_command(["mvn", "-q", "-DskipTests", "package"])

    run_command([
        str(Path.home() / "spark" / "bin" / "spark-submit"),
        "--class", "recommendation.Main",
        "--master", "local[2]",
        str(BASE_DIR / "target" / "recommendation-1.0-SNAPSHOT.jar"),
        "refresh",
    ])

    return jsonify({"status": "success", "message": "Recommendations refreshed"})





@app.get("/api/recommendations/<int:user_id>")
def get_recommendations(user_id):
    with open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
        rows = [
            row for row in csv.DictReader(f)
            if int(row["userId"]) == user_id
        ]

    return jsonify({"userId": user_id, "recommendations": rows})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002, debug=False)
