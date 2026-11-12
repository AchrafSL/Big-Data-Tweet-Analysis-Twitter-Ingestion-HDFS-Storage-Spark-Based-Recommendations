
import csv
import glob
import os
import subprocess
from datetime import date

from flask import Flask, jsonify, request

app = Flask(__name__)

PROJECT_DIR = "/home/hdoop/project_calcule_parallele"
SPARK_SUBMIT = "/home/hdoop/spark/bin/spark-submit"
CLASS_DIR = os.path.join(PROJECT_DIR, "target/classes")
EXTRA_JARS_DIR = "/home/hdoop/spark-extra-jars"
EXPORT_DIR = os.path.join(PROJECT_DIR, "exports")

ANALYZER_CLASS = "TopicTrendAnalyzer"


def run_analyzer(mode, args, output_path):
    """Run TopicTrendAnalyzer and read its Spark CSV output."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    extra_jars = glob.glob(os.path.join(EXTRA_JARS_DIR, "*.jar"))
    if not extra_jars:
        raise FileNotFoundError(f"No extra JARs found in {EXTRA_JARS_DIR}")

    command = [
        SPARK_SUBMIT,
        "--class", ANALYZER_CLASS,
        "--master", "local[*]",
        "--jars", ",".join(extra_jars),
        CLASS_DIR,
        mode,
        *map(str, args),
        output_path,
    ]

    result = subprocess.run(
        command,
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=600,
    )

    if result.returncode != 0:
        app.logger.error("Spark stderr: %s", result.stderr[-5000:])
        raise RuntimeError("Trend calculation failed")

    csv_files = glob.glob(os.path.join(output_path, "part-*.csv"))
    trends = []

    for csv_path in csv_files:
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                trends.append({
                    "topic": row["topic"],
                    "frequency": int(row["frequency"]),
                })

    return trends


def parse_limit():
    limit = int(request.args.get("limit", 0))

    if limit < 0:
        raise ValueError("limit must be 0 or greater")

    return limit

def parse_date():
    day = request.args.get("date", "").strip()
    if not day or date.fromisoformat(day).isoformat() != day:
        raise ValueError("date must use YYYY-MM-DD format")
    return day



@app.post("/api/trends/global")
def global_trends():
    try:
        limit = parse_limit()
        output_path = os.path.join(EXPORT_DIR, "global")

        trends = run_analyzer(
            "global",
            [limit],
            output_path,
        )

        return jsonify({
            "type": "global",
            "trends": trends,
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Calculation timed out"}), 504
    except Exception:
        app.logger.exception("Global trends failed")
        return jsonify({"error": "Could not calculate global trends"}), 500


@app.post("/api/trends/daily")
def daily_trends():
    try:
        day = parse_date()
        limit = parse_limit()
        output_path = os.path.join(EXPORT_DIR, "daily", day)

        trends = run_analyzer(
            "daily",
            [day, limit],
            output_path,
        )

        return jsonify({
            "type": "daily",
            "date": day,
            "trends": trends,
        })

    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Calculation timed out"}), 504
    except Exception:
        app.logger.exception("Daily trends failed")
        return jsonify({"error": "Could not calculate daily trends"}), 500


@app.post("/api/trends/hourly")
def hourly_trends():
    try:
        day = parse_date()
        hour = int(request.args.get("hour", ""))

        if not 0 <= hour <= 23:
            return jsonify({"error": "hour must be between 0 and 23"}), 400

        limit = parse_limit()
        output_path = os.path.join(
            EXPORT_DIR, "hourly", day, f"{hour:02d}"
        )

        trends = run_analyzer(
            "hourly",
            [day, hour, limit],
            output_path,
        )

        return jsonify({
            "type": "hourly",
            "date": day,
            "hour": hour,
            "trends": trends,
        })

    except ValueError as e:
        return jsonify({
            "error": str(e) or "Invalid hour or limit"
        }), 400
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Calculation timed out"}), 504
    except Exception:
        app.logger.exception("Hourly trends failed")
        return jsonify({"error": "Could not calculate hourly trends"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
