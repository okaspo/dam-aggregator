# scripts/fetch_example.py
# 目的: ダミーCSVを読み、今回分の「最新スナップショット」を JSON で出力
import csv, json, pathlib
from datetime import datetime, timezone, timedelta

SRC = pathlib.Path("public/data/sample_source.csv")
OUT = pathlib.Path("public/data/latest.json")

def main():
    rows = []
    with SRC.open(encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # 文字列→数値
            row["lat"] = float(row["lat"])
            row["lon"] = float(row["lon"])
            row["rate_pct"] = float(row["rate_pct"]) if row["rate_pct"] else None
            row["level_m"]  = float(row["level_m"]) if row["level_m"] else None
            rows.append(row)
    out = {
        "fetched_at": datetime.now(timezone(timedelta(hours=9))).isoformat(),
        "records": rows
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {OUT}")

if __name__ == "__main__":
    main()
