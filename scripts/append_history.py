# scripts/append_history.py
import json, pathlib

HIST_DIR = pathlib.Path("public/data/history")
LATEST = pathlib.Path("public/data/latest.json")

def append_records():
    data = json.loads(LATEST.read_text(encoding="utf-8"))
    recs = data["records"]
    HIST_DIR.mkdir(parents=True, exist_ok=True)

    # dam_id + observed_at をユニークキーとして追記（重複はスキップ）
    seen = {}
    # 既存のキー読み込み（メモリに全読み込みしない軽量版でもOK）
    for p in HIST_DIR.glob("*.ndjson"):
        dam_id = p.stem
        seen.setdefault(dam_id, set())
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip(): continue
            obj = json.loads(line)
            seen[dam_id].add(obj["observed_at"])

    # 追記
    appended = 0
    for r in recs:
        dam_id = r["dam_id"]
        observed = r["observed_at"]
        path = HIST_DIR / f"{dam_id}.ndjson"
        if dam_id not in seen: seen[dam_id] = set()
        if observed in seen[dam_id]:
            continue
        row = {
            "dam_id": dam_id,
            "observed_at": observed,
            "rate_pct": r.get("rate_pct"),
            "level_m": r.get("level_m"),
            "source": "sample"  # 後で prefect_xxx に差し替え
        }
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        seen[dam_id].add(observed)
        appended += 1
    print(f"appended {appended} records")

if __name__ == "__main__":
    append_records()
