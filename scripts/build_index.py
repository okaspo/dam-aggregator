# scripts/build_index.py
import json, pathlib, datetime

OUT = pathlib.Path("public/data/dams_index.json")
HIST_DIR = pathlib.Path("public/data/history")
LATEST = pathlib.Path("public/data/latest.json")

def tail_two_values(dam_id):
    p = HIST_DIR / f"{dam_id}.ndjson"
    if not p.exists(): return None
    lines = [l for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]
    if len(lines) < 2: return None
    a = json.loads(lines[-2]); b = json.loads(lines[-1])
    if a.get("rate_pct") is None or b.get("rate_pct") is None: return None
    return (a, b)

def build_index():
    latest = json.loads(LATEST.read_text(encoding="utf-8"))
    rows = latest["records"]
    out = []

    for r in rows:
        dam_id = r["dam_id"]
        # 直近差
        delta_last = None
        pair = tail_two_values(dam_id)
        if pair:
            a, b = pair
            delta_last = round((b["rate_pct"] - a["rate_pct"]), 1)

        # 24h差（簡易：直近2点の間隔が約24hならその差。厳密化は後で）
        delta_24h = delta_last  # MVPでは簡易に
        slope = delta_24h  # %/日 仮

        out.append({
            "dam_id": dam_id,
            "name": r["name"],
            "pref": r["pref"],
            "operator_group": r["operator_group"],
            "lat": r["lat"], "lon": r["lon"],
            "rate_pct": r.get("rate_pct"),
            "level_m": r.get("level_m"),
            "updated_at": r["observed_at"],
            "delta_pct_last": delta_last,
            "delta_pct_24h": delta_24h,
            "slope_pct_per_day": slope
        })

    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"wrote {OUT}")

if __name__ == "__main__":
    build_index()
