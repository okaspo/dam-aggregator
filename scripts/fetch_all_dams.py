#!/usr/bin/env python3
"""
全国のダムデータを統合して取得
"""
import json
import pathlib
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import urllib.request
import urllib.error
from html.parser import HTMLParser

# 各スクリプトからダムリストをインポート
import sys
sys.path.append(str(pathlib.Path(__file__).parent))

# fetch_mlit_dams.py から関数をインポート
from fetch_mlit_dams import (
    DAM_STATIONS as MLIT_DAMS,
    DamDataParser,
    fetch_dam_data,
    load_dam_locations
)


def load_all_dam_metadata() -> Dict[str, Dict]:
    """すべてのダムメタデータを読み込み"""
    all_dams = {}
    locations = load_dam_locations()

    # 国土交通省管理ダム
    for station_id, info in MLIT_DAMS.items():
        loc = locations.get(station_id, {"lat": 35.6895, "lon": 139.6917})
        all_dams[station_id] = {
            **info,
            "lat": loc["lat"],
            "lon": loc["lon"],
            "dam_id": f"dam_{station_id}",
        }

    # 水資源機構管理ダムのメタデータを読み込み
    water_file = pathlib.Path(__file__).parent.parent / "public/data/water_resource_dams.json"
    if water_file.exists():
        water_dams = json.loads(water_file.read_text(encoding="utf-8"))
        for dam in water_dams:
            station_id = dam["station_id"]
            if station_id not in all_dams:  # 重複を避ける
                all_dams[station_id] = {
                    "name": dam["name"],
                    "pref": dam["pref"],
                    "river": dam.get("river", ""),
                    "operator_group": dam["operator_group"],
                    "lat": dam["lat"],
                    "lon": dam["lon"],
                    "dam_id": dam["dam_id"],
                }

    # 自治体管理ダムのメタデータを読み込み
    local_file = pathlib.Path(__file__).parent.parent / "public/data/local_dams.json"
    if local_file.exists():
        local_dams = json.loads(local_file.read_text(encoding="utf-8"))
        for dam in local_dams:
            station_id = dam["station_id"]
            if station_id not in all_dams:  # 重複を避ける
                all_dams[station_id] = {
                    "name": dam["name"],
                    "pref": dam["pref"],
                    "river": dam.get("river", ""),
                    "operator_group": dam["operator_group"],
                    "lat": dam["lat"],
                    "lon": dam["lon"],
                    "dam_id": dam["dam_id"],
                }

    return all_dams


def fetch_all_dams_data() -> List[Dict]:
    """全ダムのデータを取得"""
    all_dams = load_all_dam_metadata()
    records = []

    total = len(all_dams)
    for idx, (station_id, info) in enumerate(all_dams.items(), 1):
        print(f"[{idx}/{total}] {info['name']}... ", end='', flush=True)

        data = fetch_dam_data(station_id)

        if data and data.get("observed_at"):
            record = {
                "dam_id": info["dam_id"],
                "name": info["name"],
                "pref": info["pref"],
                "river": info.get("river", ""),
                "operator_group": info["operator_group"],
                "lat": info["lat"],
                "lon": info["lon"],
                "observed_at": data["observed_at"],
                "rate_pct": data.get("rate_pct"),
                "level_m": data.get("level_m"),
                "storage_m3": data.get("storage_m3"),
                "inflow_m3s": data.get("inflow_m3s"),
                "outflow_m3s": data.get("outflow_m3s"),
            }
            records.append(record)
            print("✓")
        else:
            print("✗")

        # サーバーに負荷をかけないよう待機
        time.sleep(0.5)

    return records


def main():
    """メイン処理"""
    OUT = pathlib.Path("public/data/latest.json")

    print("=" * 60)
    print("全国のダムデータを取得中...")
    print("=" * 60)

    # ScrapingBee使用状況を表示
    import os
    if os.environ.get('SCRAPINGBEE_API_KEY'):
        print("✓ ScrapingBee経由でアクセスします（IP制限回避モード）")
    else:
        print("⚠ 直接アクセスモード（IP制限の影響を受ける可能性があります）")
    print()

    # メタデータファイルを先に生成
    print("メタデータファイルを生成中...")
    import subprocess
    subprocess.run([sys.executable, str(pathlib.Path(__file__).parent / "fetch_water_dams.py")], check=True)
    subprocess.run([sys.executable, str(pathlib.Path(__file__).parent / "fetch_local_dams.py")], check=True)
    print()

    # データ取得
    records = fetch_all_dams_data()

    # 保存
    out = {
        "fetched_at": datetime.now(timezone(timedelta(hours=9))).isoformat(),
        "source": "国土交通省 川の防災情報",
        "records": records
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print("=" * 60)
    print(f"✓ {len(records)}件のダムデータを取得しました")
    print(f"✓ {OUT} に保存しました")
    print("=" * 60)


if __name__ == "__main__":
    main()
