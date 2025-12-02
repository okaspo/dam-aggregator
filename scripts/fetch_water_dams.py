#!/usr/bin/env python3
"""
水資源機構管理ダムのデータ取得
水資源機構は利根川・荒川、木曽川、淀川、吉野川、筑後川の主要ダムを管理
"""
import json
import pathlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List

# 水資源機構管理ダム
# これらのダムは国土交通省のシステムにも含まれているため、
# 観測所記号を使用してriver.go.jpから取得可能
WATER_RESOURCE_DAMS = {
    # 利根川・荒川水系
    "1368080700010": {"name": "矢木沢ダム", "pref": "群馬県", "river": "利根川", "lat": 36.8833, "lon": 138.9167},
    "1368080700020": {"name": "奈良俣ダム", "pref": "群馬県", "river": "利根川", "lat": 36.8500, "lon": 139.0333},
    "1368080700030": {"name": "藤原ダム", "pref": "群馬県", "river": "利根川", "lat": 36.7667, "lon": 139.1000},
    "1368080700040": {"name": "相俣ダム", "pref": "群馬県", "river": "利根川", "lat": 36.7833, "lon": 139.0833},
    "1368080700050": {"name": "薗原ダム", "pref": "群馬県", "river": "利根川", "lat": 36.7500, "lon": 139.1167},
    "1368080700060": {"name": "下久保ダム", "pref": "群馬県", "river": "利根川", "lat": 36.2167, "lon": 138.9667},
    "1368080700070": {"name": "草木ダム", "pref": "群馬県", "river": "利根川", "lat": 36.5333, "lon": 139.3000},
    "1368080700080": {"name": "渡良瀬貯水池", "pref": "栃木県", "river": "利根川", "lat": 36.5667, "lon": 139.6167},
    "1368080750010": {"name": "二瀬ダム", "pref": "埼玉県", "river": "荒川", "lat": 35.9500, "lon": 138.8667},
    "1368080750020": {"name": "浦山ダム", "pref": "埼玉県", "river": "荒川", "lat": 35.9667, "lon": 138.9333},
    "1368080750030": {"name": "滝沢ダム", "pref": "埼玉県", "river": "荒川", "lat": 35.9167, "lon": 138.9000},
    "1368080750040": {"name": "有間ダム", "pref": "埼玉県", "river": "荒川", "lat": 35.9000, "lon": 139.0000},

    # 木曽川水系
    "1368081450030": {"name": "岩屋ダム", "pref": "岐阜県", "river": "木曽川", "lat": 35.7833, "lon": 137.0833},
    "1368081450040": {"name": "徳山ダム", "pref": "岐阜県", "river": "木曽川", "lat": 35.5500, "lon": 136.4167},

    # 淀川水系
    "1368083350010": {"name": "天ヶ瀬ダム", "pref": "京都府", "river": "淀川", "lat": 34.9167, "lon": 135.8167},
    "1368083350020": {"name": "高山ダム", "pref": "京都府", "river": "淀川", "lat": 35.2500, "lon": 135.8667},
    "1368083350030": {"name": "日吉ダム", "pref": "京都府", "river": "淀川", "lat": 35.2167, "lon": 135.5833},
    "1368083350040": {"name": "比奈知ダム", "pref": "三重県", "river": "淀川", "lat": 34.6333, "lon": 136.1333},
    "1368083350050": {"name": "室生ダム", "pref": "奈良県", "river": "淀川", "lat": 34.5333, "lon": 136.0500},
    "1368083350060": {"name": "布目ダム", "pref": "奈良県", "river": "淀川", "lat": 34.7333, "lon": 135.9833},
    "1368083350070": {"name": "一庫ダム", "pref": "兵庫県", "river": "淀川", "lat": 34.9667, "lon": 135.3167},

    # 吉野川水系
    "1368084650010": {"name": "早明浦ダム", "pref": "高知県", "river": "吉野川", "lat": 33.8500, "lon": 133.4167},
    "1368084650020": {"name": "池田ダム", "pref": "徳島県", "river": "吉野川", "lat": 33.9833, "lon": 133.8167},

    # 筑後川水系
    "1368085450010": {"name": "松原ダム", "pref": "大分県", "river": "筑後川", "lat": 33.3000, "lon": 131.0667},
    "1368085450020": {"name": "下筌ダム", "pref": "熊本県", "river": "筑後川", "lat": 33.2500, "lon": 131.0000},
    "1368085450030": {"name": "寺内ダム", "pref": "福岡県", "river": "筑後川", "lat": 33.4333, "lon": 130.7667},
}


def create_water_resource_data():
    """
    水資源機構管理ダムのメタデータを作成
    実データはfetch_mlit_dams.pyで取得されるため、ここではメタデータのみ
    """
    output = []

    for station_id, info in WATER_RESOURCE_DAMS.items():
        output.append({
            "station_id": station_id,
            "dam_id": f"water_{station_id}",
            "name": info["name"],
            "pref": info["pref"],
            "river": info["river"],
            "operator_group": "水資源機構",
            "lat": info["lat"],
            "lon": info["lon"],
            "data_source": "川の防災情報（国土交通省）",
        })

    return output


def main():
    """メイン処理"""
    OUT = pathlib.Path("public/data/water_resource_dams.json")

    print("水資源機構管理ダムのメタデータを作成中...")
    data = create_water_resource_data()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n✓ {len(data)}件のダムメタデータを作成しました")
    print(f"✓ {OUT} に保存しました")
    print("\n注: 実際のデータ取得はfetch_mlit_dams.pyで実行されます")


if __name__ == "__main__":
    main()
