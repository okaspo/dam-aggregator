#!/usr/bin/env python3
"""
各自治体管理ダムのデータ取得
主要都道府県の自治体管理ダムを追加
"""
import json
import pathlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List

# 各自治体管理ダム（river.go.jpにデータがあるもの）
LOCAL_DAMS = {
    # 北海道
    "1468082350010": {"name": "桂沢ダム", "pref": "北海道", "river": "石狩川", "operator_group": "北海道", "lat": 43.2167, "lon": 142.0333},
    "1468082350020": {"name": "金山ダム", "pref": "北海道", "river": "石狩川", "operator_group": "北海道", "lat": 43.6667, "lon": 142.5333},
    "1468082350030": {"name": "大雪ダム", "pref": "北海道", "river": "石狩川", "operator_group": "北海道", "lat": 43.7000, "lon": 142.9167},

    # 宮城県
    "1368081650010": {"name": "釜房ダム", "pref": "宮城県", "river": "名取川", "operator_group": "国", "lat": 38.2833, "lon": 140.7000},
    "1368081650020": {"name": "七ヶ宿ダム", "pref": "宮城県", "river": "阿武隈川", "operator_group": "国", "lat": 38.0333, "lon": 140.3667},

    # 山形県
    "1368082050010": {"name": "寒河江ダム", "pref": "山形県", "river": "最上川", "operator_group": "国", "lat": 38.4333, "lon": 140.2000},
    "1368082050020": {"name": "白川ダム", "pref": "山形県", "river": "最上川", "operator_group": "国", "lat": 38.0667, "lon": 140.1333},

    # 福島県
    "1368081850010": {"name": "摺上川ダム", "pref": "福島県", "river": "阿武隈川", "operator_group": "国", "lat": 37.7833, "lon": 140.3333},
    "1368081850020": {"name": "三春ダム", "pref": "福島県", "river": "阿武隈川", "operator_group": "国", "lat": 37.4333, "lon": 140.5333},

    # 新潟県
    "1368082250010": {"name": "奥只見ダム", "pref": "新潟県", "river": "阿賀野川", "operator_group": "民間", "lat": 37.3667, "lon": 139.2167},
    "1368082250020": {"name": "大川ダム", "pref": "新潟県", "river": "阿賀野川", "operator_group": "国", "lat": 37.5500, "lon": 139.4833},

    # 長野県
    "1368082450010": {"name": "奈川渡ダム", "pref": "長野県", "river": "信濃川", "operator_group": "民間", "lat": 36.2000, "lon": 137.7333},
    "1368082450020": {"name": "水殿ダム", "pref": "長野県", "river": "信濃川", "operator_group": "民間", "lat": 36.2333, "lon": 137.7667},

    # 静岡県
    "1368081250010": {"name": "畑薙第一ダム", "pref": "静岡県", "river": "大井川", "operator_group": "民間", "lat": 35.3667, "lon": 138.2167},
    "1368081250020": {"name": "井川ダム", "pref": "静岡県", "river": "大井川", "operator_group": "民間", "lat": 35.3167, "lon": 138.2667},

    # 愛知県
    "1368081350010": {"name": "矢作ダム", "pref": "愛知県", "river": "矢作川", "operator_group": "愛知県", "lat": 35.1833, "lon": 137.3667},
    "1368081350020": {"name": "羽布ダム", "pref": "愛知県", "river": "矢作川", "operator_group": "愛知県", "lat": 35.1500, "lon": 137.4500},

    # 兵庫県
    "1368083650010": {"name": "生野ダム", "pref": "兵庫県", "river": "市川", "operator_group": "兵庫県", "lat": 35.1833, "lon": 134.6833},
    "1368083650020": {"name": "引原ダム", "pref": "兵庫県", "river": "市川", "operator_group": "兵庫県", "lat": 35.1667, "lon": 134.6167},

    # 岡山県
    "1368084150010": {"name": "苫田ダム", "pref": "岡山県", "river": "吉井川", "operator_group": "国", "lat": 35.1333, "lon": 134.1167},
    "1368084150020": {"name": "河本ダム", "pref": "岡山県", "river": "高梁川", "operator_group": "岡山県", "lat": 34.8333, "lon": 133.4667},

    # 広島県
    "1368084350010": {"name": "八田原ダム", "pref": "広島県", "river": "芦田川", "operator_group": "国", "lat": 34.6833, "lon": 133.0167},
    "1368084350020": {"name": "温井ダム", "pref": "広島県", "river": "太田川", "operator_group": "国", "lat": 34.6500, "lon": 132.2333},

    # 山口県
    "1368084550010": {"name": "菅野ダム", "pref": "山口県", "river": "椹野川", "operator_group": "山口県", "lat": 34.2167, "lon": 131.4500},

    # 香川県
    "1368084850010": {"name": "満濃池", "pref": "香川県", "river": "土器川", "operator_group": "香川県", "lat": 34.1833, "lon": 133.9167},

    # 愛媛県
    "1368085050010": {"name": "鹿野川ダム", "pref": "愛媛県", "river": "肱川", "operator_group": "国", "lat": 33.5667, "lon": 132.7833},
    "1368085050020": {"name": "野村ダム", "pref": "愛媛県", "river": "肱川", "operator_group": "国", "lat": 33.4833, "lon": 132.7167},

    # 高知県
    "1368084750010": {"name": "大渡ダム", "pref": "高知県", "river": "仁淀川", "operator_group": "高知県", "lat": 33.5833, "lon": 133.1167},

    # 福岡県
    "1368085350010": {"name": "油木ダム", "pref": "福岡県", "river": "遠賀川", "operator_group": "福岡県", "lat": 33.7167, "lon": 130.7833},

    # 長崎県
    "1468085750010": {"name": "本河内低部ダム", "pref": "長崎県", "river": "本河内川", "operator_group": "長崎市", "lat": 32.7500, "lon": 129.8667},

    # 熊本県
    "1368085850010": {"name": "市房ダム", "pref": "熊本県", "river": "球磨川", "operator_group": "熊本県", "lat": 32.3500, "lon": 130.8833},

    # 大分県
    "1368085650010": {"name": "耶馬渓ダム", "pref": "大分県", "river": "山国川", "operator_group": "大分県", "lat": 33.4833, "lon": 131.1333},

    # 宮崎県
    "1368086150010": {"name": "一ツ瀬ダム", "pref": "宮崎県", "river": "一ツ瀬川", "operator_group": "民間", "lat": 32.2833, "lon": 131.2167},

    # 鹿児島県
    "1468086350010": {"name": "鶴田ダム", "pref": "鹿児島県", "river": "川内川", "operator_group": "国", "lat": 32.0333, "lon": 130.6333},
}


def main():
    """メイン処理"""
    OUT = pathlib.Path("public/data/local_dams.json")

    print("各自治体管理ダムのメタデータを作成中...")

    output = []
    for station_id, info in LOCAL_DAMS.items():
        output.append({
            "station_id": station_id,
            "dam_id": f"local_{station_id}",
            "name": info["name"],
            "pref": info["pref"],
            "river": info.get("river", ""),
            "operator_group": info["operator_group"],
            "lat": info["lat"],
            "lon": info["lon"],
            "data_source": "川の防災情報（国土交通省）",
        })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n✓ {len(output)}件のダムメタデータを作成しました")
    print(f"✓ {OUT} に保存しました")
    print("\n注: 実際のデータ取得はfetch_mlit_dams.pyで実行されます")


if __name__ == "__main__":
    main()
