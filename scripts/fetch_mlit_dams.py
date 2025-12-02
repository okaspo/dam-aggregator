#!/usr/bin/env python3
"""
国土交通省 川の防災情報からダムデータを取得
http://www1.river.go.jp/cgi-bin/DspDamData.exe?ID=[観測所記号]&KIND=3
"""
import json
import pathlib
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import urllib.request
import urllib.error
from html.parser import HTMLParser

# 主要ダムの観測所記号リスト（国土交通省管理ダム）
# 観測所記号は川の防災情報サイトから取得可能
DAM_STATIONS = {
    # 利根川水系
    "1368080700010": {"name": "矢木沢ダム", "pref": "群馬県", "river": "利根川", "operator_group": "国"},
    "1368080700020": {"name": "奈良俣ダム", "pref": "群馬県", "river": "利根川", "operator_group": "国"},
    "1368080700030": {"name": "藤原ダム", "pref": "群馬県", "river": "利根川", "operator_group": "国"},
    "1368080700040": {"name": "相俣ダム", "pref": "群馬県", "river": "利根川", "operator_group": "国"},
    "1368080700050": {"name": "薗原ダム", "pref": "群馬県", "river": "利根川", "operator_group": "国"},
    "1368080700060": {"name": "下久保ダム", "pref": "群馬県", "river": "利根川", "operator_group": "国"},
    "1368080700070": {"name": "草木ダム", "pref": "群馬県", "river": "利根川", "operator_group": "国"},
    "1368080700080": {"name": "渡良瀬貯水池", "pref": "栃木県", "river": "利根川", "operator_group": "国"},
    "1368080700090": {"name": "八ッ場ダム", "pref": "群馬県", "river": "利根川", "operator_group": "国"},

    # 荒川水系
    "1368080750010": {"name": "二瀬ダム", "pref": "埼玉県", "river": "荒川", "operator_group": "国"},
    "1368080750020": {"name": "浦山ダム", "pref": "埼玉県", "river": "荒川", "operator_group": "国"},
    "1368080750030": {"name": "滝沢ダム", "pref": "埼玉県", "river": "荒川", "operator_group": "国"},
    "1368080750040": {"name": "有間ダム", "pref": "埼玉県", "river": "荒川", "operator_group": "国"},

    # 木曽川水系
    "1368081450010": {"name": "丸山ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},
    "1368081450020": {"name": "阿木川ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},
    "1368081450030": {"name": "岩屋ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},
    "1368081450040": {"name": "徳山ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},

    # 淀川水系
    "1368083350010": {"name": "天ヶ瀬ダム", "pref": "京都府", "river": "淀川", "operator_group": "国"},
    "1368083350020": {"name": "高山ダム", "pref": "京都府", "river": "淀川", "operator_group": "国"},
    "1368083350030": {"name": "日吉ダム", "pref": "京都府", "river": "淀川", "operator_group": "国"},
    "1368083350040": {"name": "比奈知ダム", "pref": "三重県", "river": "淀川", "operator_group": "国"},
    "1368083350050": {"name": "室生ダム", "pref": "奈良県", "river": "淀川", "operator_group": "国"},
    "1368083350060": {"name": "布目ダム", "pref": "奈良県", "river": "淀川", "operator_group": "国"},
    "1368083350070": {"name": "一庫ダム", "pref": "兵庫県", "river": "淀川", "operator_group": "国"},

    # 吉野川水系
    "1368084650010": {"name": "早明浦ダム", "pref": "高知県", "river": "吉野川", "operator_group": "国"},
    "1368084650020": {"name": "池田ダム", "pref": "徳島県", "river": "吉野川", "operator_group": "国"},

    # 筑後川水系
    "1368085450010": {"name": "松原ダム", "pref": "大分県", "river": "筑後川", "operator_group": "国"},
    "1368085450020": {"name": "下筌ダム", "pref": "熊本県", "river": "筑後川", "operator_group": "国"},
    "1368085450030": {"name": "寺内ダム", "pref": "福岡県", "river": "筑後川", "operator_group": "国"},

    # 球磨川水系
    "1368085850010": {"name": "市房ダム", "pref": "熊本県", "river": "球磨川", "operator_group": "熊本県"},

    # その他主要ダム
    "1368080150020": {"name": "宮ヶ瀬ダム", "pref": "神奈川県", "river": "相模川", "operator_group": "国"},
    "1368082850010": {"name": "宇奈月ダム", "pref": "富山県", "river": "黒部川", "operator_group": "国"},
    "1368082950010": {"name": "手取川ダム", "pref": "石川県", "river": "手取川", "operator_group": "国"},
}


class DamDataParser(HTMLParser):
    """川の防災情報のHTMLをパースしてダムデータを抽出"""

    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_row = []
        self.data_rows = []
        self.cell_data = ""

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.in_table = True
        elif tag == "tr" and self.in_table:
            self.in_row = True
            self.current_row = []
        elif tag == "td" and self.in_row:
            self.in_cell = True
            self.cell_data = ""

    def handle_endtag(self, tag):
        if tag == "table":
            self.in_table = False
        elif tag == "tr" and self.in_row:
            if self.current_row:
                self.data_rows.append(self.current_row)
            self.in_row = False
        elif tag == "td" and self.in_cell:
            self.current_row.append(self.cell_data.strip())
            self.in_cell = False

    def handle_data(self, data):
        if self.in_cell:
            self.cell_data += data


def fetch_dam_data(station_id: str, timeout: int = 30) -> Optional[Dict]:
    """
    指定された観測所記号のダムデータを取得

    Args:
        station_id: 観測所記号
        timeout: タイムアウト秒数

    Returns:
        ダムデータの辞書、取得失敗時はNone
    """
    url = f"http://www1.river.go.jp/cgi-bin/DspDamData.exe?ID={station_id}&KIND=3"

    try:
        # User-Agentヘッダーを追加してブラウザからのアクセスに見せかける
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        req = urllib.request.Request(url, headers=headers)

        with urllib.request.urlopen(req, timeout=timeout) as response:
            html = response.read().decode('shift_jis', errors='ignore')

        parser = DamDataParser()
        parser.feed(html)

        # データ行から最新の情報を抽出
        if not parser.data_rows:
            return None

        # 最新行（通常は最後の行）からデータを取得
        latest_row = None
        for row in reversed(parser.data_rows):
            if len(row) >= 5 and row[0]:  # 日時データがある行
                latest_row = row
                break

        if not latest_row:
            return None

        # データ抽出（カラム位置はサイトのHTMLに依存）
        result = {
            "observed_at": parse_datetime(latest_row[0]) if len(latest_row) > 0 else None,
            "level_m": parse_float(latest_row[1]) if len(latest_row) > 1 else None,
            "storage_m3": parse_float(latest_row[2]) if len(latest_row) > 2 else None,
            "rate_pct": parse_float(latest_row[3]) if len(latest_row) > 3 else None,
            "inflow_m3s": parse_float(latest_row[4]) if len(latest_row) > 4 else None,
            "outflow_m3s": parse_float(latest_row[5]) if len(latest_row) > 5 else None,
        }

        return result

    except urllib.error.URLError as e:
        print(f"Error fetching {station_id}: {e}")
        return None
    except Exception as e:
        print(f"Error parsing {station_id}: {e}")
        return None


def parse_float(value: str) -> Optional[float]:
    """文字列を浮動小数点数に変換"""
    try:
        cleaned = value.strip().replace(',', '').replace('　', '')
        if cleaned == '' or cleaned == '-' or cleaned == '---':
            return None
        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def parse_datetime(value: str) -> Optional[str]:
    """日時文字列をISO形式に変換"""
    try:
        # 例: "2025/12/02 10:00" -> "2025-12-02T10:00:00+09:00"
        cleaned = value.strip()
        if not cleaned or cleaned == '-':
            return None

        # 日本時間として解釈
        dt = datetime.strptime(cleaned, "%Y/%m/%d %H:%M")
        jst = timezone(timedelta(hours=9))
        dt_jst = dt.replace(tzinfo=jst)
        return dt_jst.isoformat()
    except (ValueError, AttributeError):
        return None


def load_dam_locations() -> Dict:
    """ダムの位置情報を読み込み"""
    location_file = pathlib.Path(__file__).parent / "dam_locations.json"
    if location_file.exists():
        return json.loads(location_file.read_text(encoding="utf-8"))
    return {}


def fetch_all_dams() -> List[Dict]:
    """全ダムのデータを取得"""
    records = []
    locations = load_dam_locations()

    for station_id, info in DAM_STATIONS.items():
        print(f"Fetching {info['name']}... ", end='', flush=True)

        data = fetch_dam_data(station_id)

        if data and data.get("observed_at"):
            # 位置情報を取得
            loc = locations.get(station_id, {"lat": 35.6895, "lon": 139.6917})

            record = {
                "dam_id": f"mlit_{station_id}",
                "name": info["name"],
                "pref": info["pref"],
                "river": info.get("river", ""),
                "operator_group": info["operator_group"],
                "lat": loc["lat"],
                "lon": loc["lon"],
                "observed_at": data["observed_at"],
                "rate_pct": data.get("rate_pct"),
                "level_m": data.get("level_m"),
                "storage_m3": data.get("storage_m3"),
                "inflow_m3s": data.get("inflow_m3s"),
                "outflow_m3s": data.get("outflow_m3s"),
            }
            records.append(record)
            print("OK")
        else:
            print("Failed")

        # サーバーに負荷をかけないよう待機
        time.sleep(1)

    return records


def main():
    """メイン処理"""
    OUT = pathlib.Path("public/data/latest.json")

    print("国土交通省ダムデータを取得中...")
    records = fetch_all_dams()

    out = {
        "fetched_at": datetime.now(timezone(timedelta(hours=9))).isoformat(),
        "records": records
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n✓ {len(records)}件のダムデータを取得しました")
    print(f"✓ {OUT} に保存しました")


if __name__ == "__main__":
    main()
