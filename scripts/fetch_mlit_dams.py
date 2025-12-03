#!/usr/bin/env python3
"""
国土交通省 川の防災情報からダムデータを取得
http://www1.river.go.jp/cgi-bin/DspDamData.exe?ID=[観測所記号]&KIND=3
"""
import json
import pathlib
import time
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import urllib.request
import urllib.error
from html.parser import HTMLParser

# 全国の主要ダムの観測所記号リスト（国土交通省・自治体管理）
# 観測所記号は川の防災情報サイトから取得可能
DAM_STATIONS = {
    # ========== 北海道開発局 ==========
    # 石狩川水系
    "1468080550010": {"name": "金山ダム", "pref": "北海道", "river": "石狩川", "operator_group": "国"},
    "1468080550020": {"name": "大雪ダム", "pref": "北海道", "river": "石狩川", "operator_group": "国"},
    "1468080550030": {"name": "忠別ダム", "pref": "北海道", "river": "石狩川", "operator_group": "国"},
    "1468080550040": {"name": "桂沢ダム", "pref": "北海道", "river": "石狩川", "operator_group": "北海道"},
    "1468080550050": {"name": "漁川ダム", "pref": "北海道", "river": "石狩川", "operator_group": "北海道"},
    # 天塩川水系
    "1468080650010": {"name": "岩尾内ダム", "pref": "北海道", "river": "天塩川", "operator_group": "国"},
    # 留萌川水系
    "1468080750010": {"name": "留萌ダム", "pref": "北海道", "river": "留萌川", "operator_group": "北海道"},
    # 常呂川水系
    "1468080850010": {"name": "鹿ノ子ダム", "pref": "北海道", "river": "常呂川", "operator_group": "国"},
    # 沙流川水系
    "1468081050010": {"name": "二風谷ダム", "pref": "北海道", "river": "沙流川", "operator_group": "国"},
    "1468081050020": {"name": "平取ダム", "pref": "北海道", "river": "沙流川", "operator_group": "国"},

    # ========== 東北地方整備局 ==========
    # 北上川水系
    "1368081550010": {"name": "四十四田ダム", "pref": "岩手県", "river": "北上川", "operator_group": "国"},
    "1368081550020": {"name": "御所ダム", "pref": "岩手県", "river": "北上川", "operator_group": "国"},
    "1368081550030": {"name": "田瀬ダム", "pref": "岩手県", "river": "北上川", "operator_group": "国"},
    "1368081550040": {"name": "湯田ダム", "pref": "岩手県", "river": "北上川", "operator_group": "国"},
    "1368081550050": {"name": "石淵ダム", "pref": "岩手県", "river": "北上川", "operator_group": "国"},
    "1368081550060": {"name": "鳴子ダム", "pref": "宮城県", "river": "北上川", "operator_group": "国"},
    "1368081550070": {"name": "花山ダム", "pref": "宮城県", "river": "北上川", "operator_group": "宮城県"},
    # 名取川水系
    "1368081650010": {"name": "釜房ダム", "pref": "宮城県", "river": "名取川", "operator_group": "国"},
    "1368081650020": {"name": "大倉ダム", "pref": "宮城県", "river": "名取川", "operator_group": "宮城県"},
    "1368081650030": {"name": "七ヶ宿ダム", "pref": "宮城県", "river": "名取川", "operator_group": "国"},
    # 阿武隈川水系
    "1368081850010": {"name": "摺上川ダム", "pref": "福島県", "river": "阿武隈川", "operator_group": "国"},
    "1368081850020": {"name": "三春ダム", "pref": "福島県", "river": "阿武隈川", "operator_group": "国"},
    # 最上川水系
    "1368082050010": {"name": "寒河江ダム", "pref": "山形県", "river": "最上川", "operator_group": "国"},
    "1368082050020": {"name": "白川ダム", "pref": "山形県", "river": "最上川", "operator_group": "国"},
    "1368082050030": {"name": "長井ダム", "pref": "山形県", "river": "最上川", "operator_group": "国"},
    # 雄物川水系
    "1368082150010": {"name": "玉川ダム", "pref": "秋田県", "river": "雄物川", "operator_group": "国"},
    "1368082150020": {"name": "皆瀬ダム", "pref": "秋田県", "river": "雄物川", "operator_group": "国"},
    "1368082150030": {"name": "森吉山ダム", "pref": "秋田県", "river": "雄物川", "operator_group": "国"},

    # ========== 関東地方整備局 ==========
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
    # 相模川水系
    "1368080150020": {"name": "宮ヶ瀬ダム", "pref": "神奈川県", "river": "相模川", "operator_group": "国"},
    "1368080150030": {"name": "相模ダム", "pref": "神奈川県", "river": "相模川", "operator_group": "神奈川県"},
    "1368080150040": {"name": "城山ダム", "pref": "神奈川県", "river": "相模川", "operator_group": "神奈川県"},
    # 那珂川水系
    "1368081750010": {"name": "深山ダム", "pref": "栃木県", "river": "那珂川", "operator_group": "栃木県"},

    # ========== 北陸地方整備局 ==========
    # 阿賀野川水系
    "1368082250010": {"name": "奥只見ダム", "pref": "新潟県", "river": "阿賀野川", "operator_group": "民間"},
    "1368082250020": {"name": "大川ダム", "pref": "新潟県", "river": "阿賀野川", "operator_group": "国"},
    "1368082250030": {"name": "鹿瀬ダム", "pref": "新潟県", "river": "阿賀野川", "operator_group": "民間"},
    # 信濃川水系
    "1368082450010": {"name": "奈川渡ダム", "pref": "長野県", "river": "信濃川", "operator_group": "民間"},
    "1368082450020": {"name": "水殿ダム", "pref": "長野県", "river": "信濃川", "operator_group": "民間"},
    "1368082450030": {"name": "大町ダム", "pref": "長野県", "river": "信濃川", "operator_group": "国"},
    # 黒部川水系
    "1368082850010": {"name": "宇奈月ダム", "pref": "富山県", "river": "黒部川", "operator_group": "国"},
    "1368082850020": {"name": "黒部ダム", "pref": "富山県", "river": "黒部川", "operator_group": "民間"},
    # 常願寺川水系
    "1368082750010": {"name": "有峰ダム", "pref": "富山県", "river": "常願寺川", "operator_group": "民間"},
    "1368082750020": {"name": "真立ダム", "pref": "富山県", "river": "常願寺川", "operator_group": "民間"},
    # 手取川水系
    "1368082950010": {"name": "手取川ダム", "pref": "石川県", "river": "手取川", "operator_group": "国"},
    "1368082950020": {"name": "九谷ダム", "pref": "石川県", "river": "手取川", "operator_group": "石川県"},

    # ========== 中部地方整備局 ==========
    # 富士川水系
    "1368081150010": {"name": "雨畑ダム", "pref": "山梨県", "river": "富士川", "operator_group": "民間"},
    # 天竜川水系
    "1368081250010": {"name": "平岡ダム", "pref": "長野県", "river": "天竜川", "operator_group": "民間"},
    "1368081250020": {"name": "美和ダム", "pref": "長野県", "river": "天竜川", "operator_group": "国"},
    "1368081250030": {"name": "小渋ダム", "pref": "長野県", "river": "天竜川", "operator_group": "国"},
    # 大井川水系
    "1368081150020": {"name": "畑薙第一ダム", "pref": "静岡県", "river": "大井川", "operator_group": "民間"},
    "1368081150030": {"name": "井川ダム", "pref": "静岡県", "river": "大井川", "operator_group": "民間"},
    "1368081150040": {"name": "長島ダム", "pref": "静岡県", "river": "大井川", "operator_group": "国"},
    # 矢作川水系
    "1368081350010": {"name": "矢作ダム", "pref": "愛知県", "river": "矢作川", "operator_group": "愛知県"},
    "1368081350020": {"name": "羽布ダム", "pref": "愛知県", "river": "矢作川", "operator_group": "愛知県"},
    "1368081350030": {"name": "黒田ダム", "pref": "愛知県", "river": "矢作川", "operator_group": "愛知県"},
    # 木曽川水系
    "1368081450010": {"name": "丸山ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},
    "1368081450020": {"name": "阿木川ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},
    "1368081450030": {"name": "岩屋ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},
    "1368081450040": {"name": "徳山ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},
    "1368081450050": {"name": "横山ダム", "pref": "岐阜県", "river": "木曽川", "operator_group": "国"},
    "1368081450060": {"name": "牧尾ダム", "pref": "長野県", "river": "木曽川", "operator_group": "水資源機構"},

    # ========== 近畿地方整備局 ==========
    # 淀川水系
    "1368083350010": {"name": "天ヶ瀬ダム", "pref": "京都府", "river": "淀川", "operator_group": "国"},
    "1368083350020": {"name": "高山ダム", "pref": "京都府", "river": "淀川", "operator_group": "国"},
    "1368083350030": {"name": "日吉ダム", "pref": "京都府", "river": "淀川", "operator_group": "国"},
    "1368083350040": {"name": "比奈知ダム", "pref": "三重県", "river": "淀川", "operator_group": "国"},
    "1368083350050": {"name": "室生ダム", "pref": "奈良県", "river": "淀川", "operator_group": "国"},
    "1368083350060": {"name": "布目ダム", "pref": "奈良県", "river": "淀川", "operator_group": "国"},
    "1368083350070": {"name": "一庫ダム", "pref": "兵庫県", "river": "淀川", "operator_group": "国"},
    "1368083350080": {"name": "琵琶湖", "pref": "滋賀県", "river": "淀川", "operator_group": "国"},
    # 紀の川水系
    "1368083550010": {"name": "大滝ダム", "pref": "奈良県", "river": "紀の川", "operator_group": "国"},
    "1368083550020": {"name": "猿谷ダム", "pref": "奈良県", "river": "紀の川", "operator_group": "国"},
    # 円山川水系
    "1368083750010": {"name": "大路ダム", "pref": "兵庫県", "river": "円山川", "operator_group": "国"},

    # ========== 中国地方整備局 ==========
    # 斐伊川水系
    "1368084050010": {"name": "尾原ダム", "pref": "島根県", "river": "斐伊川", "operator_group": "国"},
    "1368084050020": {"name": "志津見ダム", "pref": "島根県", "river": "斐伊川", "operator_group": "国"},
    # 江の川水系
    "1368083950010": {"name": "土師ダム", "pref": "広島県", "river": "江の川", "operator_group": "国"},
    "1368083950020": {"name": "灰塚ダム", "pref": "島根県", "river": "江の川", "operator_group": "国"},
    # 高梁川水系
    "1368084150010": {"name": "河本ダム", "pref": "岡山県", "river": "高梁川", "operator_group": "岡山県"},
    "1368084150020": {"name": "新成羽川ダム", "pref": "岡山県", "river": "高梁川", "operator_group": "国"},
    # 吉井川水系
    "1368084250010": {"name": "苫田ダム", "pref": "岡山県", "river": "吉井川", "operator_group": "国"},
    # 芦田川水系
    "1368084350010": {"name": "八田原ダム", "pref": "広島県", "river": "芦田川", "operator_group": "国"},
    # 太田川水系
    "1368084450010": {"name": "温井ダム", "pref": "広島県", "river": "太田川", "operator_group": "国"},
    "1368084450020": {"name": "土師ダム", "pref": "広島県", "river": "太田川", "operator_group": "国"},

    # ========== 四国地方整備局 ==========
    # 吉野川水系
    "1368084650010": {"name": "早明浦ダム", "pref": "高知県", "river": "吉野川", "operator_group": "国"},
    "1368084650020": {"name": "池田ダム", "pref": "徳島県", "river": "吉野川", "operator_group": "国"},
    "1368084650030": {"name": "富郷ダム", "pref": "愛媛県", "river": "吉野川", "operator_group": "国"},
    "1368084650040": {"name": "柳瀬ダム", "pref": "愛媛県", "river": "吉野川", "operator_group": "国"},
    "1368084650050": {"name": "新宮ダム", "pref": "愛媛県", "river": "吉野川", "operator_group": "国"},
    # 那賀川水系
    "1368084750010": {"name": "長安口ダム", "pref": "徳島県", "river": "那賀川", "operator_group": "国"},
    # 仁淀川水系
    "1368084850010": {"name": "大渡ダム", "pref": "高知県", "river": "仁淀川", "operator_group": "高知県"},
    # 肱川水系
    "1368085050010": {"name": "鹿野川ダム", "pref": "愛媛県", "river": "肱川", "operator_group": "国"},
    "1368085050020": {"name": "野村ダム", "pref": "愛媛県", "river": "肱川", "operator_group": "国"},

    # ========== 九州地方整備局 ==========
    # 筑後川水系
    "1368085450010": {"name": "松原ダム", "pref": "大分県", "river": "筑後川", "operator_group": "国"},
    "1368085450020": {"name": "下筌ダム", "pref": "熊本県", "river": "筑後川", "operator_group": "国"},
    "1368085450030": {"name": "寺内ダム", "pref": "福岡県", "river": "筑後川", "operator_group": "国"},
    "1368085450040": {"name": "江川ダム", "pref": "福岡県", "river": "筑後川", "operator_group": "水資源機構"},
    "1368085450050": {"name": "大山ダム", "pref": "大分県", "river": "筑後川", "operator_group": "国"},
    # 遠賀川水系
    "1368085350010": {"name": "遠賀川河口堰", "pref": "福岡県", "river": "遠賀川", "operator_group": "国"},
    # 嘉瀬川水系
    "1368085550010": {"name": "嘉瀬川ダム", "pref": "佐賀県", "river": "嘉瀬川", "operator_group": "国"},
    # 本明川水系
    "1368085650010": {"name": "本明川ダム", "pref": "長崎県", "river": "本明川", "operator_group": "国"},
    # 緑川水系
    "1368085750010": {"name": "緑川ダム", "pref": "熊本県", "river": "緑川", "operator_group": "国"},
    "1368085750020": {"name": "竜門ダム", "pref": "熊本県", "river": "緑川", "operator_group": "国"},
    # 球磨川水系
    "1368085850010": {"name": "市房ダム", "pref": "熊本県", "river": "球磨川", "operator_group": "熊本県"},
    # 大分川水系
    "1368085950010": {"name": "大分川ダム", "pref": "大分県", "river": "大分川", "operator_group": "国"},
    # 大淀川水系
    "1368086050010": {"name": "高崎ダム", "pref": "宮崎県", "river": "大淀川", "operator_group": "宮崎県"},
    # 川内川水系
    "1368086150010": {"name": "鶴田ダム", "pref": "鹿児島県", "river": "川内川", "operator_group": "国"},
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

    # ScrapingBee APIキーが設定されている場合はプロキシ経由でアクセス
    scrapingbee_api_key = os.environ.get('SCRAPINGBEE_API_KEY')

    try:
        if scrapingbee_api_key:
            # ScrapingBee経由でアクセス（IP制限回避）
            import urllib.parse
            scrapingbee_url = 'https://app.scrapingbee.com/api/v1/'
            params = {
                'api_key': scrapingbee_api_key,
                'url': url,
                'render_js': 'false',
                'premium_proxy': 'true',
                'country_code': 'jp'
            }
            scrapingbee_request_url = f"{scrapingbee_url}?{urllib.parse.urlencode(params)}"

            req = urllib.request.Request(scrapingbee_request_url)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                html = response.read().decode('shift_jis', errors='ignore')
        else:
            # 直接アクセス（従来の方法）
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
