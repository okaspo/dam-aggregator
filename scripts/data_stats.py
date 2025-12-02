#!/usr/bin/env python3
"""
データ統計情報の表示
"""
import json
import pathlib
import gzip
from datetime import datetime
from collections import defaultdict

HIST_DIR = pathlib.Path("public/data/history")
ARCHIVE_DIR = pathlib.Path("public/data/archive")


def analyze_history():
    """履歴データの統計"""
    print("📊 履歴データ統計")
    print("=" * 60)

    if not HIST_DIR.exists():
        print("履歴ディレクトリがありません")
        return

    total_files = 0
    total_records = 0
    total_size = 0
    dam_stats = {}

    for ndjson_file in HIST_DIR.glob("*.ndjson"):
        dam_id = ndjson_file.stem
        file_size = ndjson_file.stat().st_size
        total_size += file_size

        lines = ndjson_file.read_text(encoding="utf-8").strip().split('\n')
        record_count = len([l for l in lines if l.strip()])

        total_files += 1
        total_records += record_count

        if record_count > 0:
            # 最古・最新の日時を取得
            records = [json.loads(l) for l in lines if l.strip()]
            dates = [r.get("observed_at") for r in records if r.get("observed_at")]
            if dates:
                dam_stats[dam_id] = {
                    "records": record_count,
                    "size_kb": file_size / 1024,
                    "oldest": min(dates),
                    "newest": max(dates)
                }

    print(f"ダム数: {total_files}")
    print(f"総レコード数: {total_records:,}")
    print(f"総サイズ: {total_size / 1024 / 1024:.2f} MB")
    print()

    # 上位5ダムを表示
    if dam_stats:
        print("📈 データ量上位5ダム:")
        sorted_dams = sorted(dam_stats.items(), key=lambda x: x[1]["records"], reverse=True)
        for dam_id, stats in sorted_dams[:5]:
            print(f"  {dam_id}: {stats['records']:,}件 ({stats['size_kb']:.1f} KB)")
            print(f"    期間: {stats['oldest'][:10]} 〜 {stats['newest'][:10]}")
        print()


def analyze_archives():
    """アーカイブデータの統計"""
    print("📦 アーカイブデータ統計")
    print("=" * 60)

    if not ARCHIVE_DIR.exists() or not list(ARCHIVE_DIR.glob("*.ndjson.gz")):
        print("アーカイブがありません")
        return

    total_files = 0
    total_size = 0
    total_records = 0
    monthly_stats = defaultdict(lambda: {"files": 0, "records": 0, "size_mb": 0})

    for archive_file in ARCHIVE_DIR.glob("*.ndjson.gz"):
        file_size = archive_file.stat().st_size
        total_size += file_size
        total_files += 1

        # ファイル名から月を抽出
        parts = archive_file.stem.replace('.ndjson', '').split('_')
        if len(parts) >= 2:
            month = parts[-1]  # YYYY-MM
        else:
            month = "不明"

        # レコード数をカウント
        try:
            with gzip.open(archive_file, 'rt', encoding='utf-8') as f:
                record_count = sum(1 for line in f if line.strip())
                total_records += record_count

                monthly_stats[month]["files"] += 1
                monthly_stats[month]["records"] += record_count
                monthly_stats[month]["size_mb"] += file_size / 1024 / 1024
        except Exception as e:
            print(f"  警告: {archive_file.name} の読み込みエラー: {e}")

    print(f"アーカイブファイル数: {total_files}")
    print(f"総レコード数: {total_records:,}")
    print(f"総サイズ: {total_size / 1024 / 1024:.2f} MB")
    print(f"圧縮率: {total_size / max(1, total_records) / 1024:.2f} KB/レコード")
    print()

    # 月別統計
    if monthly_stats:
        print("📅 月別統計:")
        for month in sorted(monthly_stats.keys(), reverse=True)[:6]:
            stats = monthly_stats[month]
            print(f"  {month}: {stats['files']}ファイル, {stats['records']:,}レコード, {stats['size_mb']:.2f} MB")
        print()


def analyze_coverage():
    """データカバレッジの分析"""
    print("🔍 データカバレッジ")
    print("=" * 60)

    # latest.jsonから現在登録されているダム数を取得
    latest_file = pathlib.Path("public/data/latest.json")
    if latest_file.exists():
        latest_data = json.loads(latest_file.read_text(encoding="utf-8"))
        registered_dams = set(r["dam_id"] for r in latest_data.get("records", []))
        print(f"登録ダム数: {len(registered_dams)}")

        # 履歴データがあるダム
        if HIST_DIR.exists():
            history_dams = set(f.stem for f in HIST_DIR.glob("*.ndjson"))
            print(f"履歴データあり: {len(history_dams)}")
            print(f"データカバレッジ: {len(history_dams) / max(1, len(registered_dams)) * 100:.1f}%")

            # 履歴データがないダム
            no_history = registered_dams - history_dams
            if no_history:
                print(f"\n履歴データなし ({len(no_history)}件):")
                for dam_id in sorted(no_history)[:10]:
                    print(f"  - {dam_id}")
                if len(no_history) > 10:
                    print(f"  ... 他 {len(no_history) - 10}件")
        print()


def main():
    """メイン処理"""
    print("\n" + "=" * 60)
    print("ダムデータ統計情報")
    print("=" * 60)
    print()

    analyze_history()
    print()
    analyze_archives()
    print()
    analyze_coverage()
    print()

    print("=" * 60)
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
