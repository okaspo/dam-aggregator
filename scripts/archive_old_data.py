#!/usr/bin/env python3
"""
古いデータのアーカイブと管理
- 90日以上経過したデータを月次アーカイブに移動
- アーカイブデータは圧縮保存
"""
import json
import pathlib
import gzip
from datetime import datetime, timezone, timedelta
from collections import defaultdict

HIST_DIR = pathlib.Path("public/data/history")
ARCHIVE_DIR = pathlib.Path("public/data/archive")
RETENTION_DAYS = 90  # 直近データ保持期間


def parse_observed_at(date_str: str) -> datetime:
    """ISO形式の日時文字列をdatetimeに変換"""
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except:
        return None


def archive_old_data():
    """古いデータをアーカイブ"""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    print(f"アーカイブ対象: {cutoff_date.date()} より前のデータ")

    total_archived = 0
    total_kept = 0

    for ndjson_file in HIST_DIR.glob("*.ndjson"):
        dam_id = ndjson_file.stem

        # データを読み込み
        lines = ndjson_file.read_text(encoding="utf-8").strip().split('\n')
        records = [json.loads(line) for line in lines if line.strip()]

        # 日付でグループ分け
        recent_records = []
        archive_by_month = defaultdict(list)

        for rec in records:
            obs_dt = parse_observed_at(rec.get("observed_at", ""))
            if not obs_dt:
                recent_records.append(rec)
                continue

            if obs_dt >= cutoff_date:
                # 直近データ
                recent_records.append(rec)
                total_kept += 1
            else:
                # アーカイブ対象
                month_key = obs_dt.strftime("%Y-%m")
                archive_by_month[month_key].append(rec)
                total_archived += 1

        # 直近データを書き戻し
        if recent_records:
            ndjson_file.write_text(
                '\n'.join(json.dumps(r, ensure_ascii=False) for r in recent_records) + '\n',
                encoding="utf-8"
            )
        else:
            # データがない場合はファイルを削除
            ndjson_file.unlink()

        # 月次アーカイブを圧縮保存
        for month, records in archive_by_month.items():
            archive_file = ARCHIVE_DIR / f"{dam_id}_{month}.ndjson.gz"

            # 既存アーカイブがあれば読み込んで結合
            existing_records = []
            if archive_file.exists():
                with gzip.open(archive_file, 'rt', encoding='utf-8') as f:
                    existing_records = [json.loads(line) for line in f if line.strip()]

            # 結合して重複排除（observed_atでユニーク化）
            all_records = existing_records + records
            unique_records = {}
            for r in all_records:
                obs_at = r.get("observed_at")
                if obs_at:
                    unique_records[obs_at] = r

            # 時系列順にソートして保存
            sorted_records = sorted(unique_records.values(), key=lambda x: x.get("observed_at", ""))

            # gzip圧縮して保存
            with gzip.open(archive_file, 'wt', encoding='utf-8') as f:
                for r in sorted_records:
                    f.write(json.dumps(r, ensure_ascii=False) + '\n')

            print(f"  ✓ {dam_id} {month}: {len(records)}件 → {archive_file}")

    print(f"\nアーカイブ完了:")
    print(f"  保持: {total_kept}件")
    print(f"  アーカイブ: {total_archived}件")


def list_archives():
    """アーカイブの一覧を表示"""
    if not ARCHIVE_DIR.exists():
        print("アーカイブがありません")
        return

    archives = list(ARCHIVE_DIR.glob("*.ndjson.gz"))
    if not archives:
        print("アーカイブがありません")
        return

    print(f"\nアーカイブファイル: {len(archives)}件")
    total_size = sum(f.stat().st_size for f in archives)
    print(f"合計サイズ: {total_size / 1024 / 1024:.2f} MB")


def main():
    """メイン処理"""
    print("=" * 60)
    print("古いデータのアーカイブ処理")
    print("=" * 60)
    print()

    archive_old_data()
    list_archives()


if __name__ == "__main__":
    main()
