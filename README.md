# ダムウォッチ - 日本全国ダム貯水量モニタリングサイト

日本全国のダムの貯水量をリアルタイムで表示・監視するWebアプリケーションです。

## 特徴

- 🗾 **日本全国のダムを網羅**
  - 国土交通省管理ダム
  - 水資源機構管理ダム
  - 都道府県・自治体管理ダム
  - 約100以上のダムをカバー

- 📊 **リアルタイム監視**
  - 貯水率、水位、流入量、放流量
  - 24時間変化率の表示
  - 時系列グラフ表示

- 🗺️ **地図表示**
  - MapLibre GL による地図表示
  - 貯水率による色分け
  - クリックで詳細情報表示

- 🔍 **高度なフィルタリング**
  - ダム名・河川名検索
  - 都道府県別フィルタ
  - 貯水率範囲指定
  - 管理体制別フィルタ
  - 上昇・下降フィルタ

- ⚠️ **アラート機能**
  - 低水位警告（貯水率 < 30%）
  - 急激な減少警告（24h変化 < -5%）
  - 急激な増加警告（24h変化 > +10%）

- 📥 **データエクスポート**
  - CSV形式でエクスポート
  - JSON形式でエクスポート

## アーキテクチャ

### フロントエンド
- 純粋なHTML/CSS/JavaScript
- MapLibre GL（地図表示）
- Chart.js（グラフ描画）
- レスポンシブデザイン

### バックエンド
- Python 3.12
- データソース：国土交通省 川の防災情報
- GitHub Actions による自動データ取得（1日4回）
- GitHub Pages による静的サイトホスティング

### データパイプライン

```
川の防災情報
     ↓
fetch_all_dams.py (データ取得)
     ↓
append_history.py (履歴蓄積)
     ↓
build_index.py (インデックス構築)
     ↓
GitHub Pages (公開)
```

## ディレクトリ構造

```
dam-aggregator/
├── .github/
│   └── workflows/
│       └── ingest.yml          # データ取得自動化
├── public/
│   ├── index.html              # メインページ
│   └── data/
│       ├── latest.json         # 最新データ
│       ├── dams_index.json     # インデックス
│       └── history/            # 履歴データ（NDJSON）
│           └── {dam_id}.ndjson
├── scripts/
│   ├── fetch_all_dams.py       # 全ダムデータ取得
│   ├── fetch_mlit_dams.py      # 国交省ダム取得
│   ├── fetch_water_dams.py     # 水資源機構メタデータ
│   ├── fetch_local_dams.py     # 自治体ダムメタデータ
│   ├── dam_locations.json      # ダム位置情報
│   ├── append_history.py       # 履歴追記
│   └── build_index.py          # インデックス構築
└── README.md
```

## セットアップ

### 必要要件
- Python 3.12+
- Git

### ローカル開発

1. リポジトリをクローン

```bash
git clone https://github.com/yourusername/dam-aggregator.git
cd dam-aggregator
```

2. データ取得

```bash
python scripts/fetch_all_dams.py
python scripts/append_history.py
python scripts/build_index.py
```

3. ローカルサーバー起動

```bash
cd public
python -m http.server 8000
```

4. ブラウザで `http://localhost:8000` を開く

### GitHub Pages でのデプロイ

1. GitHub リポジトリ設定で Pages を有効化
2. Source を `gh-pages` ブランチに設定
3. GitHub Actions が自動的にデータを取得し、デプロイします

自動実行スケジュール：
- JST 06:05
- JST 12:05
- JST 18:05
- JST 23:05

手動実行：Actions タブから `workflow_dispatch` を使用

## データソース

### 主要データソース
- [国土交通省 川の防災情報](http://www1.river.go.jp/)
- [水資源機構 リアルタイム水源情報](https://www.water.go.jp/honsya/honsya/suigen/realtime/index.html)
- [国土交通省 全国のダム貯水情報](https://www.mlit.go.jp/mizukokudo/mizsei/mizukokudo_mizsei_fr2_000008.html)

### 対象ダム
- **利根川水系**：矢木沢、奈良俣、藤原、相俣、薗原、下久保、草木、渡良瀬貯水池、八ッ場
- **荒川水系**：二瀬、浦山、滝沢、有間
- **木曽川水系**：岩屋、徳山、丸山、阿木川
- **淀川水系**：天ヶ瀬、高山、日吉、比奈知、室生、布目、一庫
- **吉野川水系**：早明浦、池田
- **筑後川水系**：松原、下筌、寺内
- **その他主要水系**のダム多数

## 開発

### 新しいダムの追加

1. `scripts/fetch_mlit_dams.py` または `scripts/fetch_local_dams.py` の該当辞書にダム情報を追加

```python
"観測所記号": {
    "name": "ダム名",
    "pref": "都道府県",
    "river": "河川名",
    "operator_group": "管理体制",
}
```

2. `scripts/dam_locations.json` に位置情報を追加

```json
{
  "観測所記号": {"lat": 緯度, "lon": 経度}
}
```

### データ取得間隔の変更

`.github/workflows/ingest.yml` の `cron` を編集：

```yaml
schedule:
  - cron: '5 21,3,9,14 * * *'  # UTC時刻
```

## ライセンス

このプロジェクトは公開情報を利用していますが、各データソースの利用規約を遵守してください。

### データ利用規約
- 国土交通省の公開データを利用
- 各自治体の公開データ利用規約に準拠
- 商用利用前に必ず各データソースの規約を確認してください

## 貢献

プルリクエスト、イシュー報告を歓迎します！

### 貢献方法
1. Fork する
2. Feature ブランチを作成 (`git checkout -b feature/amazing-feature`)
3. 変更をコミット (`git commit -m 'Add some amazing feature'`)
4. ブランチにプッシュ (`git push origin feature/amazing-feature`)
5. プルリクエストを作成

## サポート

イシューやバグ報告は [GitHub Issues](https://github.com/yourusername/dam-aggregator/issues) までお願いします。

## 謝辞

- データ提供：国土交通省、水資源機構、各自治体
- 地図タイル：国土地理院
- 参考プロジェクト：[damapi](https://github.com/jacopen/damapi)

## 免責事項

このサイトの情報は参考値です。観測・取得時刻のずれや欠測がある場合があります。
正確な情報は各管理機関の公式サイトをご確認ください。

---

**ダムウォッチ** - 日本のダムを見守る
