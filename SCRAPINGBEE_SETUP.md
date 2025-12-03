# ScrapingBee セットアップガイド

国土交通省のIP制限を回避するため、ScrapingBeeプロキシサービスを使用します。

## 1. ScrapingBeeアカウント作成

1. https://www.scrapingbee.com/ にアクセス
2. 「Start Free Trial」をクリック
3. メールアドレスとパスワードで登録
4. **無料プラン**: 月1,000リクエストまで無料

## 2. APIキーの取得

1. ダッシュボードにログイン
2. 左メニューの「API」をクリック
3. 「API Key」をコピー（例: `ABCD1234...`）

## 3. GitHub Secretsに設定

1. GitHubリポジトリページを開く
2. `Settings` タブをクリック
3. 左メニューの `Secrets and variables` → `Actions` をクリック
4. `New repository secret` ボタンをクリック
5. 以下を入力:
   - **Name**: `SCRAPINGBEE_API_KEY`
   - **Secret**: コピーしたAPIキーを貼り付け
6. `Add secret` をクリック

## 4. ワークフローを実行

GitHub Actionsワークフローを実行すると、自動的にScrapingBee経由でアクセスします。

ログに以下のメッセージが表示されれば成功です:
```
✓ ScrapingBee経由でアクセスします（IP制限回避モード）
```

## 料金プラン

- **無料**: 月1,000リクエスト
- **Freelancer**: $49/月 - 150,000クレジット
- **Startup**: $149/月 - 500,000クレジット

### 必要リクエスト数の計算

- ダム数: 約200件
- 1日4回更新: 200 × 4 = 800リクエスト/日
- 月間: 800 × 30 = 24,000リクエスト/月

→ **Freelancerプラン（$49/月）で十分対応可能**

## トラブルシューティング

### APIキーが無効

```
Error: 403 Forbidden
```

→ GitHub Secretsの設定を確認してください

### クレジット不足

```
Error: 429 Too Many Requests
```

→ ScrapingBeeダッシュボードでクレジット残高を確認してください

## ローカルテスト

ローカルでテストする場合:

```bash
export SCRAPINGBEE_API_KEY="your-api-key-here"
python scripts/fetch_all_dams.py
```
