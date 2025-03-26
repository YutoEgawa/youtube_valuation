import streamlit as st
import pandas as pd
import re
from datetime import datetime
import numpy as np
import math
import urllib.parse
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# 🌐 Language switcher
language = st.radio("🌐 Select Language / 言語を選んでください", ("Japanese", "English"))

labels = {
    "Japanese": {
        "title": "YouTube Channel資産価値査定",
        "channelname": "チャンネル名（任意）",
        "upload": "月別・動画別の再生回数をCSV形式でご自身のYoutube Studioから取得し以下にアップロードしてください",
        "short_result": "short動画分類後のデータ",
        "future_views": "将来の再生回数（予測）付きデータ",
        "value_table": "DCF資産価値（動画単位）",
        "channel_value": "📈 チャンネル全体の資産価値：{:,} 円",
        "share": "🕊 Xでシェアする（旧Twitter）",
        "download": "💾 動画ごとの資産価値データをCSVでダウンロード",
        "tweet_text": "私のYouTubeチャンネルの資産価値は {:,} 円でした！📈\n\n#YouTube #資産価値 #動画分析",
        "contact":"📩 ご不明点があれば [your-email@example.com](mailto:your-email@example.com) までお気軽にお問い合わせください。",
        "privacy_policy": "🔐当アプリはアップロードされたCSVファイルを処理のために一時的に使用します。\nユーザーの同意なしに、情報を第三者が閲覧できる状態に置くことはありません。"   
    },
    "English": {
        "title": "YouTube Channel Asset Valuation App",
        "channelname": "Channel Name (optional)",
        "upload": "Upload your monthly view-counts by video downloadable from your YouTube Studio",
        "short_result": "Short Video Classification Result",
        "future_views": "Future Monthly Views (Projected)",
        "value_table": "DCF Asset Value per Video",
        "channel_value": "📈 Total Channel Asset Value: ¥{:,}",
        "share": "🕊 Share on X (Twitter)",
        "download": "💾 Download CSV of Asset Value per Video",
        "tweet_text": "My YouTube channel is worth ¥{:,}! 📈\n\n#YouTube #AssetValuation #CreatorEconomy",
        "contact":"📩 Contact us: [your-email@example.com](mailto:your-email@example.com)" ,
        "privacy_policy":" 🔐This app uses uploaded CSV files only for processing purposes. No data will be shared externally without consent." 
    }
}

text = labels[language]

st.title(text["title"])

# アップロード部分
uploaded_files = st.file_uploader(
    text["upload"],
    type="csv",
    accept_multiple_files=True
)

# 日本語→英語へのカラム名変換マップ（必要な分だけ）
rename_dict = {
    'コンテンツ': 'videoId',
    '動画のタイトル': 'title',
    '動画公開時刻': 'publishedAt',
    '長さ': 'duration',
    '視聴回数': 'viewCount',
    '新しい視聴者数': 'newViewers',
    '登録者増加数': 'subscriberGain',
}

# ファイル名から年月を抽出
def extract_year_month(filename):
    match = re.search(r"(\d{4})[^\d]?(\d{2})", filename)
    if match:
        return match.group(1), match.group(2)
    return None, None

# CSVを処理する関数
def process_csv(file):
    df = pd.read_csv(file)
    df.rename(columns=rename_dict, inplace=True)
    
    # ファイル名から年月取得
    year, month = extract_year_month(file.name)
    if not year or not month:
        st.warning(f"{file.name} から年月を取得できませんでした。スキップします。")
        return None

    # 合計行は除外
    df = df[df["videoId"] != "合計"]

    # 必要なカラムだけ残す
    df = df[['videoId', 'title', 'publishedAt', 'duration', 'viewCount', 'newViewers', 'subscriberGain']]

    # カラム名を年月付きに変更
    df.rename(columns={
        'viewCount': f'viewCount_{year}_{month}',
        'newViewers': f'newViewers_{year}_{month}',
        'subscriberGain': f'subscriberGain_{year}_{month}',
    }, inplace=True)

    return df


# 公開日と動画時間をもとにshortを判定
def classify_short_videos(df):
    cutoff_date = datetime(2024, 10, 15)

    def is_short(row):
        if pd.isna(row['publishedAt']) or pd.isna(row['duration']):
            return 0

        try:
            duration_sec = float(row['duration'])
        except ValueError:
            return 0

        # 公開日が過去か未来かで基準が変わる
        pub_date = pd.to_datetime(row['publishedAt'], errors='coerce')
        if pd.isna(pub_date):
            return 0

        if pub_date < cutoff_date:
            return 1 if duration_sec <= 60 else 0
        else:
            return 1 if duration_sec <= 179 else 0

    df['short'] = df.apply(is_short, axis=1)
    return df

def project_future_views(df, growth_factors=(0.5, 0.4, 0.3)):
    # 月別再生回数カラムを抽出
    view_cols = [col for col in df.columns if col.startswith("viewCount_") and not col.endswith("E")]
    view_cols_sorted = sorted(view_cols)

    # 最終月を取得
    latest_col = view_cols_sorted[-1]
    latest_date_str = latest_col.replace("viewCount_", "")  # '2024_10'
    latest_date = pd.to_datetime(latest_date_str, format="%Y_%m")

    for i, factor in enumerate(growth_factors, start=1):
        future_date = latest_date + pd.DateOffset(months=i)
        future_col = f"viewCount_{future_date.strftime('%Y_%m')}E"

        df[future_col] = df[latest_col] * factor

    return df

def compute_dcf(df, revenue_per_view=0.5, discount_rate=0.1, terminal_multiple=12):
    future_cols = [col for col in df.columns if col.endswith("E")]
    future_cols = sorted(future_cols)

    monthly_discount = math.pow(1 + discount_rate, 1/12) - 1

    for i, col in enumerate(future_cols):
        dcf_col = f"dcf_{col}"
        df[dcf_col] = (df[col] * revenue_per_view) / ((1 + monthly_discount) ** (i + 1))

    if future_cols:
        last_col = future_cols[-1]
        df["terminal_views"] = df[last_col] * terminal_multiple
        df["dcf_terminal"] = (df["terminal_views"] * revenue_per_view) / ((1 + monthly_discount) ** len(future_cols))

    dcf_cols = [col for col in df.columns if col.startswith("dcf_")]
    df["total_dcf_value"] = df[dcf_cols].sum(axis=1)

    return df

def upload_csv_to_drive(file, username="anonymous"):
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    creds = service_account.Credentials.from_service_account_file(
        "credentials.json", scopes=SCOPES
    )
    service = build('drive', 'v3', credentials=creds)

    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{username}_{timestamp}_{file.name}"

    file_data = io.BytesIO(file.getvalue())
    media = MediaIoBaseUpload(file_data, mimetype='text/csv')

    # 🔽 保存先フォルダIDを指定（ここを変更）
    file_metadata = {
        'name': filename,
        'parents': ['1QiQ9s9xj3rCBbNBBauxSTP8sLKogHT1o']  # ← あなたのフォルダIDに置き換え
    }

    uploaded = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    return uploaded.get('id')

username = st.text_input(text["channelname"], value="blank")

if uploaded_files:
    dfs = []
    for file in uploaded_files:
        file_id = upload_csv_to_drive(file, username)
        st.success(f"✅ {file.name} をGoogle Driveにアップロードしました（ID: {file_id}）")
        df = process_csv(file)
        if df is not None:
            dfs.append(df)

    if dfs:
        df_merged = dfs[0]
        for df in dfs[1:]:
            df_merged = pd.merge(df_merged, df, on=['videoId', 'title', 'publishedAt', 'duration'], how='outer')

        # 🔥 short分類の実行
        df_merged = classify_short_videos(df_merged)

        # 🔮 将来3か月分の再生回数を予測
        df_merged = project_future_views(df_merged)

        # 💰 DCFで動画ごとの価値を算出
        df_merged = compute_dcf(df_merged)

        st.subheader(text["value_table"])
        st.dataframe(df_merged[['videoId', 'title', 'total_dcf_value']].sort_values(by='total_dcf_value', ascending=False).head(10))

        # 📊 チャンネル全体の資産価値
        total_channel_value = df_merged["total_dcf_value"].sum()
        st.success(text["channel_value"].format(round(total_channel_value)))

        # 🕊 Xでシェアするリンクを表示
        tweet_text = text["tweet_text"].format(total_channel_value)
        tweet_url = "https://twitter.com/intent/tweet?text=" + urllib.parse.quote(tweet_text)
        st.markdown(f"[{text['share']}]({tweet_url})", unsafe_allow_html=True)

        # 💾 ダウンロード用のCSVを作成
        csv = df_merged[['videoId', 'title', 'total_dcf_value']].to_csv(index=False).encode('utf-8')
        st.download_button(
            label=text["download"],
            data=csv,
            file_name='youtube_asset_valuation.csv',
            mime='text/csv'
        )

st.markdown("---")
st.markdown(text["contact"], unsafe_allow_html=True)

st.markdown(text["privacy_policy"])
