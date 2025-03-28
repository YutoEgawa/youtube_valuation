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
import json

creds_dict = json.loads(st.secrets["gcp_service_account"])
creds = service_account.Credentials.from_service_account_info(creds_dict)

# 🌐 Language switcher
language = st.radio("🌐 Select Language / 言語を選んでください", ("Japanese", "English"))

labels = {
    "Japanese": {
        "title": "YouTube Channel資産価値査定",
        "channelname": "チャンネル名（任意）",
        "upload": "月別・動画別の再生回数をCSV形式でご自身のYoutube Studioから取得し以下にアップロードしてください。過去６ヶ月分のファイル、つまり６つのCSVファイルをアップロードいただければ最も正確な査定結果をご提供いたします。",
        "short_result": "short動画分類後のデータ",
        "future_views": "将来の再生回数（予測）付きデータ",
        "value_table": "DCF資産価値（動画単位）",
        "channel_value": "📈 チャンネル全体の資産価値：{:,} 円",
        "share": "🕊 Xでシェアする（旧Twitter）",
        "download": "💾 動画ごとの資産価値データをCSVでダウンロード",
        "tweet_text": "私のYouTubeチャンネルの資産価値は {:,} 円でした！📈\n\n#YouTube #資産価値 #動画分析",
        "contact":"📩 ご不明点があれば [your-email@example.com](mailto:your-email@example.com) までお気軽にお問い合わせください。",
        "privacy_policy": "🔐当アプリはアップロードされたCSVファイルを処理のために一時的に使用します。\nユーザーの同意なしに、情報を第三者が閲覧できる状態に置くことはありません。" ,  
        "analyzing": "📊 CSVファイルを分析しています..." ,  
        "finished": "どうですか？" ,  
        "no_date_file": "⚠️ CSVファイルの名前から年月の抽出に失敗しました。ファイル名はYYYY_MMに統一してください。" , 
        "no_avail_file": "❗️有効なファイルがありません。", 
    },

    "English": {
        "title": "YouTube Channel Asset Valuation App",
        "channelname": "Channel Name (optional)",
        "upload": "Upload your monthly view-counts by video downloadable from your YouTube Studio. To offer most accurate result, please upload the latest 6 months data, i.e. 6 csv files below.",
        "short_result": "Short Video Classification Result",
        "future_views": "Future Monthly Views (Projected)",
        "value_table": "DCF Asset Value per Video",
        "channel_value": "📈 Total Channel Asset Value: ¥{:,}",
        "share": "🕊 Share on X (Twitter)",
        "download": "💾 Download CSV of Asset Value per Video",
        "tweet_text": "My YouTube channel is worth ¥{:,}! 📈\n\n#YouTube #AssetValuation #CreatorEconomy",
        "contact":"📩 Contact us: [your-email@example.com](mailto:your-email@example.com)" ,
        "privacy_policy":" 🔐This app uses uploaded CSV files only for processing purposes. No data will be shared externally without consent." ,
        "analyzing": "📊 analyzing CSV file..." ,  
        "finished": "how do you find this service?" ,  
        "no_date_file": "⚠️ failed to extract date from your CSV file name . Please format the file name as 'YYYY_MM'." , 
        "no_avail_file": "❗️no files available", 
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
    
    # ✅ 自動判定：日本語のカラムが存在するか確認
    is_japanese = any(col in rename_dict for col in df.columns)

    # 🎯 日本語の場合のみカラム名変換
    if is_japanese:
        df.rename(columns=rename_dict, inplace=True)

    # ファイル名から年月取得
    year, month = extract_year_month(file.name)
    if not year or not month:
        st.warning(f"⚠️ {file.name} から年月を取得できませんでした。スキップします。")
        return None

    # ✅ 合計行は除外
    df = df[df["videoId"] != "合計"]

    # ✅ 必要なカラムだけ残す
    target_cols = ['videoId', 'title', 'publishedAt', 'duration', 'viewCount']
    # 🎯 存在するカラムだけ残す（日本語・英語どちらでも動作）
    df = df[[col for col in target_cols if col in df.columns]]

    # ✅ カラム名を年月付きに変更（英語でもそのまま適用）
    df.rename(columns={
        'viewCount': f'viewCount_{year}_{month}',
    }, inplace=True)

    return df

# Function to parse publishedAt with multiple formats
def parse_published_at(date_str):
    for fmt in ('%Y-%m-%dT%H:%M:%SZ', '%b %d, %Y'):  # Supports both formats
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None  # Return None if no format matches

# Function to classify videos as "short" or not
def is_short(row):
    if row['publishedAt'] is None or pd.isna(row['duration']):
        return 0  # Handle missing or invalid dates/durations

    try:
        duration_sec = float(row['duration'])  # Convert duration to a number (seconds)
    except ValueError:
        return 0  # If duration is not numeric, assume it's not a short

    # Define the classification rules
    cutoff_date = datetime(2024, 10, 15)

    if row['publishedAt'] < cutoff_date:
        return 1 if duration_sec <= 60 else 0
    else:
        return 1 if duration_sec <= 179 else 0  # 2 minutes 59 seconds = 179 seconds
def project_future_views(df_merged, growth_slowdown_1st, growth_slowdown_2nd, growth_slowdown_steady, growth_speculation):
    """
    Projects future monthly view counts based on historical data.

    Parameters:
        df_merged (pd.DataFrame): The DataFrame containing video view counts.
        growth_slowdown_1st (float): Growth slowdown one month after publish.
        growth_slowdown_2nd (float): Growth slowdown two months after publish.
        growth_slowdown_steady (float): Steady-state growth.
        growth_speculation (float): Additional speculative growth for acquisitions.

    Returns:
        pd.DataFrame: The DataFrame with projected view counts added.
    """

    # Identify view count columns
    monthly_view_count_columns = [col for col in df_merged.columns if col.startswith("viewCount_")]

    # Count valid (non-null) monthly view counts
    num_valid_values = df_merged[monthly_view_count_columns].notna().sum(axis=1)

    # Extract the latest date from column names
    latest_date_str = "_".join(monthly_view_count_columns[-1].split("_")[1:])
    latest_date = pd.to_datetime(latest_date_str, format="%Y_%m")  # Convert to datetime

    # Iterate through each row and project future view counts
    for index, valid_count in enumerate(num_valid_values):
        if valid_count == 1:
            base_col = monthly_view_count_columns[-1]
            base_value = df_merged.loc[index, base_col]

            projected_value = base_value * growth_slowdown_1st  # First projection
            for i, growth_factor in enumerate([growth_slowdown_1st, growth_slowdown_2nd, growth_slowdown_steady], start=1):
                future_date = latest_date + pd.DateOffset(months=i)
                future_col = f"viewCount_{future_date.strftime('%Y_%m')}E"
                df_merged.loc[index, future_col] = projected_value
                projected_value *= growth_factor  # Apply the next growth factor sequentially

        elif valid_count == 2:
            base_col1 = monthly_view_count_columns[-2]
            base_col2 = monthly_view_count_columns[-1]

            if pd.notna(df_merged.loc[index, base_col1]) and pd.notna(df_merged.loc[index, base_col2]):
                projected_value = df_merged.loc[index, base_col2] * growth_slowdown_2nd  # First projection

                for i, growth_factor in enumerate([growth_slowdown_2nd, growth_slowdown_steady, growth_slowdown_steady], start=1):
                    future_date = latest_date + pd.DateOffset(months=i)
                    future_col = f"viewCount_{future_date.strftime('%Y_%m')}E"
                    df_merged.loc[index, future_col] = projected_value
                    projected_value *= growth_factor  # Apply the next growth factor sequentially

        elif 3 <= valid_count <= 5:
            base_col1 = monthly_view_count_columns[-3]
            base_col2 = monthly_view_count_columns[-2]
            base_col3 = monthly_view_count_columns[-1]

            if all(pd.notna(df_merged.loc[index, [base_col1, base_col2, base_col3]])):
                growth_rate_1 = (df_merged.loc[index, base_col2] / df_merged.loc[index, base_col1]) - 1
                growth_rate_2 = (df_merged.loc[index, base_col3] / df_merged.loc[index, base_col2]) - 1
                avg_growth_rate = (growth_rate_1 + growth_rate_2) / 2
                projected_value = df_merged.loc[index, base_col3] * (1 + avg_growth_rate + growth_speculation)  # First projection

                for i in range(1, 4):
                    future_date = latest_date + pd.DateOffset(months=i)
                    future_col = f"viewCount_{future_date.strftime('%Y_%m')}E"
                    df_merged.loc[index, future_col] = projected_value
                    projected_value *= (1 + avg_growth_rate + growth_speculation)  # Apply compounded growth sequentially

        elif valid_count >= 6:
            base_col1 = monthly_view_count_columns[-3]
            base_col2 = monthly_view_count_columns[-2]
            base_col3 = monthly_view_count_columns[-1]

            if all(pd.notna(df_merged.loc[index, [base_col1, base_col2, base_col3]])):
                ave_base_col = (df_merged.loc[index, base_col1] + df_merged.loc[index, base_col2] + df_merged.loc[index, base_col3]) / 3
                projected_value = ave_base_col * (1 + growth_speculation)  # First projection

                for i in range(1, 4):
                    future_date = latest_date + pd.DateOffset(months=i)
                    future_col = f"viewCount_{future_date.strftime('%Y_%m')}E"
                    df_merged.loc[index, future_col] = projected_value
                    projected_value *= (1 + growth_speculation)  # Apply compounded growth sequentially

    return df_merged  # Return updated DataFrame

def compute_dcf(df_merged, discount_rate, revenue_per_view, terminal_multiple, running_cost):
    """
    Computes Discounted Cash Flow (DCF) for projected data and calculates average total DCF ViewCount for the last 3 months.

    Parameters:
        df_merged (pd.DataFrame): The DataFrame containing projected view counts.
        discount_rate (float): Annual discount rate (default = 10%).
        revenue_per_view (float): Revenue per view (default = 0.5).
        terminal_multiple (float): Terminal multiple for final view count estimation (default = 12).
        running_cost (float): Business running cost (default = 300,000).

    Returns:
        pd.DataFrame: The updated DataFrame with DCF calculations.
        float: The average total DCF ViewCount over the last 3 months.
    """

    # Identify view count columns
    monthly_view_count_columns = [col for col in df_merged.columns if col.startswith("viewCount_")]

    # Count valid (non-null) monthly view counts
    num_valid_values = df_merged[monthly_view_count_columns].notna().sum(axis=1)

    # Extract the latest date from column names
    latest_date_str = "_".join(monthly_view_count_columns[-4].split("_")[1:])
    latest_date = pd.to_datetime(latest_date_str, format="%Y_%m")  # Convert to dateti
    
    # Convert yearly discount rate to monthly discount rate
    monthly_discount_rate = math.pow(1 + discount_rate, 1/12) - 1

    # Extract projected columns
    dcf_columns = [col for col in df_merged.columns if col.endswith("E")]

    # Compute discounted cash flows
    for t, col in enumerate(dcf_columns):
        dcf_key = f"dcf_{col}"
        df_merged[dcf_key] = (df_merged[col] * revenue_per_view) / ((1 + monthly_discount_rate) ** (t + 1))

    # Calculate terminal view count based on the last projected column
    if dcf_columns:
        last_projected_col = dcf_columns[2]  # Using the 3rd projected month
        df_merged["terminal_viewCount"] = df_merged[last_projected_col] * terminal_multiple

        # Discount terminal view count using the last projected discount period
        df_merged["dcf_terminal_viewCount"] = (df_merged["terminal_viewCount"] * revenue_per_view) / ((1 + monthly_discount_rate) ** 3)

    # Sum up all discounted view counts
    dcf_sum_columns = [col for col in df_merged.columns if col.startswith("dcf_")]
    df_merged["total_dcf_viewCount"] = df_merged[dcf_sum_columns].sum(axis=1)

    # Step 1: Identify videos published in the last 3 months
    three_months_ago = latest_date - pd.DateOffset(months=2)

    # Convert publication date column to datetime if not already
    df_merged["publishedAt"] = pd.to_datetime(df_merged["publishedAt"])

    # Filter videos published in the last 3 months
    recent_videos = df_merged[df_merged["publishedAt"] >= three_months_ago]

    # Step 2: Calculate the average total_dcf_viewCount across those videos
    avg_dcf_viewCount_3M = recent_videos["total_dcf_viewCount"].mean() if not recent_videos.empty else 0

    return df_merged, avg_dcf_viewCount_3M

def valuation(df_merged, growth_slowdown_1st, growth_slowdown_2nd, growth_slowdown_steady, growth_speculation,discount_rate, revenue_per_view, terminal_multiple, running_cost, new_publish):
  df_merged = project_future_views(df_merged, growth_slowdown_1st, growth_slowdown_2nd, growth_slowdown_steady, growth_speculation)
  df_merged, avg_dcf_viewCount_3M = compute_dcf(df_merged, discount_rate, revenue_per_view, terminal_multiple, running_cost)

  df_short = df_merged[df_merged["short"] == 0]
  total_dcf_sum_filtered = df_short["total_dcf_viewCount"].sum() + avg_dcf_viewCount_3M * new_publish

  return round(total_dcf_sum_filtered)

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
    # 📂 アップロードされたファイルを一時保存
    file_data_list = []

    # 🎯 アップロード進捗バーを追加
    progress_bar = st.progress(0)
    total_files = len(uploaded_files)
    
    # 📢 ステータスメッセージの初期化
    status_msg = st.empty()
    # 📢 進捗中のメッセージ
    status_msg.info(text["analyzing"])
    
    # ファイルを処理して一時保存
    for i, file in enumerate(uploaded_files):
        
        # 🎯 Google Drive にファイルをアップロード
        file_id = upload_csv_to_drive(file, username)
        
        # 📚 進捗状況を更新
        progress_percentage = (i + 1) / total_files
        progress_bar.progress(progress_percentage)

        # ファイル名から年月を抽出
        year, month = extract_year_month(file.name)

        # 年月が抽出できた場合のみ処理を続ける
        if year and month:
            df = process_csv(file)
            if df is not None:
                # 📚 年月とデータフレームを一時保存
                file_data_list.append({"year": int(year), "month": int(month), "df": df})
        else:
            st.warning(text["no_date_file"])

    # ✅ すべてのアップロード完了後のメッセージ
    progress_bar.empty()  # ゲージバーを消す
   
    # 📅 年月順（昇順）でソート
    file_data_list = sorted(file_data_list, key=lambda x: (x["year"], x["month"]))

    # 📊 データフレームを順番にマージ
    if file_data_list:
        df_merged = file_data_list[0]["df"]
        for file_data in file_data_list[1:]:
            df_merged = pd.merge(
                df_merged,
                file_data["df"],
                on=['videoId', 'title', 'publishedAt', 'duration'],
                how='outer'
            )

        # 🔥 short分類の実行
        df_merged['publishedAt'] = df_merged['publishedAt'].astype(str).apply(parse_published_at)
        df_merged['short'] = df_merged.apply(is_short, axis=1)

        # 📊 チャンネル全体の資産価値
        total_channel_value = valuation(df_merged,
          growth_slowdown_1st=0.23,
          growth_slowdown_2nd=0.44,
          growth_slowdown_steady=1,
          growth_speculation=0.2,
          discount_rate=0.1,
          revenue_per_view=0.5,
          terminal_multiple=12,
          running_cost=300000,
          new_publish=12
        )

        st.success(text["channel_value"].format(round(total_channel_value)))
        
        # 📢 結果発表後のメッセージ
        status_msg.info(text["finished"])

        # 🕊 Xでシェアするリンクを表示
        tweet_text = text["tweet_text"].format(total_channel_value)
        tweet_url = "https://twitter.com/intent/tweet?text=" + urllib.parse.quote(tweet_text)
        st.markdown(f"[{text['share']}]({tweet_url})", unsafe_allow_html=True)

        # 💾 ダウンロード用のCSVを作成
        csv = df_merged[['title', 'total_dcf_viewCount']].to_csv(index=False).encode('utf-8')
        st.download_button(
            label=text["download"],
            data=csv,
            file_name='youtube_asset_valuation.csv',
            mime='text/csv'
        )
    else:
        st.error(text["no_avail_file"])


st.markdown("---")
st.markdown(text["contact"], unsafe_allow_html=True)

st.markdown(text["privacy_policy"])

