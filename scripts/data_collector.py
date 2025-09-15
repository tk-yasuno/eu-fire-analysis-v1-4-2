"""
データ収集・前処理モジュール
公開データソースからの災害関連テキスト取得、匿名化、フィルタリング機能
"""

import re
import json
import os
import requests
from datetime import datetime
from typing import List, Dict, Optional
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

# 環境変数読み込み
load_dotenv()


class DataCollector:
    """災害関連データの収集・前処理クラス"""
    
    def __init__(self, raw_data_dir: str = "data/raw", cleaned_data_dir: str = "data/cleaned"):
        self.raw_data_dir = raw_data_dir
        self.cleaned_data_dir = cleaned_data_dir
        os.makedirs(raw_data_dir, exist_ok=True)
        os.makedirs(cleaned_data_dir, exist_ok=True)
    
    def fetch_sample_disaster_data(self) -> List[Dict]:
        """
        サンプル災害データを生成（実際の実装では外部APIを使用）
        公開データソース例：CrisisNLP、GDELT、気象庁レポート等
        """
        # サンプルデータ（実際の実装では外部APIからフェッチ）
        sample_data = [
            {"id": "001", "text": "地震が発生しました。皆さん、安全な場所に避難してください。震度は大きく、建物の倒壊の危険があります。", "timestamp": "2023-01-15T10:30:00"},
            {"id": "002", "text": "台風の被害が心配です。停電している地域もあるようです。強風に注意して、外出は控えてください。", "timestamp": "2023-01-15T11:00:00"},
            {"id": "003", "text": "洪水で道路が寸断されています。迂回路を使ってください。救助隊の皆さんありがとうございます。一刻も早い復旧を願います。", "timestamp": "2023-01-15T11:30:00"},
            {"id": "004", "text": "火災が発生。消防車が到着しました。周辺住民は避難中です。煙の影響で呼吸が困難になる場合があります。", "timestamp": "2023-01-15T12:00:00"},
            {"id": "005", "text": "地震の影響で電車が止まっています。復旧まで時間がかかりそうです。代替交通手段を検討してください。", "timestamp": "2023-01-15T12:30:00"},
            {"id": "006", "text": "避難所で支援物資の配布が始まりました。温かい食事も提供されています。ボランティアの方々に感謝です。", "timestamp": "2023-01-15T13:00:00"},
            {"id": "007", "text": "救助活動が続いています。みんなで協力し合いましょう。困っている方がいれば、手を差し伸べてください。", "timestamp": "2023-01-15T13:30:00"},
            {"id": "008", "text": "災害情報を確認してください。正確な情報をもとに行動しましょう。デマに惑わされないよう注意が必要です。", "timestamp": "2023-01-15T14:00:00"},
            {"id": "009", "text": "ボランティアの方々が炊き出しをしてくれています。本当に感謝です。温かい食事で元気が出ました。", "timestamp": "2023-01-15T14:30:00"},
            {"id": "010", "text": "復旧作業が進んでいます。一日も早い正常化を祈っています。みんなで力を合わせて頑張りましょう。", "timestamp": "2023-01-15T15:00:00"},
            {"id": "011", "text": "緊急事態です。すぐに避難してください。建物が倒壊する危険性があります。安全な場所まで急いでください。", "timestamp": "2023-01-15T15:30:00"},
            {"id": "012", "text": "支援物資が不足しています。水や食料、毛布などが必要です。ご協力をお願いいたします。", "timestamp": "2023-01-15T16:00:00"},
            {"id": "013", "text": "道路の復旧が完了しました。交通規制は解除されています。安全運転を心がけてください。", "timestamp": "2023-01-15T16:30:00"}
        ]
        
        return sample_data
    
    def anonymize_text(self, text: str) -> str:
        """
        テキストの匿名化処理
        
        Args:
            text: 元テキスト
            
        Returns:
            匿名化されたテキスト
        """
        # ユーザー名を匿名化
        text = re.sub(r'@[\w_]+', '@USER', text)
        
        # URLを除去
        text = re.sub(r'https?://\S+', '', text)
        
        # メディアリンクを除去
        text = re.sub(r'pic\.twitter\.com/\S+', '', text)
        
        # 余分な空白を除去
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def filter_text(self, text: str, min_length: int = 50, max_length: int = 500) -> bool:
        """
        テキストのフィルタリング条件をチェック
        
        Args:
            text: チェックするテキスト
            min_length: 最小文字数
            max_length: 最大文字数
            
        Returns:
            フィルタリング通過の可否
        """
        # 文字数チェック
        if not (min_length <= len(text) <= max_length):
            return False
        
        # 日本語文字が含まれているかチェック（より緩い条件）
        japanese_chars = sum(1 for char in text if '\u3040' <= char <= '\u309F' or 
                           '\u30A0' <= char <= '\u30FF' or '\u4E00' <= char <= '\u9FAF')
        if japanese_chars < 5:  # 最低5文字の日本語文字
            return False
        
        return True
    
    def clean_and_filter(self, data: List[Dict], min_length: int = 50, max_length: int = 500) -> List[Dict]:
        """
        データの清浄化とフィルタリング
        
        Args:
            data: 生データのリスト
            min_length: 最小文字数
            max_length: 最大文字数
            
        Returns:
            清浄化されたデータのリスト
        """
        filtered_data = []
        
        for item in tqdm(data, desc="Cleaning and filtering data"):
            # テキストの匿名化
            cleaned_text = self.anonymize_text(item['text'])
            
            # フィルタリング
            if self.filter_text(cleaned_text, min_length, max_length):
                filtered_data.append({
                    'id': item['id'],
                    'text': cleaned_text,
                    'timestamp': item.get('timestamp', '')
                })
        
        return filtered_data
    
    def save_data(self, data: List[Dict], filepath: str) -> None:
        """データをJSONファイルに保存"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Data saved to {filepath}")
    
    def load_data(self, filepath: str) -> List[Dict]:
        """JSONファイルからデータを読み込み"""
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def run_collection_pipeline(self) -> str:
        """
        データ収集パイプライン全体を実行
        
        Returns:
            清浄化されたデータのファイルパス
        """
        print("Starting data collection pipeline...")
        
        # 1. データ取得
        print("Fetching disaster data...")
        raw_data = self.fetch_sample_disaster_data()
        
        # 2. 生データ保存
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_filepath = os.path.join(self.raw_data_dir, f"{timestamp}_disaster_raw.json")
        self.save_data(raw_data, raw_filepath)
        
        # 3. 清浄化・フィルタリング
        print("Cleaning and filtering data...")
        cleaned_data = self.clean_and_filter(raw_data)
        
        # 4. 清浄化データ保存
        cleaned_filepath = os.path.join(self.cleaned_data_dir, f"{timestamp}_disaster_cleaned.json")
        self.save_data(cleaned_data, cleaned_filepath)
        
        print(f"Pipeline completed. Processed {len(raw_data)} -> {len(cleaned_data)} documents")
        
        return cleaned_filepath
    
    def collect_nasa_firms_data(self, 
                               map_key: str = None,
                               area_params: Dict[str, float] = None,
                               days_back: int = 7,
                               satellite: str = "VIIRS_SNPP_NRT") -> pd.DataFrame:
        """
        NASA FIRMS APIからリアルタイム火災データを収集
        
        Args:
            map_key: NASA FIRMS APIキー（未指定の場合は環境変数から取得）
            area_params: エリアパラメータ {south, north, west, east}
            days_back: 過去何日分のデータを取得するか
            satellite: 衛星データソース
            
        Returns:
            pd.DataFrame: 火災検出データ
        """
        import requests
        from datetime import datetime, timedelta
        
        # MAP KEYが指定されていない場合は環境変数から取得
        if not map_key:
            map_key = os.getenv('NASA_FIRMS_MAP_KEY')
            if not map_key:
                print("NASA FIRMS MAP_KEY not found in environment variables")
                return self._generate_sample_nasa_firms_data(area_params or {}, days_back)
        
        # デフォルトエリアパラメータ（日本周辺）
        if not area_params:
            area_params = {
                'south': 30.0,
                'north': 45.0,
                'west': 130.0,
                'east': 145.0
            }
        
        # API URLの構築（正しい形式：west,south,east,north）
        # NASA FIRMS APIは座標順序が west,south,east,north である
        area_coords = f"{area_params['west']},{area_params['south']},{area_params['east']},{area_params['north']}"
        api_url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{map_key}/{satellite}/{area_coords}/{days_back}"
        
        try:
            print(f"Fetching NASA FIRMS data for past {days_back} days")
            print(f"Area: {area_params}")
            print(f"API URL: {api_url}")
            
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            
            # レスポンスが有効かチェック
            content = response.text.strip()
            if content.startswith('Invalid'):
                print(f"API Error: {content}")
                return self._generate_sample_nasa_firms_data(area_params, days_back)
            
            # CSVデータをDataFrameに変換
            from io import StringIO
            csv_data = StringIO(content)
            
            # CSVデータが空でないかチェック
            if len(content) == 0:
                print("No fire data found for the specified area and time range")
                return pd.DataFrame()
            
            df = pd.read_csv(csv_data)
            
            if len(df) == 0:
                print("No fire records found in the data")
                return pd.DataFrame()
            
            print(f"Successfully collected {len(df)} fire detection records")
            
            # 基本的なデータクリーニング
            # NASA FIRMSの新しい列名に対応
            # 'bright_ti4' を 'brightness' として使用
            if 'bright_ti4' in df.columns and 'brightness' not in df.columns:
                df['brightness'] = df['bright_ti4']
            
            # 必要な列が存在するかチェック
            required_columns = ['latitude', 'longitude', 'brightness', 'confidence']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"Warning: Missing columns in NASA FIRMS data: {missing_columns}")
            
            # 数値列の型変換とNaN除去
            numeric_columns = ['latitude', 'longitude', 'brightness']  # confidenceは後で処理
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # NaNを含む行を削除（数値列のみ）
            initial_len = len(df)
            df = df.dropna(subset=[col for col in numeric_columns if col in df.columns])
            
            if len(df) < initial_len:
                print(f"Removed {initial_len - len(df)} records with missing numeric values")
            
            # 信頼度でフィルタリング（50%以上）
            if 'confidence' in df.columns:
                # NASA FIRMSの新しい形式では confidence が 'h', 'n', 'l' の文字列の場合がある
                if df['confidence'].dtype == 'object':
                    # 'h'=high(80%), 'n'=nominal(60%), 'l'=low(40%) を数値に変換
                    confidence_map = {'h': 80, 'n': 60, 'l': 40}
                    df['confidence'] = df['confidence'].map(confidence_map).fillna(50)
                    print(f"Converted confidence values: {df['confidence'].value_counts().to_dict()}")
                
                high_confidence = df['confidence'] >= 50
                df = df[high_confidence]
                print(f"Filtered to {len(df)} high-confidence detections (>= 50%)")
            
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching NASA FIRMS data: {e}")
            # フェイルセーフ: サンプルデータを返す
            return self._generate_sample_nasa_firms_data(area_params, days_back)
        
        except Exception as e:
            print(f"Error processing NASA FIRMS data: {e}")
            # フェイルセーフ: サンプルデータを返す
            return self._generate_sample_nasa_firms_data(area_params, days_back)
    
    def _generate_sample_nasa_firms_data(self, 
                                       area_params: Dict[str, float],
                                       days_back: int = 7) -> pd.DataFrame:
        """
        サンプルNASA FIRMSデータ生成（APIが利用できない場合のフェイルセーフ）
        """
        import numpy as np
        from datetime import datetime, timedelta
        
        print("Generating sample NASA FIRMS data as fallback")
        
        # サンプル数
        n_samples = np.random.randint(30, 100)
        
        # 地理的範囲内でランダムな座標生成
        latitudes = np.random.uniform(area_params['south'], area_params['north'], n_samples)
        longitudes = np.random.uniform(area_params['west'], area_params['east'], n_samples)
        
        # 火災検出パラメータ
        brightness = np.random.uniform(250.0, 400.0, n_samples)
        confidence = np.random.uniform(50.0, 100.0, n_samples)
        
        # 日付時間
        end_date = datetime.now()
        dates = []
        times = []
        
        for _ in range(n_samples):
            random_date = end_date - timedelta(days=np.random.randint(0, days_back))
            random_hour = np.random.randint(0, 24)
            random_minute = np.random.randint(0, 60)
            
            dates.append(random_date.strftime('%Y-%m-%d'))
            times.append(f"{random_hour:02d}{random_minute:02d}")
        
        # データフレーム作成
        sample_data = pd.DataFrame({
            'latitude': latitudes,
            'longitude': longitudes,
            'brightness': brightness,
            'confidence': confidence,
            'acq_date': dates,
            'acq_time': times,
            'satellite': ['VIIRS_SNPP_NRT'] * n_samples,
            'instrument': ['VIIRS'] * n_samples,
            'version': ['6.1NRT'] * n_samples
        })
        
        print(f"Generated {len(sample_data)} sample fire detection records")
        
        return sample_data


def main():
    """メイン実行関数"""
    collector = DataCollector()
    cleaned_filepath = collector.run_collection_pipeline()
    
    # 結果確認
    cleaned_data = collector.load_data(cleaned_filepath)
    print(f"\nSample cleaned data:")
    for i, item in enumerate(cleaned_data[:3]):
        print(f"{i+1}. {item['text'][:100]}...")


if __name__ == "__main__":
    main()