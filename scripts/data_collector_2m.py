#!/usr/bin/env python3
"""
災害感情分析システム - 200万文書データコレクター for v1-0スケーリングテスト
2M Document Collector for Scaling Test (v0-7 based)
v1-0開発用の2倍スケーリング検証データ生成
"""

import gc
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Any
import psutil
from tqdm import tqdm

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Data2MConfig:
    """200万文書対応データ生成設定"""
    target_documents: int = 2_000_000  # 200万文書
    batch_size: int = 10000  # 1万文書/バッチ
    chunk_size: int = 100000  # 10万文書/チャンク（v0-7と同サイズ）
    workers: int = 16  # v0-7の12から増強
    output_dir: str = "data_v1-0"
    max_memory_gb: float = 12.0  # メモリ制限
    
    # 品質設定（v0-7ベース改良）
    sentiments: List[str] = None
    locations: List[str] = None
    fire_types: List[str] = None
    
    def __post_init__(self):
        # v0-7から拡張された感情カテゴリ（14種類）
        self.sentiments = [
            "不安", "恐怖", "怒り", "悲しみ", "絶望", 
            "希望", "安堵", "感謝", "連帯", "決意",
            "困惑", "焦燥", "警戒", "冷静"  # 2種類追加
        ]
        
        # グローバル災害発生地点（v0-7の153から200箇所に拡張）
        self.locations = [
            # 日本（詳細化）
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
            "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
            "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
            "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
            
            # 北米（拡張）
            "カリフォルニア州", "テキサス州", "フロリダ州", "ニューヨーク州", "ペンシルベニア州",
            "イリノイ州", "オハイオ州", "ジョージア州", "ノースカロライナ州", "ミシガン州",
            "ニュージャージー州", "バージニア州", "ワシントン州", "アリゾナ州", "マサチューセッツ州",
            "テネシー州", "インディアナ州", "メリーランド州", "ミズーリ州", "ウィスコンシン州",
            "コロラド州", "ミネソタ州", "サウスカロライナ州", "アラバマ州", "ルイジアナ州",
            "ケンタッキー州", "オレゴン州", "オクラホマ州", "コネチカット州", "ユタ州",
            
            # カナダ
            "ブリティッシュコロンビア州", "アルバータ州", "サスカチュワン州", "マニトバ州",
            "オンタリオ州", "ケベック州", "ニューブランズウィック州", "ノバスコシア州",
            "プリンスエドワードアイランド州", "ニューファンドランド・ラブラドール州",
            
            # オーストラリア
            "ニューサウスウェールズ州", "ビクトリア州", "クイーンズランド州", "西オーストラリア州",
            "南オーストラリア州", "タスマニア州", "ノーザンテリトリー", "オーストラリア首都特別地域",
            
            # ヨーロッパ（主要国）
            "ドイツ・バイエルン州", "ドイツ・ノルトライン＝ヴェストファーレン州", "フランス・プロヴァンス",
            "スペイン・アンダルシア州", "イタリア・シチリア島", "イタリア・トスカーナ州",
            "ギリシャ・アッティカ州", "ポルトガル・アレンテージョ州", "英国・スコットランド",
            "英国・ウェールズ", "スウェーデン・イェータランド地方", "ノルウェー・南ノルウェー",
            
            # 南米
            "ブラジル・アマゾナス州", "ブラジル・サンパウロ州", "ブラジル・リオデジャネイロ州",
            "アルゼンチン・ブエノスアイレス州", "チリ・サンティアゴ首都州", "ペルー・リマ州",
            "コロンビア・アンティオキア県", "ベネズエラ・カラカス首都地区",
            
            # アジア（拡張）
            "中国・四川省", "中国・雲南省", "中国・新疆ウイグル自治区", "中国・内モンゴル自治区",
            "インド・ウッタラーカンド州", "インド・ヒマーチャル・プラデーシュ州", "インド・マハーラーシュトラ州",
            "インドネシア・カリマンタン島", "インドネシア・スマトラ島", "マレーシア・サバ州",
            "タイ・チェンマイ県", "フィリピン・ミンダナオ島", "韓国・江原道", "モンゴル・ウランバートル",
            
            # アフリカ
            "南アフリカ・西ケープ州", "南アフリカ・クワズール・ナタール州", "ケニア・リフトバレー州",
            "タンザニア・アルーシャ州", "モロッコ・カサブランカ＝セタット地方", "アルジェリア・アルジェ県",
            "エジプト・カイロ県", "ナイジェリア・ラゴス州", "ガーナ・アシャンティ州",
            
            # 中東
            "トルコ・アンタルヤ県", "トルコ・ムーラ県", "イスラエル・ハイファ地区",
            "レバノン・南レバノン県", "ヨルダン・アンマン県", "イラン・テヘラン州",
            
            # その他の地域
            "ロシア・シベリア連邦管区", "ロシア・極東連邦管区", "ニュージーランド・カンタベリー地方",
            "フィジー・ビティレブ島", "パプアニューギニア・ポートモレスビー"
        ]
        
        # 火災タイプ（v0-7の60+から80+に拡張）
        self.fire_types = [
            # 森林火災
            "針葉樹林火災", "広葉樹林火災", "混交林火災", "竹林火災", "原生林火災",
            "人工林火災", "二次林火災", "防風林火災", "海岸林火災", "高山林火災",
            
            # 草地・農地火災
            "草原火災", "牧草地火災", "農地火災", "休耕地火災", "畑作地火災",
            "水田火災", "果樹園火災", "茶畑火災", "花畑火災", "ビニールハウス火災",
            
            # 都市・住宅火災
            "住宅密集地火災", "高層建築火災", "商業施設火災", "工業地帯火災", "倉庫火災",
            "学校火災", "病院火災", "文化財火災", "宗教施設火災", "公共施設火災",
            
            # 交通・インフラ火災
            "道路沿い火災", "鉄道沿線火災", "高速道路火災", "トンネル火災", "橋梁火災",
            "空港火災", "港湾火災", "発電所火災", "変電所火災", "通信施設火災",
            
            # 自然災害関連
            "地震後火災", "津波後火災", "台風関連火災", "落雷火災", "火山噴火火災",
            "土砂崩れ後火災", "洪水後火災", "竜巻後火災", "雹害後火災", "干ばつ火災",
            
            # 産業・特殊火災
            "石油化学火災", "ガス施設火災", "化学工場火災", "金属工場火災", "食品工場火災",
            "繊維工場火災", "製紙工場火災", "リサイクル施設火災", "廃棄物処理場火災", "下水処理場火災",
            
            # 特殊環境火災
            "湿地火災", "砂漠火災", "氷河地帯火災", "洞窟火災", "地下火災",
            "炭鉱火災", "油田火災", "ガス田火災", "塩田火災", "採石場火災",
            
            # 新分類（v1-0追加）
            "バイオマス火災", "太陽光発電所火災", "風力発電所火災", "データセンター火災",
            "物流センター火災", "コンテナ火災", "船舶火災", "航空機火災",
            "宇宙施設火災", "研究施設火災", "核施設周辺火災", "軍事施設火災",
            "観光地火災", "温泉地火災", "スキー場火災", "ゴルフ場火災",
            "キャンプ場火災", "国立公園火災", "動物園火災", "植物園火災"
        ]

class WildfireDataCollector2M:
    """200万文書対応災害感情データコレクター"""
    
    def __init__(self, config: Data2MConfig):
        self.config = config
        self.process = psutil.Process()
        
        # 出力ディレクトリ作成
        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # 統計カウンター
        self.stats = {
            "total_generated": 0,
            "chunks_created": 0,
            "start_time": None,
            "sentiment_distribution": {},
            "location_distribution": {},
            "fire_type_distribution": {}
        }
        
        logger.info(f"Initialized 2M Document Collector for v1-0")
        logger.info(f"Target: {config.target_documents:,} documents")
        logger.info(f"Workers: {config.workers}, Batch: {config.batch_size:,}, Chunk: {config.chunk_size:,}")
        logger.info(f"Quality: {len(config.sentiments)} sentiments, {len(config.locations)} locations")

    def _log_memory_usage(self, step_name: str):
        """メモリ使用量ログ"""
        memory_gb = self.process.memory_info().rss / 1024**3
        logger.info(f"{step_name} - Memory: {memory_gb:.2f}GB")
        
        if memory_gb > self.config.max_memory_gb:
            logger.warning(f"High memory usage: {memory_gb:.2f}GB")
            gc.collect()

    def _generate_enhanced_document(self) -> Dict[str, Any]:
        """v1-0向け高品質文書生成"""
        sentiment = random.choice(self.config.sentiments)
        location = random.choice(self.config.locations)
        fire_type = random.choice(self.config.fire_types)
        
        # 統計更新
        self.stats["sentiment_distribution"][sentiment] = self.stats["sentiment_distribution"].get(sentiment, 0) + 1
        self.stats["location_distribution"][location] = self.stats["location_distribution"].get(location, 0) + 1
        self.stats["fire_type_distribution"][fire_type] = self.stats["fire_type_distribution"].get(fire_type, 0) + 1
        
        # v1-0向け高品質テンプレート
        templates = [
            f"{location}で{fire_type}が発生し、住民は{sentiment}を感じている。消防活動が継続中で、避難指示が出されている。",
            f"{fire_type}により{location}の広範囲が被害を受け、多くの人々が{sentiment}の感情を抱いている。",
            f"{location}での{fire_type}により、地域住民は{sentiment}を覚え、支援活動への参加を表明している。",
            f"{fire_type}の影響で{location}では避難が続いており、被災者は{sentiment}な気持ちで復旧を待っている。",
            f"{location}で発生した{fire_type}に対し、救助隊は{sentiment}を胸に懸命な救助活動を行っている。",
            f"{fire_type}による{location}の被害状況を見て、多くの市民が{sentiment}の気持ちを共有している。",
            f"{location}での{fire_type}に関する報道を受け、全国から{sentiment}の声と支援が寄せられている。",
            f"{fire_type}の脅威にさらされた{location}の住民は{sentiment}を乗り越え、結束を強めている。"
        ]
        
        base_text = random.choice(templates)
        
        # 複雑度・品質スコア計算（v0-7改良版）
        complexity_score = min(1.0, (len(base_text) + len(sentiment) + len(location) + len(fire_type)) / 200.0)
        confidence = 0.7 + (complexity_score * 0.3) + random.uniform(-0.1, 0.1)
        confidence = max(0.0, min(1.0, confidence))
        
        # 地理座標（より詳細）
        latitude = round(random.uniform(-60, 70), 6)
        longitude = round(random.uniform(-180, 180), 6)
        
        # v1-0追加メタデータ
        severity = random.choice(["軽微", "中程度", "深刻", "極めて深刻"])
        urgency = random.choice(["低", "中", "高", "最高"])
        
        return {
            "text": base_text,
            "sentiment": sentiment,
            "location": location,
            "fire_type": fire_type,
            "latitude": latitude,
            "longitude": longitude,
            "confidence": round(confidence, 3),
            "complexity_score": round(complexity_score, 3),
            "severity": severity,
            "urgency": urgency,
            "timestamp": f"2025-09-12T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}Z",
            "quality_flags": {
                "high_confidence": confidence > 0.85,
                "high_complexity": complexity_score > 0.7,
                "critical_severity": severity in ["深刻", "極めて深刻"],
                "urgent": urgency in ["高", "最高"]
            }
        }

    def _generate_batch(self, batch_id: int) -> List[Dict[str, Any]]:
        """バッチ文書生成"""
        documents = []
        
        for _ in range(self.config.batch_size):
            doc = self._generate_enhanced_document()
            documents.append(doc)
        
        return documents

    def _save_chunk(self, chunk_data: List[Dict[str, Any]], chunk_id: int) -> str:
        """チャンクファイル保存"""
        timestamp = time.strftime("%Y%m%d%H%M%S")
        filename = f"2m_wildfire_v1-0_chunk_{chunk_id:03d}_{timestamp}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(chunk_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved chunk {chunk_id:03d}: {len(chunk_data):,} docs -> {filename}")
        return str(filepath)

    def generate_2m_dataset(self) -> Dict[str, Any]:
        """200万文書データセット生成メイン処理"""
        logger.info("=== v1-0: 2M Document Collection Started ===")
        logger.info(f"Target: {self.config.target_documents:,} documents")
        logger.info(f"Configuration: {self.config.workers} workers, {self.config.batch_size:,} batch, {self.config.chunk_size:,} chunk")
        logger.info("Quality settings: Enhanced diversity for v1-0 scaling test")
        
        self.stats["start_time"] = time.time()
        
        total_batches = self.config.target_documents // self.config.batch_size
        chunk_batches = self.config.chunk_size // self.config.batch_size
        
        all_documents = []
        chunk_id = 0
        
        with ThreadPoolExecutor(max_workers=self.config.workers) as executor:
            with tqdm(total=total_batches, desc="Generating 2M documents") as pbar:
                
                # バッチ処理のスケジューリング
                futures = []
                for batch_id in range(total_batches):
                    future = executor.submit(self._generate_batch, batch_id)
                    futures.append(future)
                
                # 結果収集とチャンク保存
                for i, future in enumerate(as_completed(futures)):
                    try:
                        batch_documents = future.result()
                        all_documents.extend(batch_documents)
                        
                        # チャンクサイズに達したら保存
                        if len(all_documents) >= self.config.chunk_size:
                            chunk_documents = all_documents[:self.config.chunk_size]
                            remaining_documents = all_documents[self.config.chunk_size:]
                            
                            self._save_chunk(chunk_documents, chunk_id)
                            chunk_id += 1
                            self.stats["chunks_created"] += 1
                            
                            all_documents = remaining_documents
                            
                            # メモリ管理
                            if chunk_id % 5 == 0:
                                self._log_memory_usage(f"chunk_{chunk_id}")
                                gc.collect()
                        
                        # 進捗更新
                        pbar.update(1)
                        self.stats["total_generated"] = (i + 1) * self.config.batch_size
                        
                        # 進捗ログ（50万文書毎）
                        if self.stats["total_generated"] % 500000 == 0:
                            elapsed = time.time() - self.stats["start_time"]
                            speed = self.stats["total_generated"] / elapsed
                            memory_gb = self.process.memory_info().rss / 1024**3
                            logger.info(f"Progress: {self.stats['total_generated']:,}/{self.config.target_documents:,} documents, Memory: {memory_gb:.2f}GB")
                    
                    except Exception as e:
                        logger.error(f"Batch generation error: {e}")
                        continue
        
        # 残りの文書を最終チャンクとして保存
        if all_documents:
            self._save_chunk(all_documents, chunk_id)
            self.stats["chunks_created"] += 1
        
        # 最終統計
        end_time = time.time()
        total_time = end_time - self.stats["start_time"]
        speed = self.config.target_documents / total_time
        
        logger.info("=== v1-0: 2M Document Collection Completed ===")
        logger.info(f"Generated: {self.config.target_documents:,} documents")
        logger.info(f"Time: {total_time:.1f}s ({total_time/60:.1f}min)")
        logger.info(f"Speed: {speed:.1f} docs/sec")
        logger.info(f"Chunks: {self.stats['chunks_created']} files")
        
        memory_gb = self.process.memory_info().rss / 1024**3
        logger.info(f"Memory: {memory_gb:.2f}GB peak")
        logger.info(f"Output: {self.config.output_dir}/")
        
        return {
            "total_documents": self.config.target_documents,
            "total_time": total_time,
            "speed": speed,
            "chunks_created": self.stats["chunks_created"],
            "memory_peak_gb": memory_gb,
            "sentiment_distribution": dict(list(self.stats["sentiment_distribution"].items())[:10]),
            "location_distribution": dict(list(self.stats["location_distribution"].items())[:10]),
            "fire_type_distribution": dict(list(self.stats["fire_type_distribution"].items())[:10])
        }

def main():
    """メイン実行関数"""
    config = Data2MConfig()
    collector = WildfireDataCollector2M(config)
    
    try:
        results = collector.generate_2m_dataset()
        
        print(f"\n🎉 2M Document Collection v1-0 completed!")
        print(f"📊 Generated: {results['total_documents']:,} documents")
        print(f"⏱️  Time: {results['total_time']:.1f}s ({results['total_time']/60:.1f}min)")
        print(f"🚀 Speed: {results['speed']:.1f} docs/sec")
        print(f"📁 Chunks: {results['chunks_created']} files")
        print(f"💾 Memory: {results['memory_peak_gb']:.2f}GB")
        print(f"📂 Output: {config.output_dir}")
        print(f"🎯 Quality: Enhanced diversity for v1-0 scaling test")
        
    except Exception as e:
        logger.error(f"Data collection failed: {e}")
        raise

if __name__ == "__main__":
    main()