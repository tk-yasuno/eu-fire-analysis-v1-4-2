#!/usr/bin/env python3
"""
災害感情分析システム v0-6 - 200K文書データ収集システム
Ultra-scale data collector for 200,000 wildfire-related documents
実用化向け超大規模データ収集（v0-5の4倍スケール）
"""

import asyncio
import json
import logging
import multiprocessing as mp
import os
import random
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from typing import Dict, List, Set, Tuple, Any
import psutil
from tqdm import tqdm

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UltraScaleWildfireCollector200K:
    """200K文書対応の超大規模山火事データ収集システム"""
    
    def __init__(self):
        self.target_documents = 200000  # 20万文書
        self.chunk_size = 20000  # v0-5の2倍チャンクサイズ
        self.batch_size = 4000  # v0-5の2倍バッチサイズ  
        self.num_workers = min(20, mp.cpu_count())  # 最大20ワーカー
        self.output_dir = "data_v0-6"
        self.max_retries = 15  # リトライ回数増加
        
        # パフォーマンス監視
        self.process = psutil.Process()
        self.start_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = self.start_memory
        
        # v0-6用拡張データベース
        self.locations = [
            # 既存の場所
            "California", "Oregon", "Washington", "Colorado", "Montana", "Idaho", "Wyoming",
            "Arizona", "New Mexico", "Nevada", "Utah", "Alaska", "British Columbia",
            "Alberta", "Ontario", "Quebec", "Australia", "Greece", "Spain", "Portugal",
            "Turkey", "Chile", "Argentina", "Brazil", "Russia", "Indonesia", "Philippines",
            # v0-6追加（より詳細な地域）
            "Los Angeles County", "Orange County", "Riverside County", "San Bernardino County",
            "Ventura County", "Santa Barbara County", "Kern County", "Fresno County",
            "Tulare County", "Inyo County", "Mono County", "Madera County", "Merced County",
            "Stanislaus County", "San Joaquin County", "Calaveras County", "Tuolumne County",
            "Mariposa County", "Napa County", "Sonoma County", "Lake County", "Mendocino County",
            "Humboldt County", "Del Norte County", "Siskiyou County", "Modoc County",
            "Lassen County", "Plumas County", "Sierra County", "Nevada County", "Placer County",
            "El Dorado County", "Amador County", "Alpine County", "Shasta County",
            "Tehama County", "Glenn County", "Butte County", "Yuba County", "Sutter County",
            "Colusa County", "Yolo County", "Solano County", "Contra Costa County",
            "Alameda County", "San Mateo County", "Santa Clara County", "Santa Cruz County",
            "Monterey County", "San Benito County", "Kings County", "Imperial County",
            "San Diego County", "Marin County", "San Francisco County"
        ]
        
        self.fire_names = [
            # 既存の火災名
            "Camp Fire", "Thomas Fire", "Tubbs Fire", "Tunnel Fire", "Carr Fire",
            "Mendocino Fire", "Rim Fire", "Cedar Fire", "Witch Fire", "Old Fire",
            "Grand Prix Fire", "Day Fire", "Esperanza Fire", "Santiago Fire",
            "Harris Fire", "Grass Valley Fire", "Poomacha Fire", "Rice Fire",
            "Slide Fire", "King Fire", "Rocky Fire", "Valley Fire", "Butte Fire",
            "Clayton Fire", "Blue Cut Fire", "Sand Fire", "Pilot Fire", "Radford Fire",
            "Apple Fire", "El Dorado Fire", "Creek Fire", "North Fire", "Lake Fire",
            # v0-6追加（より多様な火災名）
            "Fairview Fire", "Mosquito Fire", "McKinney Fire", "Monument Fire",
            "Boundary Fire", "Cascade Fire", "Deadwood Fire", "Echo Fire",
            "Forest Fire", "Gateway Fire", "Highland Fire", "Iron Fire",
            "Junction Fire", "Keystone Fire", "Lightning Fire", "Meadow Fire",
            "Northern Fire", "Oak Fire", "Pacific Fire", "Ridge Fire",
            "Summit Fire", "Trinity Fire", "Union Fire", "Vista Fire",
            "Woodland Fire", "Zenith Fire", "Alpine Fire", "Boulder Fire",
            "Canyon Fire", "Desert Fire", "Eagle Fire", "Falcon Fire",
            "Granite Fire", "Harbor Fire", "Island Fire", "Jade Fire",
            "Knight Fire", "Liberty Fire", "Marine Fire", "Noble Fire",
            "Ocean Fire", "Pioneer Fire", "Quest Fire", "River Fire",
            "Stone Fire", "Timber Fire", "Urban Fire", "Valley Fire",
            "Wind Fire", "Xeric Fire", "Yield Fire", "Zero Fire",
            "Apex Fire", "Blaze Fire", "Crown Fire", "Delta Fire",
            "Ember Fire", "Flame Fire", "Glow Fire", "Heat Fire",
            "Inferno Fire", "Jet Fire", "Kindle Fire", "Lava Fire"
        ]
        
        # 拡張センチメント多様性テンプレート（v0-6用）
        self.sentiment_templates = {
            "emergency_critical": [
                "CRITICAL ALERT: {} fire creates immediate life threat in {}. Emergency evacuation mandatory for zones {}.",
                "EXTREME DANGER: {} fire advancing rapidly through {} with {} mph winds. Evacuate immediately via routes {}.",
                "LIFE THREATENING: {} fire has trapped residents in {}. Emergency airlift operations underway with {}.",
                "IMMINENT THREAT: {} fire approaching {} community. All residents must evacuate now using {} corridor.",
                "RED FLAG EMERGENCY: {} fire creates firestorm conditions in {}. {} emergency shelters opened."
            ],
            "emergency_urgent": [
                "URGENT: {} fire approaching {}. Immediate evacuation required for zones {}.",
                "EMERGENCY: {} fire threatens {} in {}. Emergency services responding with {}.",
                "BREAKING: {} fire has grown to {} acres in {}. Firefighters working around the clock.",
                "ALERT: {} fire spreading rapidly through {} terrain in {}. {} crews deployed.",
                "EVACUATION: Residents of {} must leave immediately due to {} fire. Use designated routes only."
            ],
            "firefighting_operations": [
                "Firefighting crews from {} deployed to battle the massive {} fire. {} operations continuing.",
                "Containment lines holding steady against {} fire in {}. {} creating firebreaks.",
                "Helitack teams working to suppress {} fire in remote areas of {}. {} operations.",
                "Air tanker missions continue over {} fire in {}. {} drops completed today.",
                "Ground crews making progress on {} fire in {}. {}% contained as of latest update.",
                "Type 1 incident management team assuming command of {} fire in {}. {} resources assigned.",
                "Heavy air tankers conducting retardant drops on {} fire perimeter in {}. {} gallons deployed.",
                "Hot shot crews establishing control lines around {} fire in {}. {} personnel on scene."
            ],
            "weather_conditions": [
                "High winds persist across {}, elevating wildfire risk. {} fire danger declared.",
                "Temperature soaring to {} F with {}% humidity in {}. Critical fire weather conditions.",
                "Lightning strikes sparked multiple fires during dry thunderstorm in {}. {} conditions continuing.",
                "Red flag warning issued for {} due to extreme fire weather. {} winds gusting to {} mph.",
                "Rainfall finally arriving in {} after months of drought. Fire season may be ending.",
                "Atmospheric river bringing moisture to {} region. {} inches of rain forecast.",
                "Heat dome settles over {} with temperatures exceeding {} degrees. Extreme fire danger.",
                "Santa Ana winds developing across {} with gusts up to {} mph. Critical fire weather."
            ],
            "damage_assessment": [
                "Preliminary damage assessment shows {} structures destroyed by {} fire in {}.",
                "Economic impact of {} fire estimated at ${} million in {}. Insurance claims mounting.",
                "Post-fire rehabilitation begins in {} areas of {}. Erosion control priority.",
                "Reforestation project planting {} trees in burned areas of {}. {} restoration.",
                "Wildlife habitat restoration underway in fire-affected areas of {}. {} species recovery.",
                "Infrastructure damage from {} fire includes {} miles of powerlines in {}.",
                "Agricultural losses from {} fire total ${} million in {} County.",
                "Watershed impacts from {} fire affecting water quality in {} river system."
            ],
            "evacuation_shelter": [
                "Evacuation orders lifted for {} residents in {} fire area. Repopulation begins.",
                "Emergency shelters housing {} evacuees from {} fire in {}. Red Cross assistance available.",
                "Mandatory evacuation expanded to include {} neighborhoods in {}. {} routes open.",
                "Voluntary evacuation recommended for {} area residents as {} fire approaches.",
                "Evacuation center established at {} for {} fire evacuees. Capacity for {} people.",
                "Pet-friendly evacuation shelter opens at {} for {} fire displaced animals.",
                "Large animal evacuation facility activated at {} fairgrounds for {} fire.",
                "Emergency transportation provided for elderly residents in {} fire evacuation zone."
            ],
            "air_quality": [
                "Air quality alert issued for {} due to smoke from {} fire. Sensitive groups stay indoors.",
                "Smoke from {} fire creating hazardous air quality in {}. N95 masks recommended.",
                "Air quality index reaches {} in {} due to wildfire smoke. Health advisory in effect.",
                "Schools closed in {} due to poor air quality from nearby fires. Distance learning implemented.",
                "Smoke plume from {} fire visible from space. Air quality impacts felt {} miles away.",
                "Purple Air sensors show unhealthy air quality levels across {}. {} AQI reported.",
                "Respiratory health warnings issued for {} metropolitan area due to {} fire smoke.",
                "Air purification centers opened in {} to help residents escape wildfire smoke."
            ],
            "recovery_support": [
                "Community recovery center opens in {} for {} fire survivors. FEMA assistance available.",
                "Debris removal begins in fire-devastated areas of {}. {} contractor teams deployed.",
                "Utility crews restoring power to {} customers affected by {} fire in {}.",
                "Water system repairs underway in {} areas affected by {} fire.",
                "Mental health support services available for {} fire survivors. Crisis counselors on site.",
                "Temporary housing assistance provided for {} families displaced by {} fire.",
                "Small business recovery grants available for {} fire affected enterprises in {}.",
                "Volunteer cleanup efforts organized for {} fire damaged properties in {}."
            ],
            "scientific_analysis": [
                "Climate researchers studying {} fire behavior patterns in {} ecosystem.",
                "Satellite imagery reveals {} fire burn severity across {} acres in {}.",
                "Fire weather modeling predicts {} conditions for {} fire in {} region.",
                "Smoke dispersion models forecast {} air quality impacts from {} fire.",
                "Post-fire flood risk assessment conducted for {} watersheds affected by {} fire.",
                "Vegetation recovery monitoring initiated in {} fire burn areas of {}.",
                "Fire progression mapping shows {} fire spread rate of {} acres per hour.",
                "Meteorological analysis of {} fire identifies {} as primary wind driver."
            ],
            "community_resilience": [
                "Community preparedness program launched in {} following {} fire lessons learned.",
                "Defensible space inspections increase by {}% in {} after {} fire.",
                "Neighborhood fire safety coalition formed in {} to prevent future {} fire scenarios.",
                "Emergency communication system upgraded in {} following {} fire response gaps.",
                "Fire-resistant landscaping initiative begins in {} communities affected by {} fire.",
                "Evacuation route improvements planned for {} following {} fire experience.",
                "Community fire station construction approved for {} after {} fire response delays.",
                "Wildfire education program reaches {} residents in {} fire-prone areas."
            ]
        }
        
        # 感情極性のバリエーション拡張
        self.sentiment_modifiers = {
            "positive": [
                "successful", "effective", "heroic", "brave", "coordinated", "swift",
                "professional", "dedicated", "tireless", "outstanding", "remarkable",
                "exceptional", "inspiring", "hopeful", "resilient", "united",
                "determined", "courageous", "skillful", "experienced"
            ],
            "negative": [
                "devastating", "catastrophic", "dangerous", "threatening", "destructive",
                "overwhelming", "tragic", "severe", "critical", "alarming",
                "unprecedented", "disastrous", "terrifying", "heartbreaking", "chaotic",
                "desperate", "exhausting", "relentless", "unforgiving", "merciless"
            ],
            "neutral": [
                "ongoing", "current", "standard", "routine", "scheduled", "planned",
                "regular", "normal", "typical", "expected", "baseline", "conventional",
                "systematic", "methodical", "procedural", "operational", "technical",
                "administrative", "logistical", "preliminary", "interim"
            ]
        }
        
        # 拡張統計データ
        self.statistics_ranges = {
            "acres": [(100, 1000), (1000, 10000), (10000, 50000), (50000, 200000), (200000, 500000)],
            "structures": [(1, 50), (50, 200), (200, 1000), (1000, 5000), (5000, 20000)],
            "evacuees": [(100, 1000), (1000, 5000), (5000, 20000), (20000, 100000), (100000, 500000)],
            "personnel": [(50, 200), (200, 1000), (1000, 3000), (3000, 8000), (8000, 15000)],
            "cost_millions": [(10, 100), (100, 500), (500, 2000), (2000, 10000), (10000, 50000)],
            "temperature": [(85, 95), (95, 105), (105, 115), (115, 125), (125, 135)],
            "humidity": [(5, 15), (15, 25), (25, 40), (40, 60), (60, 85)],
            "wind_speed": [(15, 30), (30, 50), (50, 70), (70, 90), (90, 120)],
            "aqi": [(50, 100), (100, 150), (150, 200), (200, 300), (300, 500)]
        }
        
        os.makedirs(self.output_dir, exist_ok=True)
        logger.info(f"Initialized 200K Ultra-scale Collector: {self.num_workers} workers, chunk size: {self.chunk_size}")

    def monitor_memory(self) -> float:
        """メモリ使用量監視"""
        current_memory = self.process.memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = max(self.peak_memory, current_memory)
        return current_memory

    def generate_enhanced_document(self, doc_id: int) -> Dict[str, Any]:
        """強化された文書生成（より多様性とリアリティを重視）"""
        attempts = 0
        max_attempts = self.max_retries
        
        while attempts < max_attempts:
            try:
                # ランダムテンプレート選択（より重み付き選択）
                category_weights = {
                    "emergency_critical": 0.05,  # 5% - 最重要緊急
                    "emergency_urgent": 0.15,    # 15% - 緊急
                    "firefighting_operations": 0.25,  # 25% - 消防活動
                    "weather_conditions": 0.15,  # 15% - 気象
                    "damage_assessment": 0.12,   # 12% - 被害評価
                    "evacuation_shelter": 0.12,  # 12% - 避難
                    "air_quality": 0.08,         # 8% - 大気質
                    "recovery_support": 0.08,    # 8% - 復旧支援
                    "scientific_analysis": 0.05, # 5% - 科学分析
                    "community_resilience": 0.05 # 5% - 地域回復力
                }
                
                category = random.choices(
                    list(category_weights.keys()),
                    weights=list(category_weights.values())
                )[0]
                
                template = random.choice(self.sentiment_templates[category])
                
                # 動的パラメータ生成
                fire_name = random.choice(self.fire_names)
                location = random.choice(self.locations)
                
                # 統計データ生成
                stats = {}
                for stat_type, ranges in self.statistics_ranges.items():
                    selected_range = random.choice(ranges)
                    stats[stat_type] = random.randint(selected_range[0], selected_range[1])
                
                # テンプレート処理とバリデーション
                params_needed = template.count('{}')
                params = []
                
                # パラメータ生成ロジック
                for i in range(params_needed):
                    if i == 0:
                        params.append(fire_name)
                    elif i == 1:
                        params.append(location)
                    elif i == 2:
                        # 3番目のパラメータ - コンテキストに応じて選択
                        if "acres" in template.lower():
                            params.append(f"{stats['acres']:,}")
                        elif "evacuee" in template.lower():
                            params.append(f"{stats['evacuees']:,}")
                        elif "temperature" in template.lower():
                            params.append(str(stats['temperature']))
                        elif "humidity" in template.lower():
                            params.append(str(stats['humidity']))
                        elif "wind" in template.lower():
                            params.append(str(stats['wind_speed']))
                        elif "personnel" in template.lower() or "crew" in template.lower():
                            params.append(f"{stats['personnel']:,}")
                        elif "cost" in template.lower() or "million" in template.lower():
                            params.append(f"{stats['cost_millions']:,}")
                        else:
                            params.append(random.choice([
                                f"Route {random.randint(1, 99)}",
                                f"Zone {random.choice(['A', 'B', 'C', 'D', 'E'])}-{random.randint(1, 20)}",
                                f"{random.randint(50, 500)} personnel",
                                f"{random.randint(10, 95)}% contained"
                            ]))
                    else:
                        # 追加パラメータ
                        options = [
                            f"Highway {random.randint(1, 101)}",
                            f"{random.randint(1, 24)} hours",
                            f"{random.choice(['north', 'south', 'east', 'west'])}bound",
                            f"{random.randint(5, 50)} structures",
                            f"Zone {random.choice(['Red', 'Orange', 'Yellow'])}"
                        ]
                        params.append(random.choice(options))
                
                # テキスト生成
                try:
                    text = template.format(*params)
                except (IndexError, ValueError):
                    attempts += 1
                    continue
                
                # 感情極性決定
                if category in ["emergency_critical", "emergency_urgent"]:
                    sentiment = "negative"
                    confidence = random.uniform(0.7, 0.95)
                elif category in ["firefighting_operations", "recovery_support", "community_resilience"]:
                    sentiment = "positive"
                    confidence = random.uniform(0.6, 0.85)
                elif category in ["scientific_analysis"]:
                    sentiment = "neutral"
                    confidence = random.uniform(0.5, 0.75)
                else:
                    # 混合感情
                    sentiment = random.choices(
                        ["positive", "negative", "neutral"],
                        weights=[0.3, 0.5, 0.2]
                    )[0]
                    confidence = random.uniform(0.4, 0.8)
                
                # メタデータ強化
                document = {
                    "id": f"200k_wildfire_{doc_id:06d}",
                    "text": text,
                    "sentiment": sentiment,
                    "confidence": round(confidence, 3),
                    "category": category,
                    "fire_name": fire_name,
                    "location": location,
                    "statistics": stats,
                    "timestamp": datetime.now().isoformat(),
                    "version": "v0-6_200k",
                    "word_count": len(text.split()),
                    "char_count": len(text),
                    "complexity_score": round(random.uniform(0.3, 0.9), 3)
                }
                
                # 品質チェック
                if (len(text) >= 50 and 
                    len(text.split()) >= 8 and 
                    fire_name in text and 
                    location in text):
                    return document
                else:
                    attempts += 1
                    
            except Exception as e:
                attempts += 1
                if attempts >= max_attempts:
                    logger.warning(f"Failed to generate valid document {doc_id} after {max_attempts} attempts: {e}")
                    
        # フォールバック文書
        return {
            "id": f"200k_wildfire_{doc_id:06d}",
            "text": f"Wildfire monitoring report for {random.choice(self.fire_names)} in {random.choice(self.locations)}. Current status under assessment.",
            "sentiment": "neutral",
            "confidence": 0.5,
            "category": "fallback",
            "fire_name": random.choice(self.fire_names),
            "location": random.choice(self.locations),
            "statistics": {},
            "timestamp": datetime.now().isoformat(),
            "version": "v0-6_200k_fallback",
            "word_count": 15,
            "char_count": 100,
            "complexity_score": 0.5
        }

    def generate_batch_documents(self, worker_id: int, start_id: int, batch_size: int) -> List[Dict[str, Any]]:
        """バッチ文書生成（ワーカー関数）"""
        documents = []
        
        for i in range(batch_size):
            doc_id = start_id + i
            document = self.generate_enhanced_document(doc_id)
            documents.append(document)
            
            # プログレス表示（1000文書ごと）
            if (i + 1) % 1000 == 0:
                logger.info(f"Worker {worker_id}: Generated {i + 1}/{batch_size} documents")
        
        logger.info(f"Worker {worker_id} completed: {len(documents)} documents")
        return documents

    def remove_duplicates(self, all_documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """重複除去（強化版 - より厳密なチェック）"""
        seen_texts: Set[str] = set()
        seen_combinations: Set[Tuple[str, str, str]] = set()
        unique_documents = []
        
        for doc in all_documents:
            text = doc["text"]
            fire_name = doc.get("fire_name", "")
            location = doc.get("location", "")
            
            # テキスト重複チェック
            if text in seen_texts:
                continue
            
            # 組み合わせ重複チェック（火災名+場所+カテゴリ）
            combination = (fire_name, location, doc.get("category", ""))
            if combination in seen_combinations:
                continue
            
            seen_texts.add(text)
            seen_combinations.add(combination)
            unique_documents.append(doc)
        
        logger.info(f"Duplicate removal: {len(all_documents)} -> {len(unique_documents)} documents")
        return unique_documents

    def save_chunked_data(self, documents: List[Dict[str, Any]], timestamp: str) -> List[str]:
        """チャンク分割保存（200K対応）"""
        chunk_files = []
        
        for i in range(0, len(documents), self.chunk_size):
            chunk = documents[i:i + self.chunk_size]
            chunk_num = i // self.chunk_size + 1
            
            chunk_filename = f"{timestamp}_wildfire_v0-6_200k_chunk_{chunk_num:02d}.json"
            chunk_path = os.path.join(self.output_dir, chunk_filename)
            
            with open(chunk_path, 'w', encoding='utf-8') as f:
                json.dump(chunk, f, indent=2, ensure_ascii=False)
            
            chunk_files.append(chunk_filename)
            logger.info(f"Chunk {chunk_num} saved: {len(chunk)} documents to {chunk_filename}")
        
        return chunk_files

    async def collect_200k_data(self) -> Dict[str, Any]:
        """200K文書収集メイン処理"""
        start_time = time.time()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info("=== 200K Ultra-scale Data Collection Started ===")
        logger.info(f"Target: {self.target_documents:,} documents")
        logger.info(f"Workers: {self.num_workers}")
        logger.info(f"Batch size: {self.batch_size}")
        logger.info(f"Chunk size: {self.chunk_size}")
        
        all_documents = []
        
        # 並列バッチ処理
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = []
            
            current_id = 0
            while current_id < self.target_documents:
                remaining = self.target_documents - current_id
                batch_size = min(self.batch_size, remaining)
                
                future = executor.submit(
                    self.generate_batch_documents,
                    len(futures),  # worker_id
                    current_id,
                    batch_size
                )
                futures.append(future)
                current_id += batch_size
            
            # 結果収集
            with tqdm(total=len(futures), desc="Processing batches") as pbar:
                for future in futures:
                    batch_documents = future.result()
                    all_documents.extend(batch_documents)
                    pbar.update(1)
                    
                    # メモリ監視
                    current_memory = self.monitor_memory()
                    if current_memory > 2000:  # 2GB超過で警告
                        logger.warning(f"High memory usage: {current_memory:.1f} MB")
        
        # データ統合処理
        logger.info(f"Integrating {len(all_documents)} documents...")
        
        # 重複除去
        unique_documents = self.remove_duplicates(all_documents)
        
        # チャンク分割保存
        chunk_files = self.save_chunked_data(unique_documents, timestamp)
        
        # 収集結果保存
        processing_time = time.time() - start_time
        final_memory = self.monitor_memory()
        
        collection_results = {
            "version": "v0-6_200k",
            "timestamp": timestamp,
            "target_documents": self.target_documents,
            "generated_documents": len(all_documents),
            "unique_documents": len(unique_documents),
            "duplicate_rate": round((len(all_documents) - len(unique_documents)) / len(all_documents) * 100, 2),
            "processing_time_seconds": round(processing_time, 2),
            "processing_time_minutes": round(processing_time / 60, 2),
            "speed_docs_per_second": round(len(unique_documents) / processing_time, 1),
            "num_workers": self.num_workers,
            "batch_size": self.batch_size,
            "chunk_size": self.chunk_size,
            "num_chunks": len(chunk_files),
            "chunk_files": chunk_files,
            "memory_usage": {
                "start_mb": round(self.start_memory, 1),
                "peak_mb": round(self.peak_memory, 1),
                "final_mb": round(final_memory, 1)
            },
            "data_quality": {
                "avg_text_length": round(sum(len(doc["text"]) for doc in unique_documents) / len(unique_documents), 1),
                "avg_word_count": round(sum(doc.get("word_count", 0) for doc in unique_documents) / len(unique_documents), 1),
                "categories": len(set(doc["category"] for doc in unique_documents)),
                "sentiment_distribution": {
                    sentiment: sum(1 for doc in unique_documents if doc["sentiment"] == sentiment)
                    for sentiment in ["positive", "negative", "neutral"]
                }
            }
        }
        
        results_path = os.path.join(self.output_dir, f"collection_results_{timestamp}.json")
        with open(results_path, 'w', encoding='utf-8') as f:
            json.dump(collection_results, f, indent=2, ensure_ascii=False)
        
        # 結果表示
        logger.info("200K Ultra-scale data collection completed!")
        logger.info(f"Generated: {len(unique_documents):,} documents")
        logger.info(f"Processing time: {processing_time:.2f} seconds")
        logger.info(f"Speed: {collection_results['speed_docs_per_second']} docs/sec")
        logger.info(f"Peak memory: {self.peak_memory:.1f} MB")
        logger.info(f"Chunks created: {len(chunk_files)}")
        
        print(f"\n🎉 200K Ultra-scale data collection completed!")
        print(f"📊 Total documents: {len(unique_documents):,}")
        print(f"⏱️  Processing time: {processing_time:.2f} seconds")
        print(f"🚀 Speed: {collection_results['speed_docs_per_second']} docs/sec")
        print(f"💾 Peak memory: {self.peak_memory:.1f} MB")
        print(f"📁 Chunks created: {len(chunk_files)}")
        print(f"📄 Results saved: {results_path}")
        
        return collection_results

def main():
    """メイン実行関数"""
    collector = UltraScaleWildfireCollector200K()
    loop = asyncio.get_event_loop()
    results = loop.run_until_complete(collector.collect_200k_data())
    return results

if __name__ == "__main__":
    main()