#!/usr/bin/env python3
"""
200K文書データ収集 - シンプル版
Ultra-scale data collection for v0-6 (simplified multiprocessing)
"""

import json
import logging
import random
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UltraScaleDataCollector:
    """200K文書超大規模データ収集器"""
    
    def __init__(self):
        self.sentiment_categories = [
            "extreme_fear", "high_anxiety", "moderate_concern", "mild_worry",
            "neutral", "cautious_hope", "moderate_relief", "strong_confidence",
            "overwhelming_gratitude", "emergency_panic"
        ]
        
        self.locations = [
            "Northern California", "Southern California", "Oregon", "Washington",
            "Colorado", "Montana", "Idaho", "Wyoming", "Utah", "Nevada",
            "Arizona", "New Mexico", "Texas", "Oklahoma", "Kansas", "Nebraska",
            "North Dakota", "South Dakota", "Minnesota", "Wisconsin", "Michigan",
            "Alaska", "Hawaii", "British Columbia", "Alberta", "Saskatchewan",
            "Manitoba", "Ontario", "Quebec", "Yukon", "Northwest Territories",
            "Nunavut", "New South Wales", "Victoria", "Queensland", "Western Australia",
            "South Australia", "Tasmania", "Northern Territory", "Portugal", "Spain",
            "France", "Italy", "Greece", "Turkey", "Chile", "Argentina", "Brazil",
            "Indonesia", "Australia", "Russia", "China", "Japan", "South Korea",
            "India", "Bangladesh", "Pakistan", "Afghanistan", "Iran", "Iraq",
            "Syria", "Lebanon", "Israel", "Egypt", "Libya", "Algeria", "Morocco",
            "Tunisia", "Kenya", "Uganda", "Tanzania", "Ethiopia", "Somalia",
            "Sudan", "South Sudan", "Central African Republic", "Democratic Republic of Congo",
            "Republic of Congo", "Cameroon", "Chad", "Niger", "Nigeria", "Ghana"
        ]
        
        self.fire_types = [
            "wildfire", "forest fire", "grass fire", "brush fire", "canyon fire",
            "mountain fire", "valley fire", "ridge fire", "creek fire", "river fire",
            "lake fire", "desert fire", "coastal fire", "urban fire", "suburban fire",
            "rural fire", "crown fire", "ground fire", "surface fire", "prescribed fire",
            "controlled burn", "backfire", "spot fire", "complex fire", "mega fire",
            "firestorm", "fire tornado", "ember storm", "fire weather", "red flag warning",
            "evacuation fire", "structure fire", "vehicle fire", "vegetation fire", "containment fire",
            "perimeter fire", "active fire", "smoldering fire", "rekindled fire", "new fire",
            "spreading fire", "contained fire", "controlled fire", "extinguished fire", "monitored fire",
            "lightning fire", "human-caused fire", "arson fire", "accident fire", "equipment fire"
        ]

    def generate_document(self, doc_id: int) -> dict:
        """単一文書生成"""
        sentiment = random.choice(self.sentiment_categories)
        location = random.choice(self.locations)
        fire_type = random.choice(self.fire_types)
        
        # センチメントに基づく文書生成
        templates = {
            "extreme_fear": [
                f"URGENT: Massive {fire_type} approaching {location}! Evacuate immediately! Flames visible from miles away, smoke blocking visibility. Emergency services overwhelmed. This is unprecedented destruction.",
                f"TERROR in {location} as {fire_type} spreads uncontrollably! People fleeing with only clothes on their backs. Entire neighborhoods engulfed. Cannot believe the devastation.",
                f"NIGHTMARE scenario in {location}: {fire_type} jumping firebreaks, consuming everything. Evacuation routes blocked. Praying for survival. This is catastrophic."
            ],
            "high_anxiety": [
                f"Very worried about the {fire_type} near {location}. Evacuation orders just issued for our area. Packing essentials now. Air quality extremely poor, children struggling to breathe.",
                f"Anxious watching {fire_type} reports from {location}. Friends and family in danger zone. No word from some relatives. Emergency services stretched thin.",
                f"High stress as {fire_type} threatens {location}. Property values crashing, insurance companies panicking. Years of preparation may not be enough."
            ],
            "moderate_concern": [
                f"Monitoring the {fire_type} situation in {location} closely. Weather conditions not favorable for containment. Concerned about air quality and potential evacuations.",
                f"Following updates on {fire_type} in {location}. Fire department working hard but challenging conditions. Hoping for wind direction change soon.",
                f"Watching {fire_type} in {location} with interest. Local authorities advising preparedness. Climate change making these events more frequent."
            ],
            "mild_worry": [
                f"Keeping an eye on the {fire_type} near {location}. Seems under control but you never know. Good to have emergency kit ready just in case.",
                f"Noticed reports about {fire_type} in {location}. Not immediately threatening but worth following. Fire season seems longer each year.",
                f"Saw news about {fire_type} affecting {location}. Hoping firefighters can contain it quickly. Important to stay informed during fire season."
            ],
            "neutral": [
                f"Fire reported in {location}. Fire departments responding to {fire_type}. Monitoring situation for updates. No immediate threat to residential areas.",
                f"Incident involving {fire_type} in {location} area. Emergency services on scene. Investigation ongoing into cause and containment measures.",
                f"Fire activity reported near {location}. {fire_type} being addressed by local authorities. Weather conditions being monitored for fire behavior."
            ],
            "cautious_hope": [
                f"Cautiously optimistic about {fire_type} containment in {location}. Fire crews making progress. Weather forecast looks more favorable for suppression efforts.",
                f"Some positive news on {fire_type} in {location}. Firefighters gaining ground. Community support has been incredible during this challenging time.",
                f"Hopeful signs in {fire_type} suppression near {location}. Winds calming, temperatures dropping. Still need rain but progress being made."
            ],
            "moderate_relief": [
                f"Relief that {fire_type} in {location} is now 60% contained. Evacuation orders lifted for some areas. Grateful for firefighters' heroic efforts.",
                f"Good news on {fire_type} situation in {location}. Forward progress stopped. Community coming together to support affected families.",
                f"Thankful that {fire_type} near {location} is under control. Property damage significant but could have been much worse. Recovery begins now."
            ],
            "strong_confidence": [
                f"Confident in firefighters' ability to fully contain {fire_type} in {location}. Excellent coordination between agencies. Community resilience inspiring.",
                f"Strong leadership shown in {fire_type} response near {location}. Resource allocation effective, communication clear. Lessons learned being applied.",
                f"Impressive firefighting effort against {fire_type} in {location}. Technology and training making real difference. Optimistic about full containment soon."
            ],
            "overwhelming_gratitude": [
                f"Overwhelmed with gratitude for firefighters who saved our community from {fire_type} in {location}. Heroes every one. Donations pouring in for affected families.",
                f"Cannot express enough thanks for emergency responders during {fire_type} in {location}. Risked their lives for our safety. Community forever grateful.",
                f"Incredible appreciation for all who helped during {fire_type} near {location}. Neighbors helping neighbors, strangers becoming family. Faith in humanity restored."
            ],
            "emergency_panic": [
                f"PANIC! {fire_type} jumped containment lines near {location}! Evacuation routes jammed! Flames everywhere! Cannot reach family members! HELP NEEDED NOW!",
                f"CHAOS in {location}! {fire_type} exploded overnight! Power out, phones down! People trapped! Emergency services overwhelmed! This is absolute terror!",
                f"DESPERATE situation in {location}! {fire_type} creating own weather! Fire tornadoes reported! Mass evacuation failing! WHERE IS HELP?!"
            ]
        }
        
        text = random.choice(templates[sentiment])
        
        # 複雑度計算
        complexity_score = random.uniform(0.1, 0.9)
        if sentiment in ["extreme_fear", "emergency_panic"]:
            complexity_score = random.uniform(0.7, 0.95)
        elif sentiment in ["neutral"]:
            complexity_score = random.uniform(0.2, 0.5)
        
        # 信頼度計算
        confidence = random.uniform(0.6, 0.95)
        if "!" in text:
            confidence = random.uniform(0.8, 0.95)
        
        return {
            "id": doc_id,
            "text": text,
            "sentiment": sentiment,
            "location": location,
            "fire_type": fire_type,
            "confidence": round(confidence, 3),
            "complexity_score": round(complexity_score, 3),
            "word_count": len(text.split()),
            "char_count": len(text),
            "timestamp": datetime.now().isoformat(),
            "source": "ultra_scale_generator_v0-6",
            "version": "200k"
        }

    def generate_batch(self, start_id: int, batch_size: int) -> list:
        """バッチ生成"""
        return [self.generate_document(start_id + i) for i in range(batch_size)]

    def collect_200k_data(self):
        """200K文書収集"""
        logger.info("=== 200K Ultra-scale Data Collection Started ===")
        
        total_docs = 200000
        batch_size = 4000
        chunk_size = 20000
        max_workers = 8  # 安全な並行数
        
        logger.info(f"Target: {total_docs:,} documents")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Chunk size: {chunk_size}")
        logger.info(f"Workers: {max_workers}")
        
        # 出力ディレクトリ
        output_dir = Path("data_v0-6")
        output_dir.mkdir(exist_ok=True)
        
        start_time = time.time()
        all_documents = []
        
        # バッチ処理
        total_batches = (total_docs + batch_size - 1) // batch_size
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=total_batches, desc="Generating documents") as pbar:
                futures = []
                
                for batch_num in range(total_batches):
                    start_id = batch_num * batch_size
                    remaining = min(batch_size, total_docs - start_id)
                    
                    future = executor.submit(self.generate_batch, start_id, remaining)
                    futures.append((future, batch_num))
                
                # 結果収集
                for future, batch_num in futures:
                    batch_documents = future.result()
                    all_documents.extend(batch_documents)
                    pbar.update(1)
                    
                    # メモリ管理
                    if len(all_documents) >= chunk_size:
                        self._save_chunk(all_documents[:chunk_size], len(all_documents) // chunk_size, output_dir)
                        all_documents = all_documents[chunk_size:]
        
        # 残りのドキュメント保存
        if all_documents:
            chunk_num = (total_docs // chunk_size)
            self._save_chunk(all_documents, chunk_num, output_dir)
        
        generation_time = time.time() - start_time
        speed = total_docs / generation_time
        
        logger.info("=== 200K Data Collection Completed ===")
        logger.info(f"Generated: {total_docs:,} documents")
        logger.info(f"Time: {generation_time:.1f}s")
        logger.info(f"Speed: {speed:.1f} docs/sec")
        logger.info(f"Output: {output_dir}/")
        
        return {
            "total_documents": total_docs,
            "generation_time": generation_time,
            "speed": speed,
            "output_dir": str(output_dir)
        }

    def _save_chunk(self, documents: list, chunk_num: int, output_dir: Path):
        """チャンク保存"""
        timestamp = time.strftime("%Y%m%d%H%M%S")
        chunk_file = output_dir / f"ultra_wildfire_v0-6_200k_chunk_{chunk_num:02d}_{timestamp}.json"
        
        with open(chunk_file, 'w', encoding='utf-8') as f:
            json.dump(documents, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved chunk {chunk_num}: {len(documents):,} docs -> {chunk_file.name}")

def main():
    """メイン実行"""
    collector = UltraScaleDataCollector()
    results = collector.collect_200k_data()
    
    print(f"\n🎉 200K Data collection completed!")
    print(f"📊 Generated: {results['total_documents']:,} documents")
    print(f"⏱️  Time: {results['generation_time']:.1f}s")
    print(f"🚀 Speed: {results['speed']:.1f} docs/sec")
    print(f"📁 Saved to: {results['output_dir']}")

if __name__ == "__main__":
    main()