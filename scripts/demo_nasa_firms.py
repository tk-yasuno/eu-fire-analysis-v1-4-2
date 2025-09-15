#!/usr/bin/env python3
"""
NASA FIRMS Real Data Collection Demo
MAP_KEYを設定した場合の実データ収集デモンストレーション
"""

import json
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# .envファイルから環境変数を読み込み
load_dotenv(Path(__file__).parent.parent / '.env')

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent))

from public_wildfire_collector import PublicWildfireDataCollector, PublicDataConfig

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def demo_template_based_collection():
    """テンプレートベースデータ収集デモ"""
    print("🌲 Template-Based Data Collection Demo")
    print("=" * 50)
    
    # 小規模テスト設定
    config = PublicDataConfig()
    config.target_documents = 300  # 3ソース × 100文書
    config.output_dir = "demo_output_template"
    
    collector = PublicWildfireDataCollector(config)
    
    try:
        # テンプレートデータ収集
        collected_data = collector.collect_all_public_data()
        
        # 結果サマリー
        source_counts = {}
        for item in collected_data:
            source = item.get('source', 'Unknown')
            source_counts[source] = source_counts.get(source, 0) + 1
        
        print(f"\n✅ Collection Results:")
        print(f"   Total: {len(collected_data)} documents")
        print(f"   Sources:")
        for source, count in source_counts.items():
            print(f"     - {source}: {count} documents")
        
        # サンプルデータ表示
        print(f"\n📝 Sample Data:")
        for i, item in enumerate(collected_data[:3]):
            print(f"   {i+1}. Source: {item.get('source', 'N/A')}")
            print(f"      Text: {item.get('text', 'N/A')[:100]}...")
            print(f"      Location: {item.get('location', 'N/A')}")
            print()
        
        return collected_data
        
    except Exception as e:
        logger.error(f"Template collection failed: {e}")
        return []

def demo_real_data_collection_simulation():
    """実データ収集シミュレーション（MAP_KEY未設定の場合）"""
    print("🛰️  Real Data Collection Simulation")
    print("=" * 50)
    
    # MAP_KEY設定チェック
    map_key = os.getenv('NASA_FIRMS_MAP_KEY')
    
    if not map_key:
        print("⚠️  NASA_FIRMS_MAP_KEY not configured")
        print("   This would normally collect real VIIRS satellite fire data")
        print("   Steps to enable real data collection:")
        print("   1. Visit: https://firms.modaps.eosdis.nasa.gov/api/map_key/")
        print("   2. Enter your email to receive free MAP_KEY")
        print("   3. Set environment variable: NASA_FIRMS_MAP_KEY=your_key")
        print("   4. Re-run this demo")
        print()
        
        # シミュレーション例
        print("📊 Simulated Real Data Collection:")
        print("   - Query: North America (7 days)")
        print("   - Sources: VIIRS_SNPP_NRT, VIIRS_NOAA20_NRT, VIIRS_NOAA21_NRT")
        print("   - Expected: 50-500 real fire detections")
        print("   - Data format: CSV with lat/lon, brightness, confidence, FRP")
        print("   - Text generation: Japanese fire reports from satellite data")
        
        return []
    else:
        print(f"✅ MAP_KEY configured: {map_key[:8]}...")
        
        # 実データ収集テスト
        config = PublicDataConfig()
        config.nasa_firms_map_key = map_key
        config.firms_default_days = 3  # 短期間でテスト
        
        collector = PublicWildfireDataCollector(config)
        
        try:
            # NASA FIRMSデータのみ収集
            real_data = collector.collect_nasa_firms_real_data()
            
            print(f"✅ Real data collection results:")
            print(f"   Total fire detections: {len(real_data)}")
            
            if real_data:
                # サンプル表示
                print(f"\n📝 Real Fire Detection Sample:")
                for i, fire in enumerate(real_data[:2]):
                    print(f"   {i+1}. {fire.get('text', 'N/A')[:150]}...")
                    coords = fire.get('coordinates', {})
                    print(f"      Coordinates: {coords.get('latitude', 'N/A')}, {coords.get('longitude', 'N/A')}")
                    print(f"      Intensity: {fire.get('fire_intensity', 'N/A')}")
                    print()
            
            return real_data
            
        except Exception as e:
            logger.error(f"Real data collection failed: {e}")
            return []

def demo_combined_pipeline():
    """統合パイプラインデモ"""
    print("🔗 Combined Pipeline Demo")
    print("=" * 50)
    
    # テンプレート + 実データ統合設定
    config = PublicDataConfig()
    config.target_documents = 150  # 小規模テスト
    config.output_dir = "demo_output_combined"
    
    # MAP_KEY設定があれば実データも含める
    map_key = os.getenv('NASA_FIRMS_MAP_KEY')
    if map_key:
        config.nasa_firms_map_key = map_key
    
    collector = PublicWildfireDataCollector(config)
    
    try:
        # 統合データ収集
        combined_data = collector.collect_all_public_data()
        
        # ソース別分析
        source_analysis = {}
        for item in combined_data:
            source = item.get('source', 'Unknown')
            if source not in source_analysis:
                source_analysis[source] = {
                    'count': 0,
                    'avg_text_length': 0,
                    'locations': set()
                }
            
            source_analysis[source]['count'] += 1
            source_analysis[source]['avg_text_length'] += len(item.get('text', ''))
            source_analysis[source]['locations'].add(item.get('location', 'Unknown'))
        
        # 平均計算
        for source in source_analysis:
            count = source_analysis[source]['count']
            if count > 0:
                source_analysis[source]['avg_text_length'] /= count
            source_analysis[source]['unique_locations'] = len(source_analysis[source]['locations'])
            del source_analysis[source]['locations']  # setはJSON化できないので削除
        
        print(f"✅ Combined Pipeline Results:")
        print(f"   Total documents: {len(combined_data)}")
        print(f"\n📊 Source Analysis:")
        
        for source, stats in source_analysis.items():
            print(f"   {source}:")
            print(f"     - Documents: {stats['count']}")
            print(f"     - Avg text length: {stats['avg_text_length']:.1f} chars")
            print(f"     - Unique locations: {stats['unique_locations']}")
        
        # データ保存
        output_file = Path(config.output_dir) / "demo_combined_results.json"
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'total_documents': len(combined_data),
                'source_analysis': source_analysis,
                'sample_data': combined_data[:5]  # 最初の5件のサンプル
            }, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 Results saved to: {output_file}")
        
        return combined_data
        
    except Exception as e:
        logger.error(f"Combined pipeline failed: {e}")
        return []

def main():
    """メインデモ実行"""
    print("🔥 NASA FIRMS Integration Demo Suite")
    print("=" * 60)
    print("Demonstrating template-based and real data collection")
    print("=" * 60)
    
    # 1. テンプレートベースデモ
    template_data = demo_template_based_collection()
    
    print("\n" + "="*60)
    
    # 2. 実データ収集デモ
    real_data = demo_real_data_collection_simulation()
    
    print("\n" + "="*60)
    
    # 3. 統合パイプラインデモ
    combined_data = demo_combined_pipeline()
    
    print("\n" + "="*60)
    print("🎉 Demo Suite Completed!")
    print(f"📈 Summary:")
    print(f"   Template data: {len(template_data)} documents")
    print(f"   Real data: {len(real_data)} documents")
    print(f"   Combined pipeline: {len(combined_data)} documents")
    
    if not os.getenv('NASA_FIRMS_MAP_KEY'):
        print(f"\n💡 Next steps:")
        print(f"   - Get NASA FIRMS MAP_KEY for real data collection")
        print(f"   - Integrate with v0-7 sentiment analysis pipeline")
        print(f"   - Scale up to production volumes (1K-10K documents)")

if __name__ == "__main__":
    main()