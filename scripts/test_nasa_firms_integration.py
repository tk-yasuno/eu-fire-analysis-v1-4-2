#!/usr/bin/env python3
"""
NASA FIRMS Real Data Integration Test
テンプレートデータと実データの統合テスト・比較分析
"""

import os
import sys
import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

# 親ディレクトリをパスに追加
sys.path.append(str(Path(__file__).parent))

from public_wildfire_collector import PublicWildfireDataCollector, PublicDataConfig

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FIRMSIntegrationTester:
    """NASA FIRMS統合テスター"""
    
    def __init__(self):
        self.output_dir = Path("test_results_nasa_firms")
        self.output_dir.mkdir(exist_ok=True)
        
    def test_template_vs_real_data(self):
        """テンプレートデータ vs 実データ比較テスト"""
        logger.info("Starting template vs real data comparison test...")
        
        results = {
            "test_timestamp": datetime.now().isoformat(),
            "template_data_results": {},
            "real_data_results": {},
            "comparison_analysis": {}
        }
        
        # テンプレートデータテスト（MAP_KEYなし）
        logger.info("Testing template-based data collection...")
        template_config = PublicDataConfig()
        template_config.nasa_firms_map_key = None  # テンプレートモード
        template_collector = PublicWildfireDataCollector(template_config)
        
        try:
            template_data = template_collector.collect_all_public_data()
            results["template_data_results"] = {
                "status": "success",
                "total_documents": len(template_data),
                "sources": self._analyze_data_sources(template_data),
                "sentiment_distribution": self._analyze_sentiment_distribution(template_data),
                "text_length_stats": self._analyze_text_lengths(template_data)
            }
            
            # テンプレートデータ保存
            template_file = self.output_dir / "template_data_test.json"
            with open(template_file, 'w', encoding='utf-8') as f:
                json.dump(template_data[:100], f, ensure_ascii=False, indent=2)  # サンプル保存
                
        except Exception as e:
            logger.error(f"Template data test failed: {e}")
            results["template_data_results"] = {"status": "failed", "error": str(e)}
        
        # 実データテスト（MAP_KEYあり）
        logger.info("Testing real NASA FIRMS data collection...")
        real_config = PublicDataConfig()
        real_config.nasa_firms_map_key = os.getenv('NASA_FIRMS_MAP_KEY')
        
        if real_config.nasa_firms_map_key:
            real_collector = PublicWildfireDataCollector(real_config)
            
            try:
                real_data = real_collector.collect_all_public_data()
                results["real_data_results"] = {
                    "status": "success",
                    "total_documents": len(real_data),
                    "sources": self._analyze_data_sources(real_data),
                    "sentiment_distribution": self._analyze_sentiment_distribution(real_data),
                    "text_length_stats": self._analyze_text_lengths(real_data),
                    "nasa_firms_stats": self._analyze_nasa_firms_data(real_data)
                }
                
                # 実データ保存
                real_file = self.output_dir / "real_data_test.json"
                with open(real_file, 'w', encoding='utf-8') as f:
                    json.dump(real_data[:100], f, ensure_ascii=False, indent=2)
                    
            except Exception as e:
                logger.error(f"Real data test failed: {e}")
                results["real_data_results"] = {"status": "failed", "error": str(e)}
        else:
            logger.warning("NASA_FIRMS_MAP_KEY not set, skipping real data test")
            results["real_data_results"] = {"status": "skipped", "reason": "MAP_KEY not configured"}
        
        # 比較分析
        if (results["template_data_results"].get("status") == "success" and 
            results["real_data_results"].get("status") == "success"):
            
            results["comparison_analysis"] = self._compare_datasets(
                results["template_data_results"],
                results["real_data_results"]
            )
        
        # 結果保存
        results_file = self.output_dir / "integration_test_results.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Integration test completed. Results saved to: {results_file}")
        return results
    
    def test_sentiment_pipeline_integration(self):
        """v0-7感情分析パイプライン統合テスト"""
        logger.info("Testing sentiment pipeline integration...")
        
        # 設定
        config = PublicDataConfig()
        config.target_documents = 100  # テスト用に小サイズ
        collector = PublicWildfireDataCollector(config)
        
        try:
            # データ収集
            data = collector.collect_all_public_data()
            
            # 感情分析実行
            sentiment_results = []
            for item in data:
                sentiment = collector.perform_sentiment_analysis(item["text"], item)
                sentiment_results.append({
                    "text": item["text"][:100] + "..." if len(item["text"]) > 100 else item["text"],
                    "source": item["source"],
                    "sentiment": sentiment["sentiment"],
                    "confidence": sentiment["confidence"]
                })
            
            # 結果分析
            df = pd.DataFrame(sentiment_results)
            
            analysis = {
                "total_samples": len(sentiment_results),
                "sentiment_distribution": df["sentiment"].value_counts().to_dict(),
                "source_sentiment_breakdown": df.groupby(["source", "sentiment"]).size().to_dict(),
                "average_confidence": df["confidence"].astype(float).mean(),
                "confidence_by_sentiment": df.groupby("sentiment")["confidence"].astype(float).mean().to_dict()
            }
            
            # 可視化
            self._create_sentiment_visualizations(df)
            
            # 結果保存
            pipeline_file = self.output_dir / "sentiment_pipeline_test.json"
            with open(pipeline_file, 'w', encoding='utf-8') as f:
                json.dump(analysis, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Sentiment pipeline test completed: {analysis['total_samples']} samples analyzed")
            return analysis
            
        except Exception as e:
            logger.error(f"Sentiment pipeline test failed: {e}")
            return {"status": "failed", "error": str(e)}
    
    def test_performance_benchmark(self):
        """パフォーマンス・スケーラビリティテスト"""
        logger.info("Running performance benchmark test...")
        
        import time
        
        benchmark_results = {}
        
        # 複数のサイズでテスト
        test_sizes = [100, 500, 1000]
        
        for size in test_sizes:
            logger.info(f"Testing with {size} documents...")
            
            config = PublicDataConfig()
            config.target_documents = size
            collector = PublicWildfireDataCollector(config)
            
            start_time = time.time()
            data = collector.collect_all_public_data()
            collection_time = time.time() - start_time
            
            start_time = time.time()
            processed_data = collector.process_collected_data(data)
            processing_time = time.time() - start_time
            
            benchmark_results[f"size_{size}"] = {
                "collection_time": collection_time,
                "processing_time": processing_time,
                "total_time": collection_time + processing_time,
                "documents_collected": len(data),
                "documents_processed": len(processed_data),
                "collection_speed": len(data) / collection_time if collection_time > 0 else 0,
                "processing_speed": len(processed_data) / processing_time if processing_time > 0 else 0
            }
        
        # 結果保存
        benchmark_file = self.output_dir / "performance_benchmark.json"
        with open(benchmark_file, 'w', encoding='utf-8') as f:
            json.dump(benchmark_results, f, ensure_ascii=False, indent=2)
        
        logger.info("Performance benchmark completed")
        return benchmark_results
    
    def _analyze_data_sources(self, data):
        """データソース分析"""
        sources = {}
        for item in data:
            source = item.get("source", "unknown")
            sources[source] = sources.get(source, 0) + 1
        return sources
    
    def _analyze_sentiment_distribution(self, data):
        """感情分布分析"""
        sentiments = {}
        for item in data:
            sentiment = item.get("sentiment", "中立")
            sentiments[sentiment] = sentiments.get(sentiment, 0) + 1
        return sentiments
    
    def _analyze_text_lengths(self, data):
        """テキスト長統計"""
        lengths = [len(item.get("text", "")) for item in data]
        if not lengths:
            return {"mean": 0, "min": 0, "max": 0}
        
        return {
            "mean": sum(lengths) / len(lengths),
            "min": min(lengths),
            "max": max(lengths),
            "total_texts": len(lengths)
        }
    
    def _analyze_nasa_firms_data(self, data):
        """NASA FIRMSデータ専用分析"""
        firms_data = [item for item in data if item.get("source") == "NASA_FIRMS_REAL"]
        
        if not firms_data:
            return {"status": "no_firms_data"}
        
        analysis = {
            "total_firms_records": len(firms_data),
            "fire_intensity_distribution": {},
            "satellite_distribution": {},
            "coordinate_ranges": {"lat_min": None, "lat_max": None, "lon_min": None, "lon_max": None}
        }
        
        # 火災強度分布
        for item in firms_data:
            intensity = item.get("fire_intensity", "unknown")
            analysis["fire_intensity_distribution"][intensity] = \
                analysis["fire_intensity_distribution"].get(intensity, 0) + 1
        
        # 衛星分布
        for item in firms_data:
            satellite_data = item.get("satellite_data", {})
            satellite = satellite_data.get("satellite", "unknown")
            analysis["satellite_distribution"][satellite] = \
                analysis["satellite_distribution"].get(satellite, 0) + 1
        
        # 座標範囲
        coordinates = [item.get("coordinates", {}) for item in firms_data if item.get("coordinates")]
        if coordinates:
            lats = [coord.get("latitude") for coord in coordinates if coord.get("latitude")]
            lons = [coord.get("longitude") for coord in coordinates if coord.get("longitude")]
            
            if lats and lons:
                analysis["coordinate_ranges"] = {
                    "lat_min": min(lats),
                    "lat_max": max(lats),
                    "lon_min": min(lons),
                    "lon_max": max(lons)
                }
        
        return analysis
    
    def _compare_datasets(self, template_results, real_results):
        """データセット比較分析"""
        comparison = {
            "document_count_ratio": real_results["total_documents"] / template_results["total_documents"],
            "source_comparison": {
                "template_sources": list(template_results["sources"].keys()),
                "real_sources": list(real_results["sources"].keys()),
                "new_sources_in_real": []
            },
            "sentiment_diversity": {
                "template_sentiments": len(template_results["sentiment_distribution"]),
                "real_sentiments": len(real_results["sentiment_distribution"])
            },
            "text_length_comparison": {
                "template_avg": template_results["text_length_stats"]["mean"],
                "real_avg": real_results["text_length_stats"]["mean"]
            }
        }
        
        # 新しいソース検出
        template_sources = set(template_results["sources"].keys())
        real_sources = set(real_results["sources"].keys())
        comparison["source_comparison"]["new_sources_in_real"] = list(real_sources - template_sources)
        
        return comparison
    
    def _create_sentiment_visualizations(self, df):
        """感情分析結果可視化"""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            plt.style.use('default')
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('NASA FIRMS Sentiment Analysis Results', fontsize=16)
            
            # 感情分布
            sentiment_counts = df['sentiment'].value_counts()
            axes[0, 0].pie(sentiment_counts.values, labels=sentiment_counts.index, autopct='%1.1f%%')
            axes[0, 0].set_title('Sentiment Distribution')
            
            # ソース別感情分布
            source_sentiment = df.groupby(['source', 'sentiment']).size().unstack(fill_value=0)
            source_sentiment.plot(kind='bar', ax=axes[0, 1], stacked=True)
            axes[0, 1].set_title('Sentiment by Source')
            axes[0, 1].tick_params(axis='x', rotation=45)
            
            # 信頼度分布
            df['confidence_float'] = df['confidence'].astype(float)
            axes[1, 0].hist(df['confidence_float'], bins=20, alpha=0.7)
            axes[1, 0].set_title('Confidence Distribution')
            axes[1, 0].set_xlabel('Confidence Score')
            
            # 感情別信頼度
            for sentiment in df['sentiment'].unique():
                sentiment_data = df[df['sentiment'] == sentiment]['confidence_float']
                axes[1, 1].hist(sentiment_data, alpha=0.5, label=sentiment, bins=10)
            axes[1, 1].set_title('Confidence by Sentiment')
            axes[1, 1].set_xlabel('Confidence Score')
            axes[1, 1].legend()
            
            plt.tight_layout()
            plt.savefig(self.output_dir / 'sentiment_analysis_results.png', dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info("Sentiment visualization saved")
            
        except ImportError:
            logger.warning("Matplotlib/Seaborn not available, skipping visualization")
        except Exception as e:
            logger.error(f"Error creating visualizations: {e}")

def main():
    """メイン実行関数"""
    print("🌲 NASA FIRMS Integration Test Suite")
    print("=" * 50)
    
    tester = FIRMSIntegrationTester()
    
    try:
        # 1. テンプレート vs 実データ比較テスト
        print("\n1. Template vs Real Data Comparison Test")
        comparison_results = tester.test_template_vs_real_data()
        print(f"   Template data: {comparison_results['template_data_results'].get('status', 'unknown')}")
        print(f"   Real data: {comparison_results['real_data_results'].get('status', 'unknown')}")
        
        # 2. 感情分析パイプライン統合テスト
        print("\n2. Sentiment Pipeline Integration Test")
        sentiment_results = tester.test_sentiment_pipeline_integration()
        if isinstance(sentiment_results, dict) and 'total_samples' in sentiment_results:
            print(f"   Analyzed {sentiment_results['total_samples']} samples")
            print(f"   Average confidence: {sentiment_results.get('average_confidence', 0):.3f}")
        
        # 3. パフォーマンスベンチマーク
        print("\n3. Performance Benchmark Test")
        benchmark_results = tester.test_performance_benchmark()
        for size, metrics in benchmark_results.items():
            print(f"   {size}: {metrics['documents_collected']} docs in {metrics['total_time']:.2f}s")
        
        print(f"\n✅ All tests completed successfully!")
        print(f"📁 Results saved to: {tester.output_dir}")
        
        # MAP_KEY設定チェック
        if not os.getenv('NASA_FIRMS_MAP_KEY'):
            print("\n⚠️  NASA_FIRMS_MAP_KEY not set. Real data tests were skipped.")
            print("   To test real data collection:")
            print("   1. Get MAP_KEY from: https://firms.modaps.eosdis.nasa.gov/api/map_key/")
            print("   2. Set environment variable: NASA_FIRMS_MAP_KEY=your_key_here")
        
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        raise

if __name__ == "__main__":
    main()