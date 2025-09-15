#!/usr/bin/env python3
"""
ヨーロッパ地域 NASA FIRMS 森林火災データ分析パイプライン
フルEU・UK・北欧をカバーするヨーロッパエリアの火災データ分析システム
結果は自動生成されるタイムスタンプ付きフォルダに保存

エリア範囲:
- 緯度: 北緯34°～北緯72° (地中海～北極圏)  
- 経度: 西経25°～東経50° (大西洋～ウラル)
- 対象国: イタリア（focus）、フランス、スペイン、ドイツ、英国、北欧諸国、全EU加盟国
"""

import os
import sys
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional
from typing import Dict, List, Optional

# パス設定
scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts')
sys.path.append(scripts_dir)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# モジュールインポート
from scripts.data_collector import DataCollector
from scripts.model_loader import ModelLoader
from scripts.embedding_generator import EmbeddingGenerator
from adaptive_clustering_selector import AdaptiveClusteringSelector
from scripts.visualization import VisualizationManager
from cluster_feature_analyzer import ClusterFeatureAnalyzer
from fire_analysis_report_generator import FireAnalysisReportGenerator

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:%(name)s:%(message)s'
)
logger = logging.getLogger(__name__)


def _time_step(step_name):
    """ステップ実行時間測定用デコレーター"""
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            logger.info(f"=== Starting: {step_name} ===")
            try:
                result = func(self, *args, **kwargs)
                end_time = time.time()
                logger.info(f"=== Completed: {step_name} ({end_time - start_time:.2f}s) ===")
                return result
            except Exception as e:
                end_time = time.time()
                logger.error(f"=== Failed: {step_name} ({end_time - start_time:.2f}s) - {e} ===")
                raise
        return wrapper
    return decorator


class EuropeFirmsAnalyzer:
    """ヨーロッパ地域NASA FIRMS森林火災分析システム"""
    
    def __init__(self, config_path: str = "config_europe_firms.json"):
        """
        初期化
        
        Args:
            config_path: 設定ファイルパス
        """
        self.config_path = config_path
        self.config = self._load_config()
        
        # タイムスタンプ付きアウトプットディレクトリ作成
        timestamp = datetime.now().strftime(self.config['output']['timestamp_format'])
        self.output_dir = f"data_firms_europe_{timestamp}"
        os.makedirs(self.output_dir, exist_ok=True)
        
        logger.info(f"Output directory: {self.output_dir}")
        
        # 処理時間記録
        self.step_times = {}
        
    def _load_config(self) -> Dict:
        """設定ファイル読み込み"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"Configuration loaded: {self.config_path}")
            return config
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise
    
    def _time_step(self, step_name: str):
        """ステップ実行時間測定デコレータ"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                start_time = time.time()
                result = func(*args, **kwargs)
                elapsed = time.time() - start_time
                self.step_times[step_name] = elapsed
                logger.info(f"Step '{step_name}' completed in {elapsed:.2f}s")
                return result
            return wrapper
        return decorator
    
    def run_pipeline(self) -> str:
        """
        ヨーロッパ地域火災分析パイプライン実行
        
        Returns:
            結果ファイルパス
        """
        logger.info("� Starting Europe FIRMS Fire Analysis Pipeline")
        
        start_time = time.time()
        
        try:
            # Step 1: コンポーネント初期化
            self._initialize_components()
            
            # Step 2: NASA FIRMSデータ収集
            data = self._collect_nasa_firms_data()
            if data is None or len(data) == 0:
                logger.warning("No data collected - aborting pipeline")
                return None
            
            # Step 3: 埋め込み生成
            embeddings, scores = self._generate_embeddings(data)
            
            # Step 4: 適応的クラスタリング
            labels, clustering_results = self._perform_adaptive_clustering(embeddings)
            
            # Step 5: 包括的可視化
            self._create_comprehensive_visualizations(embeddings, labels, data, scores)
            
            # Step 6: クラスタ特徴分析
            feature_analysis = self._perform_cluster_feature_analysis(data, labels)
            
            # Step 7: 最終結果保存
            result_path = self._save_final_results(data, labels, clustering_results, feature_analysis)
            
            # Step 8: 包括的レポート生成
            self._generate_comprehensive_report(data, labels, clustering_results, feature_analysis)
            
            # 実行完了
            total_time = time.time() - start_time
            logger.info(f"🎉 Europe Fire Analysis Pipeline completed successfully!")
            logger.info(f"Processing time: {total_time:.2f}s for {len(data)} samples")
            
            # 結果サマリー表示
            self._display_results_summary(clustering_results, total_time, len(data))
            
            return result_path
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def _initialize_components(self):
        """パイプラインコンポーネント初期化"""
        logger.info("=== Initializing Pipeline Components ===")
        
        # モデルローダー初期化
        self.model_loader = ModelLoader(
            model_name=self.config['embedding']['model_name'],
            device=self.config['embedding']['device']
        )
        self.model = self.model_loader.load_model()
        
        # 埋め込み生成器初期化
        self.embedding_generator = EmbeddingGenerator(
            model=self.model,
            batch_size=self.config['embedding']['batch_size']
        )
        
        # 適応的クラスタリング選択器初期化
        clustering_config = self.config['adaptive_clustering']
        self.clustering_selector = AdaptiveClusteringSelector(
            output_dir=self.output_dir,
            min_cluster_quality=clustering_config['quality_thresholds']['min_cluster_quality'],
            max_noise_ratio=clustering_config['quality_thresholds']['max_noise_ratio']
        )
        
        # 可視化マネージャー初期化
        self.visualization_manager = VisualizationManager(
            output_dir=self.output_dir,
            figsize=(12, 8)
        )
        
        # クラスタ特徴分析器初期化
        self.feature_analyzer = ClusterFeatureAnalyzer(
            output_dir=self.output_dir
        )
        
        # レポート生成器初期化
        self.report_generator = FireAnalysisReportGenerator(
            output_dir=self.output_dir,
            config=self.config
        )
        
        logger.info("All components initialized successfully")
    
    def _collect_nasa_firms_data(self):
        """NASA FIRMSデータ収集"""
        logger.info("=== Collecting NASA FIRMS Data (Europe Region) ===")
        
        collector = DataCollector(
            raw_data_dir=os.path.join(self.output_dir, "raw"),
            cleaned_data_dir=os.path.join(self.output_dir, "cleaned")
        )
        
        # 南米地域のデータ収集
        nasa_config = self.config['nasa_firms']
        data = collector.collect_nasa_firms_data(
            map_key=nasa_config['map_key'],
            area_params=nasa_config['area_params'],
            days_back=nasa_config['days_back'],
            satellite=nasa_config['satellite']
        )
        
        if data is not None:
            # 信頼度フィルタリング
            confidence_threshold = self.config['nasa_firms']['confidence_threshold']
            high_confidence_data = data[data['confidence'] >= confidence_threshold]
            
            logger.info(f"Filtered to {len(high_confidence_data)} high-confidence detections (>= {confidence_threshold}%)")
            
            # サンプル数制限
            max_samples = self.config['processing']['max_samples']
            if len(high_confidence_data) > max_samples:
                logger.warning(f"Data exceeds max_samples limit ({len(high_confidence_data)} > {max_samples})")
                logger.warning(f"Truncating to first {max_samples} samples for system stability")
                high_confidence_data = high_confidence_data.head(max_samples)
            
            logger.info(f"Final dataset: {len(high_confidence_data)} NASA FIRMS records for comprehensive analysis")
            
            # データ保存
            data_path = os.path.join(self.output_dir, "nasa_firms_data.csv")
            high_confidence_data.to_csv(data_path, index=False)
            logger.info(f"Data saved: {data_path}")
            
            return high_confidence_data
        else:
            logger.error("Failed to collect NASA FIRMS data")
            return None
    
    def _generate_embeddings(self, data):
        """テキスト埋め込み生成"""
        logger.info("=== Generating Text Embeddings ===")
        
        # NASA FIRMSデータから火災検知情報のテキストを生成
        texts = []
        for _, row in data.iterrows():
            # 火災検知情報からテキスト記述を生成
            text = f"Fire detection at latitude {row['latitude']:.3f}, longitude {row['longitude']:.3f} on {row['acq_date']} at {row['acq_time']}. " \
                   f"Brightness temperature: {row['bright_ti4']:.2f}K, confidence: {row['confidence']}%, " \
                   f"Fire radiative power: {row['frp']:.2f}MW. Satellite: {row['satellite']}-{row['instrument']}, " \
                   f"Day/Night: {'Daytime' if row['daynight'] == 'D' else 'Nighttime'} detection."
            texts.append(text)
        
        logger.info(f"Generated {len(texts)} fire detection text descriptions")
        
        embeddings, scores = self.embedding_generator.generate_embeddings_batch(texts)
        
        if embeddings is not None:
            logger.info(f"Generated {len(embeddings)} embeddings (dim: {embeddings.shape[1]})")
            
            # 埋め込み保存
            import numpy as np
            embeddings_path = os.path.join(self.output_dir, "embeddings.npy")
            np.save(embeddings_path, embeddings.cpu().numpy())
            logger.info(f"Embeddings saved: {embeddings_path}")
            
            return embeddings, scores
        else:
            raise ValueError("Failed to generate embeddings")
    
    def _perform_adaptive_clustering(self, embeddings):
        """適応的クラスタリング実行"""
        logger.info("=== Performing Adaptive Clustering ===")
        
        # 適応的パラメータ取得
        hdbscan_params = self.config['adaptive_clustering']['hdbscan_params']
        kmeans_params = self.config['adaptive_clustering']['kmeans_params']
        
        logger.info(f"Adaptive parameters: HDBSCAN {hdbscan_params}, k-means {kmeans_params}")
        
        # クラスタリング実行
        best_result, selection_details = self.clustering_selector.select_best_clustering(
            embeddings=embeddings.cpu().numpy(),
            hdbscan_params=hdbscan_params,
            kmeans_params=kmeans_params
        )
        
        logger.info(f"Selected method: {best_result.method}")
        logger.info(f"Selection reason: {selection_details['selection_reason']}")
        
        clustering_results = {
            'labels': best_result.labels,
            'quality_score': best_result.metrics.quality_score,
            'selected_method': best_result.method,
            'method_reason': selection_details['selection_reason'],
            'n_clusters': best_result.metrics.n_clusters,
            'noise_ratio': best_result.metrics.noise_ratio,
            'metrics': best_result.metrics
        }
        
        return best_result.labels, clustering_results
    
    def _create_comprehensive_visualizations(self, embeddings, labels, data, scores):
        """包括的可視化作成"""
        logger.info("=== Creating Comprehensive Visualizations ===")
        
        try:
            # t-SNE次元削減
            coords_2d = self.visualization_manager.reduce_dimensions_tsne(embeddings.cpu().numpy())
            
            # numpy配列に変換
            scores_np = scores.cpu().numpy() if hasattr(scores, 'cpu') else scores
            
            # t-SNEクラスター可視化
            tsne_plot_path = self.visualization_manager.create_cluster_plot(
                coords=coords_2d,
                labels=labels,
                scores=scores_np,
                title="Europe Fire Detection Clusters (t-SNE)"
            )
            
            # スコア分布プロット
            score_plot_path = self.visualization_manager.create_score_distribution_plot(
                scores=scores_np,
                labels=labels
            )
            
            logger.info(f"✅ Generated t-SNE plot: {tsne_plot_path}")
            logger.info(f"✅ Generated score distribution: {score_plot_path}")
            
            return {
                'tsne_plot': tsne_plot_path,
                'score_plot': score_plot_path,
                'coords_2d': coords_2d
            }
                
        except Exception as e:
            logger.error(f"Visualization failed: {e}")
            return {}
    
    def _perform_cluster_feature_analysis(self, data, labels):
        """クラスタ特徴分析実行"""
        logger.info("=== Performing Cluster Feature Analysis ===")
        
        # クラスタ特徴分析実行
        feature_analysis = self.feature_analyzer.analyze_cluster_features(data, labels)
        
        # 特徴分析可視化作成
        viz_files = self.feature_analyzer.create_feature_visualizations(feature_analysis, self.output_dir)
        
        logger.info(f"✅ Generated {len(viz_files)} cluster feature analysis visualizations")
        for viz_file in viz_files:
            logger.info(f"  📈 Feature viz: {viz_file}")
        
        return feature_analysis
    
    def _save_final_results(self, data, labels, clustering_results, feature_analysis):
        """最終結果保存"""
        logger.info("=== Saving Final Results ===")
        
        # 最終結果JSON保存（numpy配列を除外）
        clustering_summary = {
            'quality_score': clustering_results['quality_score'],
            'selected_method': clustering_results['selected_method'],
            'method_reason': clustering_results['method_reason'],
            'n_clusters': clustering_results['n_clusters'],
            'noise_ratio': clustering_results['noise_ratio']
        }
        
        final_results = {
            'timestamp': datetime.now().isoformat(),
            'region': 'Europe',
            'total_samples': len(data),
            'clustering': clustering_summary,
            'feature_analysis_summary': {
                'total_clusters': len(feature_analysis.get('clusters', {})),
                'geographic_distribution': len(feature_analysis.get('geographic_distribution', {})),
                'temporal_patterns': len(feature_analysis.get('temporal_patterns', {}))
            },
            'config_used': self.config_path
        }
        
        result_path = os.path.join(self.output_dir, "final_europe_results.json")
        with open(result_path, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Final results saved: {result_path}")
        
        # ラベル付きデータ保存
        data_with_labels = data.copy()
        data_with_labels['cluster_label'] = labels
        
        labeled_data_path = os.path.join(self.output_dir, "europe_fires_clustered.csv")
        data_with_labels.to_csv(labeled_data_path, index=False)
        logger.info(f"Labeled data saved: {labeled_data_path}")
        
        return result_path
    
    def _generate_comprehensive_report(self, data, labels, clustering_results, feature_analysis):
        """包括的分析レポート生成"""
        logger.info("=== Generating Comprehensive Analysis Report ===")
        
        try:
            # レポート生成データ準備
            report_data = {
                'data': data,
                'labels': labels,
                'clustering_results': clustering_results,
                'feature_analysis': feature_analysis,
                'region': 'Europe',
                'region_name': 'Europe',
                'focus_country': 'Italy'
            }
            
            # レポート生成実行
            report_path = self.report_generator.generate_report(report_data)
            
            logger.info(f"📝 Comprehensive analysis report generated: {report_path}")
            
            return report_path
            
        except Exception as e:
            logger.error(f"Report generation failed: {e}")
            logger.info("Pipeline completed successfully despite report generation issue")
            return None
    
    def _display_results_summary(self, clustering_results, total_time, num_samples):
        """結果サマリー表示"""
        print("\n" + "="*70)
        print("� EUROPE FOREST FIRE ANALYSIS RESULTS")
        print("="*70)
        print(f"✅ Status: SUCCESS")
        print(f"🎯 Selected Method: {clustering_results['selected_method']}")
        print(f"📊 Quality Score: {clustering_results['quality_score']:.3f}")
        print(f"🔢 Clusters Found: {clustering_results['n_clusters']}")
        print(f"📉 Noise Ratio: {clustering_results['noise_ratio']:.1%}")
        print(f"📦 Total Fire Detections: {num_samples}")
        print(f"⏱️ Processing Time: {total_time:.2f}s")
        print(f"📁 Results Directory: {self.output_dir}")
        print(f"🌍 Region Coverage: Europe (34°N-72°N, 25°W-50°E)")
        print("="*70)


def main():
    """メイン実行関数"""
    import argparse
    
    # コマンドライン引数の設定
    parser = argparse.ArgumentParser(description='Europe Fire Detection Analysis Pipeline')
    parser.add_argument('--config', default='config_europe_firms.json',
                       help='Configuration file path (default: config_europe_firms.json)')
    args = parser.parse_args()
    
    try:
        # 指定された設定ファイルで分析実行
        analyzer = EuropeFirmsAnalyzer(config_path=args.config)
        result_path = analyzer.run_pipeline()
        
        if result_path:
            print(f"\n🎉 Analysis completed successfully!")
            print(f"📁 Results saved in: {analyzer.output_dir}")
            print(f"📄 Main results file: {result_path}")
            
            # 生成されたファイル一覧表示
            import os
            output_files = os.listdir(analyzer.output_dir)
            
            print(f"\n📊 Generated files ({len(output_files)} total):")
            for file in sorted(output_files):
                if file.endswith('.png'):
                    print(f"  🖼️  {file}")
                elif file.endswith('.json'):
                    print(f"  📋 {file}")
                elif file.endswith('.csv'):
                    print(f"  📊 {file}")
                elif file.endswith('.md'):
                    print(f"  📝 {file} (📖 COMPREHENSIVE ANALYSIS REPORT)")
                else:
                    print(f"  📄 {file}")
            
            # レポートファイルの特別案内
            report_file = os.path.join(analyzer.output_dir, "comprehensive_fire_analysis_report.md")
            if os.path.exists(report_file):
                print(f"\n📖 **包括的分析レポートが生成されました**")
                print(f"   ファイル: {report_file}")
                print(f"   内容: 6つの図表を用いた詳細な火災分析レポート")
                print(f"   形式: Markdown形式（テキストエディタで閲覧可能）")
        else:
            print("❌ Analysis failed - no data collected")
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    main()