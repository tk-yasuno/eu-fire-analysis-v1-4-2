"""
評価・メトリクス計算モジュール
クラスタリング性能評価機能
"""

import json
import os
import numpy as np
from typing import Dict, List, Tuple, Optional
from sklearn.metrics import (
    silhouette_score, 
    davies_bouldin_score, 
    calinski_harabasz_score,
    adjusted_rand_score,
    normalized_mutual_info_score
)
from sklearn.metrics.cluster import contingency_matrix
import logging

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClusterEvaluator:
    """クラスタリング評価クラス"""
    
    def __init__(self, output_dir: str = "outputs"):
        """
        Args:
            output_dir: 出力ディレクトリ
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"ClusterEvaluator initialized with output_dir: {output_dir}")
    
    def calculate_internal_metrics(self, features: np.ndarray, labels: np.ndarray) -> Dict:
        """
        内部評価指標を計算
        
        Args:
            features: 特徴量行列
            labels: クラスターラベル
            
        Returns:
            評価指標辞書
        """
        logger.info("Calculating internal metrics...")
        
        metrics = {}
        
        try:
            # シルエットスコア（-1 to 1, 高いほど良い）
            silhouette = silhouette_score(features, labels)
            metrics['silhouette_score'] = float(silhouette)
            logger.info(f"Silhouette Score: {silhouette:.4f}")
            
        except Exception as e:
            logger.warning(f"Failed to calculate silhouette score: {e}")
            metrics['silhouette_score'] = None
        
        try:
            # Davies-Bouldin指数（低いほど良い）
            davies_bouldin = davies_bouldin_score(features, labels)
            metrics['davies_bouldin_score'] = float(davies_bouldin)
            logger.info(f"Davies-Bouldin Score: {davies_bouldin:.4f}")
            
        except Exception as e:
            logger.warning(f"Failed to calculate Davies-Bouldin score: {e}")
            metrics['davies_bouldin_score'] = None
        
        try:
            # Calinski-Harabasz指数（高いほど良い）
            calinski_harabasz = calinski_harabasz_score(features, labels)
            metrics['calinski_harabasz_score'] = float(calinski_harabasz)
            logger.info(f"Calinski-Harabasz Score: {calinski_harabasz:.4f}")
            
        except Exception as e:
            logger.warning(f"Failed to calculate Calinski-Harabasz score: {e}")
            metrics['calinski_harabasz_score'] = None
        
        return metrics
    
    def calculate_cluster_statistics(self, labels: np.ndarray, scores: np.ndarray) -> Dict:
        """
        クラスター統計を計算
        
        Args:
            labels: クラスターラベル
            scores: スコア配列
            
        Returns:
            統計情報辞書
        """
        logger.info("Calculating cluster statistics...")
        
        unique_labels = np.unique(labels)
        n_clusters = len(unique_labels)
        
        stats = {
            'n_clusters': int(n_clusters),
            'n_samples': int(len(labels)),
            'cluster_distribution': {},
            'intra_cluster_variance': {},
            'cluster_separation': {}
        }
        
        # クラスター分布
        for label in unique_labels:
            mask = labels == label
            cluster_size = np.sum(mask)
            cluster_scores = scores[mask]
            
            stats['cluster_distribution'][int(label)] = {
                'size': int(cluster_size),
                'proportion': float(cluster_size / len(labels)),
                'score_mean': float(np.mean(cluster_scores)),
                'score_std': float(np.std(cluster_scores)),
                'score_min': float(np.min(cluster_scores)),
                'score_max': float(np.max(cluster_scores))
            }
        
        # バランス指標（理想的には各クラスターのサイズが均等）
        cluster_sizes = [stats['cluster_distribution'][int(label)]['size'] for label in unique_labels]
        ideal_size = len(labels) / n_clusters
        balance_score = 1.0 - (np.std(cluster_sizes) / ideal_size)
        stats['balance_score'] = float(max(0.0, balance_score))
        
        return stats
    
    def calculate_separation_metrics(self, features: np.ndarray, labels: np.ndarray) -> Dict:
        """
        クラスター間分離度を計算
        
        Args:
            features: 特徴量行列
            labels: クラスターラベル
            
        Returns:
            分離度メトリクス辞書
        """
        logger.info("Calculating separation metrics...")
        
        unique_labels = np.unique(labels)
        n_clusters = len(unique_labels)
        
        # クラスター中心点計算
        centroids = {}
        for label in unique_labels:
            mask = labels == label
            centroids[label] = np.mean(features[mask], axis=0)
        
        # クラスター内分散（小さいほど良い）
        intra_cluster_variance = 0.0
        for label in unique_labels:
            mask = labels == label
            cluster_features = features[mask]
            centroid = centroids[label]
            
            if len(cluster_features) > 0:
                variance = np.mean(np.sum((cluster_features - centroid) ** 2, axis=1))
                intra_cluster_variance += variance
        
        intra_cluster_variance /= n_clusters
        
        # クラスター間距離（大きいほど良い）
        inter_cluster_distances = []
        for i, label1 in enumerate(unique_labels):
            for j, label2 in enumerate(unique_labels):
                if i < j:
                    dist = np.linalg.norm(centroids[label1] - centroids[label2])
                    inter_cluster_distances.append(dist)
        
        avg_inter_cluster_distance = np.mean(inter_cluster_distances) if inter_cluster_distances else 0.0
        
        # 分離度スコア（inter / intra）
        separation_score = avg_inter_cluster_distance / (intra_cluster_variance + 1e-8)
        
        separation_metrics = {
            'intra_cluster_variance': float(intra_cluster_variance),
            'avg_inter_cluster_distance': float(avg_inter_cluster_distance),
            'separation_score': float(separation_score)
        }
        
        return separation_metrics
    
    def evaluate_score_coherence(self, labels: np.ndarray, scores: np.ndarray) -> Dict:
        """
        スコアの一貫性を評価
        
        Args:
            labels: クラスターラベル
            scores: スコア配列
            
        Returns:
            一貫性メトリクス辞書
        """
        logger.info("Evaluating score coherence...")
        
        unique_labels = np.unique(labels)
        
        # クラスター内スコア分散の平均
        intra_cluster_score_variances = []
        for label in unique_labels:
            mask = labels == label
            cluster_scores = scores[mask]
            if len(cluster_scores) > 1:
                intra_cluster_score_variances.append(np.var(cluster_scores))
        
        avg_intra_score_variance = np.mean(intra_cluster_score_variances) if intra_cluster_score_variances else 0.0
        
        # クラスター間スコア差
        cluster_score_means = []
        for label in unique_labels:
            mask = labels == label
            cluster_scores = scores[mask]
            cluster_score_means.append(np.mean(cluster_scores))
        
        inter_cluster_score_variance = np.var(cluster_score_means)
        
        # 一貫性スコア（inter / (intra + ε)）
        coherence_score = inter_cluster_score_variance / (avg_intra_score_variance + 1e-8)
        
        coherence_metrics = {
            'avg_intra_score_variance': float(avg_intra_score_variance),
            'inter_cluster_score_variance': float(inter_cluster_score_variance),
            'score_coherence': float(coherence_score)
        }
        
        return coherence_metrics
    
    def generate_quality_assessment(self, metrics: Dict) -> Dict:
        """
        総合品質評価を生成
        
        Args:
            metrics: 計算されたメトリクス
            
        Returns:
            品質評価辞書
        """
        logger.info("Generating quality assessment...")
        
        assessment = {
            'overall_quality': 'unknown',
            'strengths': [],
            'weaknesses': [],
            'recommendations': []
        }
        
        # シルエットスコア評価
        silhouette = metrics.get('silhouette_score')
        if silhouette is not None:
            if silhouette > 0.7:
                assessment['strengths'].append("Excellent cluster separation (high silhouette score)")
            elif silhouette > 0.5:
                assessment['strengths'].append("Good cluster separation")
            elif silhouette > 0.25:
                assessment['weaknesses'].append("Moderate cluster separation")
            else:
                assessment['weaknesses'].append("Poor cluster separation (low silhouette score)")
                assessment['recommendations'].append("Consider adjusting the number of clusters")
        
        # Davies-Bouldin評価
        davies_bouldin = metrics.get('davies_bouldin_score')
        if davies_bouldin is not None:
            if davies_bouldin < 1.0:
                assessment['strengths'].append("Well-separated clusters (low Davies-Bouldin score)")
            elif davies_bouldin < 2.0:
                assessment['weaknesses'].append("Moderate cluster overlap")
            else:
                assessment['weaknesses'].append("High cluster overlap")
                assessment['recommendations'].append("Consider feature engineering or different clustering approach")
        
        # バランス評価
        balance_score = metrics.get('balance_score', 0)
        if balance_score > 0.8:
            assessment['strengths'].append("Well-balanced cluster sizes")
        elif balance_score < 0.5:
            assessment['weaknesses'].append("Imbalanced cluster sizes")
            assessment['recommendations'].append("Consider balancing techniques or adjusting cluster parameters")
        
        # 総合評価
        if len(assessment['strengths']) > len(assessment['weaknesses']):
            assessment['overall_quality'] = 'good'
        elif len(assessment['weaknesses']) > len(assessment['strengths']):
            assessment['overall_quality'] = 'poor'
        else:
            assessment['overall_quality'] = 'moderate'
        
        return assessment
    
    def run_comprehensive_evaluation(self, features: np.ndarray, labels: np.ndarray,
                                   scores: np.ndarray) -> Dict:
        """
        包括的評価を実行
        
        Args:
            features: 特徴量行列
            labels: クラスターラベル
            scores: スコア配列
            
        Returns:
            完全な評価結果
        """
        logger.info("Running comprehensive evaluation...")
        
        evaluation_results = {}
        
        # 各種メトリクス計算
        evaluation_results['internal_metrics'] = self.calculate_internal_metrics(features, labels)
        evaluation_results['cluster_statistics'] = self.calculate_cluster_statistics(labels, scores)
        evaluation_results['separation_metrics'] = self.calculate_separation_metrics(features, labels)
        evaluation_results['coherence_metrics'] = self.evaluate_score_coherence(labels, scores)
        
        # 総合品質評価
        all_metrics = {**evaluation_results['internal_metrics'], 
                      **evaluation_results['cluster_statistics'],
                      **evaluation_results['separation_metrics'],
                      **evaluation_results['coherence_metrics']}
        
        evaluation_results['quality_assessment'] = self.generate_quality_assessment(all_metrics)
        
        return evaluation_results
    
    def save_evaluation_results(self, evaluation_results: Dict) -> str:
        """
        評価結果を保存
        
        Args:
            evaluation_results: 評価結果辞書
            
        Returns:
            保存されたファイルパス
        """
        eval_path = os.path.join(self.output_dir, "evaluation_metrics.json")
        
        with open(eval_path, 'w', encoding='utf-8') as f:
            json.dump(evaluation_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Evaluation results saved to {eval_path}")
        return eval_path
    
    def print_evaluation_summary(self, evaluation_results: Dict):
        """評価結果サマリーを出力"""
        logger.info("=== Clustering Evaluation Summary ===")
        
        # 内部メトリクス
        internal = evaluation_results.get('internal_metrics', {})
        if internal.get('silhouette_score') is not None:
            logger.info(f"Silhouette Score: {internal['silhouette_score']:.4f}")
        if internal.get('davies_bouldin_score') is not None:
            logger.info(f"Davies-Bouldin Score: {internal['davies_bouldin_score']:.4f}")
        
        # 統計情報
        stats = evaluation_results.get('cluster_statistics', {})
        logger.info(f"Number of clusters: {stats.get('n_clusters', 'N/A')}")
        logger.info(f"Total samples: {stats.get('n_samples', 'N/A')}")
        logger.info(f"Balance score: {stats.get('balance_score', 'N/A'):.3f}")
        
        # 品質評価
        quality = evaluation_results.get('quality_assessment', {})
        logger.info(f"Overall quality: {quality.get('overall_quality', 'unknown')}")
        
        if quality.get('strengths'):
            logger.info("Strengths:")
            for strength in quality['strengths']:
                logger.info(f"  + {strength}")
        
        if quality.get('weaknesses'):
            logger.info("Weaknesses:")
            for weakness in quality['weaknesses']:
                logger.info(f"  - {weakness}")
        
        if quality.get('recommendations'):
            logger.info("Recommendations:")
            for rec in quality['recommendations']:
                logger.info(f"  → {rec}")


def main():
    """メイン実行関数（テスト用）"""
    # テストデータ作成
    n_samples = 150
    feature_dim = 50
    
    # ダミーデータ
    features = np.random.randn(n_samples, feature_dim)
    labels = np.random.randint(0, 3, n_samples)
    scores = np.random.rand(n_samples)
    
    # 評価実行
    evaluator = ClusterEvaluator()
    
    try:
        evaluation_results = evaluator.run_comprehensive_evaluation(features, labels, scores)
        eval_path = evaluator.save_evaluation_results(evaluation_results)
        evaluator.print_evaluation_summary(evaluation_results)
        
        print(f"✓ Evaluation completed successfully!")
        print(f"Results saved to: {eval_path}")
        
    except Exception as e:
        print(f"✗ Evaluation failed: {e}")
        logger.error(f"Error details: {e}", exc_info=True)


if __name__ == "__main__":
    main()